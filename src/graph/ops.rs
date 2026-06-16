/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Ergonomic, opt-in helpers for FalkorDB operations that complete in the **background** on
//! the server.
//!
//! Creating or dropping an index or a constraint returns as soon as the server *accepts* the
//! request; the work itself (populating the index, enforcing the constraint) then happens on a
//! background worker thread. `GRAPH.COPY` blocks until the copy finishes but can fail
//! transiently while the server is unable to `fork`.
//!
//! The builders in this module make waiting for these operations explicit and intuitive while
//! staying fully backward compatible: the existing eager methods (`create_index`, `copy_graph`,
//! …) are untouched, and every builder exposes a non-blocking [`execute`](IndexOpBuilder::execute)
//! terminal next to the waiting [`wait`](IndexOpBuilder::wait) terminal.

use crate::{
    Constraint, ConstraintStatus, ConstraintType, EntityType, FalkorIndex, IndexStatus, IndexType,
};
use std::time::{Duration, Instant};

/// Identifies which background operation a [`crate::FalkorDBError::Timeout`] refers to.
#[derive(Copy, Clone, Debug, Eq, PartialEq, strum::Display)]
pub enum WaitOperation {
    /// Waiting for an index to become operational.
    #[strum(serialize = "index creation")]
    IndexCreation,
    /// Waiting for an index to be removed.
    #[strum(serialize = "index drop")]
    IndexDrop,
    /// Waiting for a constraint to become operational.
    #[strum(serialize = "constraint creation")]
    ConstraintCreation,
    /// Waiting for a constraint to be removed.
    #[strum(serialize = "constraint drop")]
    ConstraintDrop,
    /// Re-issuing a graph copy until it succeeds.
    #[strum(serialize = "graph copy")]
    GraphCopy,
}

const DEFAULT_READINESS_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_COPY_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DEFAULT_MAX_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_BACKOFF_FACTOR: f64 = 1.5;
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Tunables for how a `wait` terminal polls/retries a background operation.
///
/// The defaults are sensible for readiness polling (30s timeout, 50ms initial interval growing
/// by 1.5x up to 1s). All setters normalize their input so a [`WaitOptions`] can never busy-spin
/// or sleep with a degenerate interval.
///
/// # Example
/// ```
/// use falkordb::WaitOptions;
/// use std::time::Duration;
///
/// let options = WaitOptions::with_timeout(Duration::from_secs(5))
///     .poll_interval(Duration::from_millis(100))
///     .backoff_factor(2.0)
///     .max_interval(Duration::from_secs(2));
/// # let _ = options;
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct WaitOptions {
    timeout: Duration,
    poll_interval: Duration,
    backoff_factor: f64,
    max_interval: Duration,
}

impl Default for WaitOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_READINESS_TIMEOUT,
            poll_interval: DEFAULT_POLL_INTERVAL,
            backoff_factor: DEFAULT_BACKOFF_FACTOR,
            max_interval: DEFAULT_MAX_INTERVAL,
        }
    }
}

impl WaitOptions {
    /// A [`WaitOptions`] with the default readiness settings (30s timeout).
    pub fn new() -> Self {
        Self::default()
    }

    /// The default settings used for graph copy retries (60s timeout).
    pub(crate) fn for_copy() -> Self {
        Self::default().timeout(DEFAULT_COPY_TIMEOUT)
    }

    /// A [`WaitOptions`] with the default settings but the provided overall `timeout`.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::default().timeout(timeout)
    }

    /// Sets the overall timeout, after which a `wait` terminal returns
    /// [`crate::FalkorDBError::Timeout`].
    pub fn timeout(
        mut self,
        timeout: Duration,
    ) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the initial poll interval (floored to 1ms, never larger than `max_interval`).
    pub fn poll_interval(
        mut self,
        poll_interval: Duration,
    ) -> Self {
        self.poll_interval = poll_interval.max(MIN_POLL_INTERVAL);
        self.max_interval = self.max_interval.max(self.poll_interval);
        self
    }

    /// Sets the exponential backoff multiplier applied between polls (floored to 1.0).
    pub fn backoff_factor(
        mut self,
        backoff_factor: f64,
    ) -> Self {
        self.backoff_factor = backoff_factor.max(1.0);
        self
    }

    /// Sets the maximum interval between polls (never smaller than `poll_interval`).
    pub fn max_interval(
        mut self,
        max_interval: Duration,
    ) -> Self {
        self.max_interval = max_interval.max(self.poll_interval);
        self
    }

    /// The delay to wait before the poll following `attempt` (0-based), capped at `max_interval`.
    pub(crate) fn delay_for_attempt(
        &self,
        attempt: u32,
    ) -> Duration {
        let exponent = i32::try_from(attempt).unwrap_or(i32::MAX);
        let scaled = self.poll_interval.as_secs_f64() * self.backoff_factor.powi(exponent);
        Duration::from_secs_f64(scaled.min(self.max_interval.as_secs_f64()))
    }
}

/// The outcome of a single poll attempt.
pub(crate) enum Step<T> {
    /// The operation finished; return this value.
    Done(T),
    /// The operation is still pending; poll again after a delay.
    Retry,
    /// The operation reached a terminal failure; return this error without retrying.
    Fail(crate::FalkorDBError),
}

/// What to do after a [`Step::Retry`], computed against the deadline.
enum Wakeup {
    /// Sleep for this long, then poll again.
    Sleep(Duration),
    /// The deadline has passed; time out.
    TimedOut,
}

fn next_wakeup(
    options: &WaitOptions,
    deadline: Instant,
    attempt: u32,
) -> Wakeup {
    let now = Instant::now();
    if now >= deadline {
        Wakeup::TimedOut
    } else {
        Wakeup::Sleep(options.delay_for_attempt(attempt).min(deadline - now))
    }
}

/// Synchronously polls `attempt` until it is [`Step::Done`]/[`Step::Fail`] or the timeout elapses.
pub(crate) fn poll_sync<T>(
    options: &WaitOptions,
    operation: WaitOperation,
    mut attempt: impl FnMut() -> crate::FalkorResult<Step<T>>,
) -> crate::FalkorResult<T> {
    let deadline = Instant::now() + options.timeout;
    let mut attempts = 0u32;
    loop {
        match attempt()? {
            Step::Done(value) => return Ok(value),
            Step::Fail(error) => return Err(error),
            Step::Retry => {}
        }
        match next_wakeup(options, deadline, attempts) {
            Wakeup::TimedOut => {
                return Err(crate::FalkorDBError::Timeout {
                    operation,
                    timeout: options.timeout,
                })
            }
            Wakeup::Sleep(delay) => std::thread::sleep(delay),
        }
        attempts += 1;
    }
}

/// Asynchronously polls `attempt` until it is [`Step::Done`]/[`Step::Fail`] or the timeout
/// elapses. This is the async counterpart of [`poll_sync`] and shares the exact same backoff and
/// deadline logic; it is generic over an async closure so the index/constraint wait (which needs
/// `&mut AsyncGraph`) and the graph-copy wait (which needs `&FalkorAsyncClient`) can both reuse it.
#[cfg(feature = "tokio")]
pub(crate) async fn poll_async<T>(
    options: &WaitOptions,
    operation: WaitOperation,
    mut attempt: impl AsyncFnMut() -> crate::FalkorResult<Step<T>>,
) -> crate::FalkorResult<T> {
    let deadline = Instant::now() + options.timeout;
    let mut attempts = 0u32;
    loop {
        match attempt().await? {
            Step::Done(value) => return Ok(value),
            Step::Fail(error) => return Err(error),
            Step::Retry => {}
        }
        match next_wakeup(options, deadline, attempts) {
            Wakeup::TimedOut => {
                return Err(crate::FalkorDBError::Timeout {
                    operation,
                    timeout: options.timeout,
                })
            }
            Wakeup::Sleep(delay) => tokio::time::sleep(delay).await,
        }
        attempts += 1;
    }
}

/// `true` if `error` is a transient `GRAPH.COPY` failure that should be retried (the server was
/// temporarily unable to `fork`). FalkorDB's own tests retry copies on this condition.
pub(crate) fn is_transient_copy_error(error: &crate::FalkorDBError) -> bool {
    match error {
        crate::FalkorDBError::RedisError(message)
        | crate::FalkorDBError::RedisParsingError(message) => {
            message.to_lowercase().contains("could not fork")
        }
        _ => false,
    }
}

/// Maps the result of a single `GRAPH.COPY` attempt into a [`Step`]: success completes the wait,
/// a transient `could not fork` failure retries, and any other error fails immediately. Shared by
/// the sync and async copy builders.
pub(crate) fn classify_copy_result<T>(result: crate::FalkorResult<T>) -> Step<T> {
    match result {
        Ok(value) => Step::Done(value),
        Err(error) if is_transient_copy_error(&error) => Step::Retry,
        Err(error) => Step::Fail(error),
    }
}

/// `true` if `properties` describe the same set of property names (order-insensitive).
fn same_property_set(
    left: &[String],
    right: &[String],
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort();
    right.sort();
    left == right
}

/// `true` once an index of `index_type` covering every property in `properties` is operational.
fn index_is_ready(
    indices: &[FalkorIndex],
    entity_type: EntityType,
    label: &str,
    properties: &[String],
    index_type: IndexType,
) -> bool {
    indices.iter().any(|index| {
        index.entity_type == entity_type
            && index.index_label == label
            && index.status == IndexStatus::Active
            && properties.iter().all(|property| {
                index
                    .field_types
                    .get(property)
                    .is_some_and(|types| types.contains(&index_type))
            })
    })
}

/// `true` once no property in `properties` is still indexed with `index_type`.
fn index_is_dropped(
    indices: &[FalkorIndex],
    entity_type: EntityType,
    label: &str,
    properties: &[String],
    index_type: IndexType,
) -> bool {
    !indices.iter().any(|index| {
        index.entity_type == entity_type
            && index.index_label == label
            && properties.iter().any(|property| {
                index
                    .field_types
                    .get(property)
                    .is_some_and(|types| types.contains(&index_type))
            })
    })
}

/// The readiness of a constraint, or [`ConstraintReadiness::Pending`] if it is not visible yet.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ConstraintReadiness {
    Ready,
    Pending,
    Failed,
}

fn constraint_readiness(
    constraints: &[Constraint],
    constraint_type: ConstraintType,
    entity_type: EntityType,
    label: &str,
    properties: &[String],
) -> ConstraintReadiness {
    match constraints.iter().find(|constraint| {
        constraint.constraint_type == constraint_type
            && constraint.entity_type == entity_type
            && constraint.label == label
            && same_property_set(&constraint.properties, properties)
    }) {
        Some(constraint) => match constraint.status {
            ConstraintStatus::Active => ConstraintReadiness::Ready,
            ConstraintStatus::Pending => ConstraintReadiness::Pending,
            ConstraintStatus::Failed => ConstraintReadiness::Failed,
        },
        None => ConstraintReadiness::Pending,
    }
}

fn constraint_is_dropped(
    constraints: &[Constraint],
    constraint_type: ConstraintType,
    entity_type: EntityType,
    label: &str,
    properties: &[String],
) -> bool {
    !constraints.iter().any(|constraint| {
        constraint.constraint_type == constraint_type
            && constraint.entity_type == entity_type
            && constraint.label == label
            && same_property_set(&constraint.properties, properties)
    })
}

/// A pending index readiness check shared by the sync and async wait loops.
struct IndexWait {
    drop: bool,
    index_type: IndexType,
    entity_type: EntityType,
    label: String,
    properties: Vec<String>,
}

impl IndexWait {
    fn satisfied(
        &self,
        indices: &[FalkorIndex],
    ) -> bool {
        if self.drop {
            index_is_dropped(
                indices,
                self.entity_type,
                &self.label,
                &self.properties,
                self.index_type,
            )
        } else {
            index_is_ready(
                indices,
                self.entity_type,
                &self.label,
                &self.properties,
                self.index_type,
            )
        }
    }

    fn step(
        &self,
        indices: &[FalkorIndex],
    ) -> Step<()> {
        bool_step(self.satisfied(indices))
    }
}

/// A pending constraint readiness check shared by the sync and async wait loops.
struct ConstraintWait {
    drop: bool,
    constraint_type: ConstraintType,
    entity_type: EntityType,
    label: String,
    properties: Vec<String>,
}

impl ConstraintWait {
    fn step(
        &self,
        constraints: &[Constraint],
    ) -> Step<()> {
        if self.drop {
            return bool_step(constraint_is_dropped(
                constraints,
                self.constraint_type,
                self.entity_type,
                &self.label,
                &self.properties,
            ));
        }
        match constraint_readiness(
            constraints,
            self.constraint_type,
            self.entity_type,
            &self.label,
            &self.properties,
        ) {
            ConstraintReadiness::Ready => Step::Done(()),
            ConstraintReadiness::Pending => Step::Retry,
            ConstraintReadiness::Failed => Step::Fail(crate::FalkorDBError::ConstraintFailed {
                label: self.label.clone(),
                properties: self.properties.clone(),
                constraint_type: self.constraint_type,
            }),
        }
    }
}

fn bool_step(done: bool) -> Step<()> {
    if done {
        Step::Done(())
    } else {
        Step::Retry
    }
}

/// A readiness check against either the index list or the constraint list.
enum Wait {
    Index(IndexWait),
    Constraint(ConstraintWait),
}

impl Wait {
    fn operation(&self) -> WaitOperation {
        match self {
            Wait::Index(wait) if wait.drop => WaitOperation::IndexDrop,
            Wait::Index(_) => WaitOperation::IndexCreation,
            Wait::Constraint(wait) if wait.drop => WaitOperation::ConstraintDrop,
            Wait::Constraint(_) => WaitOperation::ConstraintCreation,
        }
    }
}

/// The index operation a builder represents, independent of sync/async execution.
pub(crate) enum IndexOp {
    Create {
        index_type: IndexType,
        entity_type: EntityType,
        label: String,
        properties: Vec<String>,
        options: Option<std::collections::HashMap<String, String>>,
    },
    Drop {
        index_type: IndexType,
        entity_type: EntityType,
        label: String,
        properties: Vec<String>,
    },
}

impl IndexOp {
    fn to_wait(&self) -> Wait {
        let (drop, index_type, entity_type, label, properties) = match self {
            IndexOp::Create {
                index_type,
                entity_type,
                label,
                properties,
                ..
            } => (
                false,
                *index_type,
                *entity_type,
                label.clone(),
                properties.clone(),
            ),
            IndexOp::Drop {
                index_type,
                entity_type,
                label,
                properties,
            } => (
                true,
                *index_type,
                *entity_type,
                label.clone(),
                properties.clone(),
            ),
        };
        Wait::Index(IndexWait {
            drop,
            index_type,
            entity_type,
            label,
            properties,
        })
    }
}

/// The constraint operation a builder represents, independent of sync/async execution.
pub(crate) enum ConstraintOp {
    CreateUnique {
        entity_type: EntityType,
        label: String,
        properties: Vec<String>,
    },
    CreateMandatory {
        entity_type: EntityType,
        label: String,
        properties: Vec<String>,
    },
    Drop {
        constraint_type: ConstraintType,
        entity_type: EntityType,
        label: String,
        properties: Vec<String>,
    },
}

impl ConstraintOp {
    fn to_wait(&self) -> Wait {
        let (drop, constraint_type, entity_type, label, properties) = match self {
            ConstraintOp::CreateUnique {
                entity_type,
                label,
                properties,
            } => (
                false,
                ConstraintType::Unique,
                *entity_type,
                label.clone(),
                properties.clone(),
            ),
            ConstraintOp::CreateMandatory {
                entity_type,
                label,
                properties,
            } => (
                false,
                ConstraintType::Mandatory,
                *entity_type,
                label.clone(),
                properties.clone(),
            ),
            ConstraintOp::Drop {
                constraint_type,
                entity_type,
                label,
                properties,
            } => (
                true,
                *constraint_type,
                *entity_type,
                label.clone(),
                properties.clone(),
            ),
        };
        Wait::Constraint(ConstraintWait {
            drop,
            constraint_type,
            entity_type,
            label,
            properties,
        })
    }
}

/// Converts a slice of `Display` properties into owned strings for storage in a builder.
pub(crate) fn owned_properties<P: std::fmt::Display>(properties: &[P]) -> Vec<String> {
    properties
        .iter()
        .map(|property| property.to_string())
        .collect()
}

/// Converts owned property strings back into `&str` slices for the constraint commands.
pub(crate) fn property_refs(properties: &[String]) -> Vec<&str> {
    properties.iter().map(String::as_str).collect()
}

#[cfg(feature = "tokio")]
mod async_ops;
mod blocking_ops;

#[cfg(feature = "tokio")]
pub use async_ops::{AsyncConstraintOpBuilder, AsyncCopyGraphBuilder, AsyncIndexOpBuilder};
pub use blocking_ops::{ConstraintOpBuilder, CopyGraphBuilder, IndexOpBuilder};

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn index(
        entity_type: EntityType,
        label: &str,
        status: IndexStatus,
        field: &str,
        index_type: IndexType,
    ) -> FalkorIndex {
        let mut field_types = HashMap::new();
        field_types.insert(field.to_string(), vec![index_type]);
        FalkorIndex {
            entity_type,
            status,
            index_label: label.to_string(),
            fields: vec![field.to_string()],
            field_types,
            language: String::new(),
            stopwords: vec![],
            info: HashMap::new(),
            options: HashMap::new(),
        }
    }

    fn constraint(
        constraint_type: ConstraintType,
        entity_type: EntityType,
        label: &str,
        properties: &[&str],
        status: ConstraintStatus,
    ) -> Constraint {
        Constraint {
            constraint_type,
            label: label.to_string(),
            properties: properties.iter().map(|p| p.to_string()).collect(),
            entity_type,
            status,
        }
    }

    #[test]
    fn wait_options_defaults() {
        let options = WaitOptions::new();
        assert_eq!(options.timeout, DEFAULT_READINESS_TIMEOUT);
        assert_eq!(options.poll_interval, DEFAULT_POLL_INTERVAL);
        assert_eq!(options.backoff_factor, DEFAULT_BACKOFF_FACTOR);
        assert_eq!(options.max_interval, DEFAULT_MAX_INTERVAL);
    }

    #[test]
    fn wait_options_for_copy_uses_longer_timeout() {
        assert_eq!(WaitOptions::for_copy().timeout, DEFAULT_COPY_TIMEOUT);
    }

    #[test]
    fn wait_options_with_timeout() {
        let options = WaitOptions::with_timeout(Duration::from_secs(7));
        assert_eq!(options.timeout, Duration::from_secs(7));
    }

    #[test]
    fn wait_options_normalize_inputs() {
        let options = WaitOptions::new()
            .poll_interval(Duration::ZERO)
            .backoff_factor(0.1)
            .max_interval(Duration::ZERO);
        assert_eq!(options.poll_interval, MIN_POLL_INTERVAL);
        assert_eq!(options.backoff_factor, 1.0);
        // max_interval can never drop below poll_interval.
        assert_eq!(options.max_interval, MIN_POLL_INTERVAL);
    }

    #[test]
    fn wait_options_poll_interval_raises_max_interval() {
        let options = WaitOptions::new().poll_interval(Duration::from_secs(5));
        assert_eq!(options.poll_interval, Duration::from_secs(5));
        assert_eq!(options.max_interval, Duration::from_secs(5));
    }

    #[test]
    fn delay_for_attempt_grows_and_caps() {
        let options = WaitOptions::new()
            .poll_interval(Duration::from_millis(100))
            .backoff_factor(2.0)
            .max_interval(Duration::from_millis(500));
        assert_eq!(options.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(options.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(options.delay_for_attempt(2), Duration::from_millis(400));
        // capped at max_interval
        assert_eq!(options.delay_for_attempt(3), Duration::from_millis(500));
        assert_eq!(options.delay_for_attempt(1000), Duration::from_millis(500));
        // a huge attempt count must not overflow the i32 exponent and shrink the delay
        assert_eq!(
            options.delay_for_attempt(u32::MAX),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn next_wakeup_sleeps_then_times_out() {
        let options = WaitOptions::new();
        let future = Instant::now() + Duration::from_secs(60);
        assert!(matches!(next_wakeup(&options, future, 0), Wakeup::Sleep(_)));
        let past = Instant::now() - Duration::from_secs(1);
        assert!(matches!(next_wakeup(&options, past, 0), Wakeup::TimedOut));
    }

    #[test]
    fn wait_operation_display() {
        assert_eq!(WaitOperation::IndexCreation.to_string(), "index creation");
        assert_eq!(WaitOperation::IndexDrop.to_string(), "index drop");
        assert_eq!(
            WaitOperation::ConstraintCreation.to_string(),
            "constraint creation"
        );
        assert_eq!(WaitOperation::ConstraintDrop.to_string(), "constraint drop");
        assert_eq!(WaitOperation::GraphCopy.to_string(), "graph copy");
    }

    #[test]
    fn poll_sync_returns_first_done() {
        let mut calls = 0;
        let value = poll_sync(&WaitOptions::new(), WaitOperation::IndexCreation, || {
            calls += 1;
            Ok(Step::Done(calls))
        })
        .unwrap();
        assert_eq!(value, 1);
    }

    #[test]
    fn poll_sync_retries_until_done() {
        let options = WaitOptions::new().poll_interval(Duration::from_millis(1));
        let mut calls = 0;
        let value = poll_sync(&options, WaitOperation::IndexCreation, || {
            calls += 1;
            Ok(if calls >= 3 {
                Step::Done(calls)
            } else {
                Step::Retry
            })
        })
        .unwrap();
        assert_eq!(value, 3);
    }

    #[test]
    fn poll_sync_propagates_fail() {
        let result: crate::FalkorResult<()> = poll_sync(
            &WaitOptions::new(),
            WaitOperation::ConstraintCreation,
            || {
                Ok(Step::Fail(crate::FalkorDBError::ConstraintFailed {
                    label: "L".to_string(),
                    properties: vec!["p".to_string()],
                    constraint_type: ConstraintType::Unique,
                }))
            },
        );
        assert!(matches!(
            result,
            Err(crate::FalkorDBError::ConstraintFailed { .. })
        ));
    }

    #[test]
    fn poll_sync_propagates_attempt_error() {
        let result: crate::FalkorResult<()> =
            poll_sync(&WaitOptions::new(), WaitOperation::IndexCreation, || {
                Err(crate::FalkorDBError::ConnectionDown)
            });
        assert!(matches!(result, Err(crate::FalkorDBError::ConnectionDown)));
    }

    #[test]
    fn poll_sync_times_out() {
        let options = WaitOptions::with_timeout(Duration::ZERO);
        let result: crate::FalkorResult<()> =
            poll_sync(&options, WaitOperation::GraphCopy, || Ok(Step::Retry));
        assert!(matches!(
            result,
            Err(crate::FalkorDBError::Timeout {
                operation: WaitOperation::GraphCopy,
                timeout,
            }) if timeout == Duration::ZERO
        ));
    }

    #[test]
    fn transient_copy_error_detection() {
        assert!(is_transient_copy_error(&crate::FalkorDBError::RedisError(
            "GRAPH.COPY failed, could not fork".to_string()
        )));
        assert!(is_transient_copy_error(
            &crate::FalkorDBError::RedisParsingError("Could Not Fork".to_string())
        ));
        assert!(!is_transient_copy_error(&crate::FalkorDBError::RedisError(
            "destination key already exists".to_string()
        )));
        assert!(!is_transient_copy_error(
            &crate::FalkorDBError::ConnectionDown
        ));
    }

    #[test]
    fn same_property_set_is_order_insensitive() {
        assert!(same_property_set(
            &["a".to_string(), "b".to_string()],
            &["b".to_string(), "a".to_string()]
        ));
        assert!(!same_property_set(
            &["a".to_string()],
            &["a".to_string(), "b".to_string()]
        ));
        assert!(!same_property_set(&["a".to_string()], &["b".to_string()]));
    }

    #[test]
    fn index_ready_requires_active_and_all_properties() {
        let mut multi = index(
            EntityType::Node,
            "P",
            IndexStatus::Active,
            "name",
            IndexType::Range,
        );
        multi
            .field_types
            .insert("age".to_string(), vec![IndexType::Range]);
        multi.fields.push("age".to_string());

        let props = vec!["name".to_string(), "age".to_string()];
        assert!(index_is_ready(
            std::slice::from_ref(&multi),
            EntityType::Node,
            "P",
            &props,
            IndexType::Range
        ));

        let pending = index(
            EntityType::Node,
            "P",
            IndexStatus::Pending,
            "name",
            IndexType::Range,
        );
        assert!(!index_is_ready(
            &[pending],
            EntityType::Node,
            "P",
            &["name".to_string()],
            IndexType::Range
        ));

        // Wrong index type / wrong label / missing property all read as not ready.
        let range = index(
            EntityType::Node,
            "P",
            IndexStatus::Active,
            "name",
            IndexType::Range,
        );
        assert!(!index_is_ready(
            std::slice::from_ref(&range),
            EntityType::Node,
            "P",
            &["name".to_string()],
            IndexType::Vector
        ));
        assert!(!index_is_ready(
            std::slice::from_ref(&range),
            EntityType::Node,
            "Other",
            &["name".to_string()],
            IndexType::Range
        ));
        assert!(!index_is_ready(
            &[],
            EntityType::Node,
            "P",
            &["name".to_string()],
            IndexType::Range
        ));
    }

    #[test]
    fn index_dropped_when_type_absent() {
        let range = index(
            EntityType::Node,
            "P",
            IndexStatus::Active,
            "name",
            IndexType::Range,
        );
        // Same property still indexed with this type -> not dropped.
        assert!(!index_is_dropped(
            std::slice::from_ref(&range),
            EntityType::Node,
            "P",
            &["name".to_string()],
            IndexType::Range
        ));
        // A different index type was dropped even though the range index remains.
        assert!(index_is_dropped(
            std::slice::from_ref(&range),
            EntityType::Node,
            "P",
            &["name".to_string()],
            IndexType::Vector
        ));
        assert!(index_is_dropped(
            &[],
            EntityType::Node,
            "P",
            &["name".to_string()],
            IndexType::Range
        ));
    }

    #[test]
    fn constraint_readiness_states() {
        let props = ["id"];
        let active = constraint(
            ConstraintType::Unique,
            EntityType::Node,
            "P",
            &props,
            ConstraintStatus::Active,
        );
        let pending = constraint(
            ConstraintType::Unique,
            EntityType::Node,
            "P",
            &props,
            ConstraintStatus::Pending,
        );
        let failed = constraint(
            ConstraintType::Unique,
            EntityType::Node,
            "P",
            &props,
            ConstraintStatus::Failed,
        );
        let needle = vec!["id".to_string()];

        assert_eq!(
            constraint_readiness(
                &[active],
                ConstraintType::Unique,
                EntityType::Node,
                "P",
                &needle
            ),
            ConstraintReadiness::Ready
        );
        assert_eq!(
            constraint_readiness(
                &[pending],
                ConstraintType::Unique,
                EntityType::Node,
                "P",
                &needle
            ),
            ConstraintReadiness::Pending
        );
        assert_eq!(
            constraint_readiness(
                &[failed],
                ConstraintType::Unique,
                EntityType::Node,
                "P",
                &needle
            ),
            ConstraintReadiness::Failed
        );
        // Not visible yet reads as pending.
        assert_eq!(
            constraint_readiness(&[], ConstraintType::Unique, EntityType::Node, "P", &needle),
            ConstraintReadiness::Pending
        );
    }

    #[test]
    fn constraint_dropped_detection() {
        let props = ["id"];
        let existing = constraint(
            ConstraintType::Mandatory,
            EntityType::Node,
            "P",
            &props,
            ConstraintStatus::Active,
        );
        let needle = vec!["id".to_string()];
        assert!(!constraint_is_dropped(
            std::slice::from_ref(&existing),
            ConstraintType::Mandatory,
            EntityType::Node,
            "P",
            &needle
        ));
        assert!(constraint_is_dropped(
            &[],
            ConstraintType::Mandatory,
            EntityType::Node,
            "P",
            &needle
        ));
    }

    #[test]
    fn index_wait_satisfied_dispatch() {
        let ready = index(
            EntityType::Node,
            "P",
            IndexStatus::Active,
            "name",
            IndexType::Range,
        );
        let create = IndexWait {
            drop: false,
            index_type: IndexType::Range,
            entity_type: EntityType::Node,
            label: "P".to_string(),
            properties: vec!["name".to_string()],
        };
        assert!(create.satisfied(std::slice::from_ref(&ready)));

        let drop = IndexWait {
            drop: true,
            index_type: IndexType::Range,
            entity_type: EntityType::Node,
            label: "P".to_string(),
            properties: vec!["name".to_string()],
        };
        assert!(!drop.satisfied(std::slice::from_ref(&ready)));
        assert!(drop.satisfied(&[]));

        // `step` maps the boolean readiness into a `Step` for both branches.
        assert!(matches!(
            create.step(std::slice::from_ref(&ready)),
            Step::Done(())
        ));
        assert!(matches!(create.step(&[]), Step::Retry));
    }

    #[test]
    fn bool_step_maps_both_branches() {
        assert!(matches!(bool_step(true), Step::Done(())));
        assert!(matches!(bool_step(false), Step::Retry));
    }

    #[test]
    fn classify_copy_result_maps_each_outcome() {
        assert!(matches!(classify_copy_result::<u8>(Ok(7)), Step::Done(7)));
        assert!(matches!(
            classify_copy_result::<u8>(Err(crate::FalkorDBError::RedisError(
                "MISCONF could not fork".to_string()
            ))),
            Step::Retry
        ));
        assert!(matches!(
            classify_copy_result::<u8>(Err(crate::FalkorDBError::RedisError(
                "some other failure".to_string()
            ))),
            Step::Fail(_)
        ));
    }

    #[test]
    fn constraint_wait_step_dispatch() {
        let failed = constraint(
            ConstraintType::Unique,
            EntityType::Node,
            "P",
            &["id"],
            ConstraintStatus::Failed,
        );
        let create = ConstraintWait {
            drop: false,
            constraint_type: ConstraintType::Unique,
            entity_type: EntityType::Node,
            label: "P".to_string(),
            properties: vec!["id".to_string()],
        };
        assert!(matches!(
            create.step(std::slice::from_ref(&failed)),
            Step::Fail(crate::FalkorDBError::ConstraintFailed { .. })
        ));
        assert!(matches!(create.step(&[]), Step::Retry));

        let active = constraint(
            ConstraintType::Unique,
            EntityType::Node,
            "P",
            &["id"],
            ConstraintStatus::Active,
        );
        assert!(matches!(create.step(&[active]), Step::Done(())));

        let drop = ConstraintWait {
            drop: true,
            constraint_type: ConstraintType::Unique,
            entity_type: EntityType::Node,
            label: "P".to_string(),
            properties: vec!["id".to_string()],
        };
        assert!(matches!(drop.step(&[]), Step::Done(())));
    }

    #[test]
    fn wait_operation_mapping() {
        let index_create = Wait::Index(IndexWait {
            drop: false,
            index_type: IndexType::Range,
            entity_type: EntityType::Node,
            label: "P".to_string(),
            properties: vec![],
        });
        assert_eq!(index_create.operation(), WaitOperation::IndexCreation);

        let index_drop = Wait::Index(IndexWait {
            drop: true,
            index_type: IndexType::Range,
            entity_type: EntityType::Node,
            label: "P".to_string(),
            properties: vec![],
        });
        assert_eq!(index_drop.operation(), WaitOperation::IndexDrop);

        let constraint_create = Wait::Constraint(ConstraintWait {
            drop: false,
            constraint_type: ConstraintType::Unique,
            entity_type: EntityType::Node,
            label: "P".to_string(),
            properties: vec![],
        });
        assert_eq!(
            constraint_create.operation(),
            WaitOperation::ConstraintCreation
        );

        let constraint_drop = Wait::Constraint(ConstraintWait {
            drop: true,
            constraint_type: ConstraintType::Unique,
            entity_type: EntityType::Node,
            label: "P".to_string(),
            properties: vec![],
        });
        assert_eq!(constraint_drop.operation(), WaitOperation::ConstraintDrop);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn poll_async_returns_first_done() {
        let value = poll_async(
            &WaitOptions::new(),
            WaitOperation::IndexCreation,
            async || Ok::<_, crate::FalkorDBError>(Step::Done(7)),
        )
        .await
        .unwrap();
        assert_eq!(value, 7);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn poll_async_retries_until_done() {
        let options = WaitOptions::new().poll_interval(Duration::from_millis(1));
        let mut calls = 0u32;
        let value = poll_async(&options, WaitOperation::IndexCreation, async || {
            calls += 1;
            Ok::<_, crate::FalkorDBError>(if calls < 3 {
                Step::Retry
            } else {
                Step::Done(calls)
            })
        })
        .await
        .unwrap();
        assert_eq!(value, 3);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn poll_async_propagates_fail() {
        let result: crate::FalkorResult<()> = poll_async(
            &WaitOptions::new(),
            WaitOperation::ConstraintCreation,
            async || {
                Ok(Step::Fail(crate::FalkorDBError::ConstraintFailed {
                    label: "P".to_string(),
                    properties: vec!["id".to_string()],
                    constraint_type: ConstraintType::Unique,
                }))
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(crate::FalkorDBError::ConstraintFailed { .. })
        ));
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn poll_async_propagates_attempt_error() {
        let result: crate::FalkorResult<()> = poll_async(
            &WaitOptions::new(),
            WaitOperation::IndexCreation,
            async || Err(crate::FalkorDBError::ParsingArray),
        )
        .await;
        assert!(matches!(result, Err(crate::FalkorDBError::ParsingArray)));
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn poll_async_times_out() {
        let options = WaitOptions::new().timeout(Duration::ZERO);
        let result: crate::FalkorResult<()> =
            poll_async(&options, WaitOperation::GraphCopy, async || Ok(Step::Retry)).await;
        assert!(matches!(
            result,
            Err(crate::FalkorDBError::Timeout {
                operation: WaitOperation::GraphCopy,
                timeout,
            }) if timeout == Duration::ZERO
        ));
    }
}
