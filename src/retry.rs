/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Opt-in, client-wide retry of transient connection failures.
//!
//! A [`RetryPolicy`] is **disabled by default**, so a client built without one behaves exactly as
//! before: every operation is attempted once. When a policy is configured on the
//! [`FalkorClientBuilder`](crate::FalkorClientBuilder), eligible operations that fail with a
//! transient connection error are automatically re-issued with a bounded backoff.
//!
//! # Safety
//!
//! The only scope available today is [`RetryScope::ReadOnly`], which retries **read-only /
//! idempotent** operations (`ro_query`, `explain`, `list_*`, …). **Write operations are never
//! retried**, so enabling a policy can never duplicate a write. Classification is by the API you
//! call, never by inspecting Cypher: `query()` is treated as a write even when it only reads, so
//! use `ro_query()` for retryable reads.
//!
//! # Scope
//!
//! Retry wraps query and procedure *execution* (the `QueryBuilder` / `ProcedureQueryBuilder`
//! seams). Direct client/admin calls (`list_graphs`, config getters/setters, `slowlog`, server
//! `INFO`) and the internal schema-cache refresh that may run while a result is parsed are not yet
//! wrapped; broadening coverage is a planned follow-up.

use crate::{FalkorDBError, FalkorResult};
use std::time::Duration;

/// Which operations an enabled [`RetryPolicy`] is allowed to re-issue.
///
/// `#[non_exhaustive]` so future scopes (e.g. retrying writes that provably never reached the
/// server) can be added without a breaking change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetryScope {
    /// Retry nothing. Identical to configuring no policy at all (the default).
    Disabled,
    /// Retry read-only / idempotent operations only. Writes are never retried.
    ReadOnly,
}

/// The backoff schedule applied between retry attempts.
///
/// The delay for retry `n` (0-based) is `min(base * factor.powi(n), max_delay)`, optionally with
/// full jitter (a uniform random value in `[0, delay)`) to avoid synchronized retries across many
/// clients. Build one with [`Backoff::exponential`] or [`Backoff::fixed`].
///
/// ```
/// use falkordb::Backoff;
/// use std::time::Duration;
///
/// // Exponential: 100ms, 300ms, 900ms, … (factor 3), never above 5s, jitter off.
/// let exponential = Backoff::exponential(Duration::from_millis(100))
///     .factor(3.0)
///     .max_delay(Duration::from_secs(5))
///     .jitter(false);
///
/// // Fixed: the same delay before every retry.
/// let fixed = Backoff::fixed(Duration::from_millis(250));
/// # let _ = (exponential, fixed);
/// ```
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Backoff {
    base: Duration,
    factor: f64,
    max_delay: Duration,
    jitter: bool,
}

impl Backoff {
    /// A constant delay between every attempt (no growth, no jitter).
    #[must_use]
    pub fn fixed(delay: Duration) -> Self {
        Self {
            base: delay,
            factor: 1.0,
            max_delay: delay,
            jitter: false,
        }
    }

    /// Exponential backoff starting at `base`, doubling each attempt (factor `2.0`), capped at
    /// `1s`, with full jitter enabled. Tune with [`factor`](Self::factor),
    /// [`max_delay`](Self::max_delay) and [`jitter`](Self::jitter).
    #[must_use]
    pub fn exponential(base: Duration) -> Self {
        Self {
            base,
            factor: 2.0,
            max_delay: Duration::from_secs(1),
            jitter: true,
        }
    }

    /// Set the growth factor applied to the delay after each attempt. Values below `1.0` are
    /// clamped to `1.0` (a non-growing schedule); `1.0` yields a constant delay.
    #[must_use]
    pub fn factor(
        mut self,
        factor: f64,
    ) -> Self {
        self.factor = factor.max(1.0);
        self
    }

    /// Set the upper bound on any single delay.
    #[must_use]
    pub fn max_delay(
        mut self,
        max_delay: Duration,
    ) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Enable or disable full jitter (a uniform random value in `[0, delay)`).
    #[must_use]
    pub fn jitter(
        mut self,
        jitter: bool,
    ) -> Self {
        self.jitter = jitter;
        self
    }
}

impl Default for Backoff {
    /// Exponential backoff from `50ms`, doubling, capped at `1s`, jitter on.
    fn default() -> Self {
        Self::exponential(Duration::from_millis(50))
    }
}

/// An opt-in, client-wide policy describing how many times, how fast, and which operations to
/// re-issue on transient connection failures.
///
/// Disabled by default: [`RetryPolicy::default`] equals [`RetryPolicy::disabled`], so a client
/// built without an explicit policy retries nothing and behaves exactly as before.
///
/// ```
/// use falkordb::{RetryPolicy, Backoff};
/// use std::time::Duration;
///
/// // Retry read-only operations up to 4 times total, exponential 100ms backoff capped at 2s.
/// let policy = RetryPolicy::read_only()
///     .max_attempts(4)
///     .backoff(Backoff::exponential(Duration::from_millis(100)).max_delay(Duration::from_secs(2)));
/// ```
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RetryPolicy {
    scope: RetryScope,
    max_attempts: u32,
    backoff: Backoff,
}

impl RetryPolicy {
    /// A policy that retries nothing — identical to configuring no policy at all.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            scope: RetryScope::Disabled,
            max_attempts: 1,
            backoff: Backoff::default(),
        }
    }

    /// Retry read-only / idempotent operations on transient connection failures: `3` attempts
    /// total with the [default exponential backoff](Backoff::default). Writes are never retried.
    #[must_use]
    pub fn read_only() -> Self {
        Self {
            scope: RetryScope::ReadOnly,
            max_attempts: 3,
            backoff: Backoff::default(),
        }
    }

    /// Set the **total** number of attempts (the first try plus retries). Values below `1` are
    /// clamped to `1`; `max_attempts(1)` is equivalent to disabling retries.
    #[must_use]
    pub fn max_attempts(
        mut self,
        attempts: u32,
    ) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// Replace the [`Backoff`] schedule used between attempts.
    #[must_use]
    pub fn backoff(
        mut self,
        backoff: Backoff,
    ) -> Self {
        self.backoff = backoff;
        self
    }

    /// The scope this policy applies to.
    #[must_use]
    pub fn scope(&self) -> RetryScope {
        self.scope
    }
}

impl Default for RetryPolicy {
    /// The default policy is [`RetryPolicy::disabled`].
    fn default() -> Self {
        Self::disabled()
    }
}

/// Whether an operation may be retried under [`RetryScope::ReadOnly`]. Threaded explicitly from
/// each call site (never inferred from the Cypher text) so the read/write boundary is the API the
/// caller chose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OpKind {
    /// A read-only / idempotent operation, safe to re-issue.
    ReadOnly,
    /// A write or otherwise non-idempotent operation; never auto-retried in v1.
    Write,
}

impl RetryPolicy {
    /// Fast-path check: when `true`, the execution seams skip the retry machinery entirely and run
    /// the operation exactly once, guaranteeing byte-identical behavior to having no policy.
    pub(crate) fn is_disabled(&self) -> bool {
        self.scope == RetryScope::Disabled || self.max_attempts <= 1
    }

    /// Whether `err` is a transient connection failure that a fresh attempt could recover from.
    ///
    /// Deliberately conservative: only the dead-/missing-connection family is retryable. The mixed
    /// [`RedisError`](FalkorDBError::RedisError) bucket (logic/syntax/auth/query-timeout/IO) and the
    /// [`Timeout`](FalkorDBError::Timeout) wait-operation error are **not** retried.
    fn is_retryable_error(err: &FalkorDBError) -> bool {
        matches!(
            err,
            FalkorDBError::ConnectionDown
                | FalkorDBError::NoConnection
                | FalkorDBError::EmptyConnection
                | FalkorDBError::SentinelConnection(_)
        )
    }

    /// Whether an operation of `kind` that failed with `err` is eligible for another attempt under
    /// this policy. Used as backon's `.when(..)` predicate; the attempt budget is enforced
    /// separately by the backoff schedule.
    pub(crate) fn should_retry(
        &self,
        kind: OpKind,
        err: &FalkorDBError,
    ) -> bool {
        match self.scope {
            RetryScope::Disabled => false,
            RetryScope::ReadOnly => kind == OpKind::ReadOnly && Self::is_retryable_error(err),
        }
    }

    /// Build the backoff that drives this policy's sleep schedule and attempt budget. We supply our
    /// own [`FalkorBackoffBuilder`] (rather than `backon::ExponentialBuilder`) so jitter is true
    /// *full* jitter bounded by `max_delay` — `backon`'s built-in jitter is additive and can exceed
    /// the cap. `backon` still drives the retry loop and sleeping.
    pub(crate) fn backon_builder(&self) -> FalkorBackoffBuilder {
        FalkorBackoffBuilder {
            base: self.backoff.base,
            factor: self.backoff.factor,
            max_delay: self.backoff.max_delay,
            max_attempts: self.max_attempts,
            jitter: self.backoff.jitter,
        }
    }
}

/// A [`backon::BackoffBuilder`] producing our exponential schedule: each delay is
/// `min(base * factor^n, max_delay)`, optionally replaced by full jitter (a uniform value in
/// `[0, delay)`). Yields `max_attempts - 1` delays, then `None` to stop.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FalkorBackoffBuilder {
    base: Duration,
    factor: f64,
    max_delay: Duration,
    max_attempts: u32,
    jitter: bool,
}

impl backon::BackoffBuilder for FalkorBackoffBuilder {
    type Backoff = FalkorBackoff;

    fn build(self) -> Self::Backoff {
        FalkorBackoff {
            next_base: self.base.as_secs_f64(),
            factor: self.factor,
            max_delay: self.max_delay.as_secs_f64(),
            remaining: self.max_attempts.saturating_sub(1),
            jitter: self.jitter,
            rng: if self.jitter { jitter_seed() } else { 0 },
        }
    }
}

/// The [`backon::Backoff`] iterator built from a [`FalkorBackoffBuilder`].
#[derive(Debug)]
pub(crate) struct FalkorBackoff {
    next_base: f64,
    factor: f64,
    max_delay: f64,
    remaining: u32,
    jitter: bool,
    rng: u64,
}

impl Iterator for FalkorBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;

        let capped = self.next_base.min(self.max_delay);
        // Advance the (pre-jitter) base for the next attempt, holding it at the cap.
        self.next_base = (self.next_base * self.factor).min(self.max_delay);

        let delay = if self.jitter {
            // Full jitter: a uniform value in [0, capped), so it never exceeds max_delay.
            capped * next_unit_f64(&mut self.rng)
        } else {
            capped
        };
        Some(Duration::from_secs_f64(delay))
    }
}

/// Seed the jitter PRNG from a process-wide counter mixed with the wall clock, so concurrent
/// retries don't share a sequence. Jitter does not need cryptographic randomness.
fn jitter_seed() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    // Ensure non-zero so the first splitmix64 step is well-distributed.
    (nanos ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15)) | 1
}

/// One `splitmix64` step mapped to `[0, 1)`.
fn next_unit_f64(state: &mut u64) -> f64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    // Take the top 53 bits for a uniform double in [0, 1).
    (z >> 11) as f64 / (1u64 << 53) as f64
}

/// The explicit read/write classification of a `QueryBuilder` wire command, used to decide retry
/// eligibility. An allowlist of idempotent commands defaulting to [`OpKind::Write`], so an
/// unrecognized (or future) command is never retried until it is deliberately classified here.
/// `GRAPH.EXPLAIN` is read-only even though it is not `GRAPH.RO_QUERY`; `GRAPH.PROFILE` runs the
/// query and is therefore a write.
pub(crate) fn op_kind_for_command(command: &str) -> OpKind {
    match command {
        "GRAPH.RO_QUERY" | "GRAPH.EXPLAIN" => OpKind::ReadOnly,
        _ => OpKind::Write,
    }
}

/// Run a blocking operation under `policy`, retrying transient failures eligible for `kind`.
///
/// When the policy [is disabled](RetryPolicy::is_disabled) the operation runs exactly once with no
/// retry machinery, guaranteeing byte-identical behavior to having no policy.
pub(crate) fn run_with_retry_blocking<T>(
    policy: &RetryPolicy,
    kind: OpKind,
    mut attempt: impl FnMut() -> FalkorResult<T>,
) -> FalkorResult<T> {
    if policy.is_disabled() {
        return attempt();
    }
    use backon::BlockingRetryable as _;
    attempt
        .retry(policy.backon_builder())
        .when(|err: &FalkorDBError| policy.should_retry(kind, err))
        .call()
}

/// Async counterpart of [`run_with_retry_blocking`]. The `attempt` closure is re-invoked per try, so
/// each attempt re-borrows a fresh connection (the healed one) before re-sending the command.
#[cfg(feature = "tokio")]
pub(crate) async fn run_with_retry_async<T, Fut>(
    policy: &RetryPolicy,
    kind: OpKind,
    mut attempt: impl FnMut() -> Fut,
) -> FalkorResult<T>
where
    Fut: std::future::Future<Output = FalkorResult<T>>,
{
    if policy.is_disabled() {
        return attempt().await;
    }
    use backon::Retryable as _;
    attempt
        .retry(policy.backon_builder())
        .when(|err: &FalkorDBError| policy.should_retry(kind, err))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_is_disabled() {
        assert_eq!(RetryPolicy::default(), RetryPolicy::disabled());
        assert_eq!(RetryPolicy::default().scope(), RetryScope::Disabled);
        assert!(RetryPolicy::default().is_disabled());
    }

    #[test]
    fn read_only_preset_is_enabled() {
        let policy = RetryPolicy::read_only();
        assert_eq!(policy.scope(), RetryScope::ReadOnly);
        assert!(!policy.is_disabled());
    }

    #[test]
    fn max_attempts_one_disables_retry() {
        let policy = RetryPolicy::read_only().max_attempts(1);
        assert!(policy.is_disabled());
    }

    #[test]
    fn max_attempts_is_clamped_to_at_least_one() {
        assert!(RetryPolicy::read_only().max_attempts(0).is_disabled());
    }

    #[test]
    fn disabled_scope_never_retries_any_error() {
        let policy = RetryPolicy::disabled();
        for err in retryable_errors() {
            assert!(!policy.should_retry(OpKind::ReadOnly, &err));
            assert!(!policy.should_retry(OpKind::Write, &err));
        }
    }

    #[test]
    fn read_only_retries_read_ops_on_transient_errors() {
        let policy = RetryPolicy::read_only();
        for err in retryable_errors() {
            assert!(
                policy.should_retry(OpKind::ReadOnly, &err),
                "expected retry for read op on {err:?}"
            );
        }
    }

    #[test]
    fn read_only_never_retries_writes() {
        let policy = RetryPolicy::read_only();
        for err in retryable_errors() {
            assert!(
                !policy.should_retry(OpKind::Write, &err),
                "writes must never be retried, even on {err:?}"
            );
        }
    }

    #[test]
    fn non_transient_errors_are_not_retried() {
        let policy = RetryPolicy::read_only();
        for err in non_retryable_errors() {
            assert!(
                !policy.should_retry(OpKind::ReadOnly, &err),
                "non-transient error must not be retried: {err:?}"
            );
        }
    }

    #[test]
    fn connection_down_healed_into_no_connection_is_still_retryable_for_reads() {
        // The recovery path can convert a post-send ConnectionDown into NoConnection; both remain
        // retryable for reads, and (critically) neither is ever retried for writes in v1.
        let policy = RetryPolicy::read_only();
        assert!(policy.should_retry(OpKind::ReadOnly, &FalkorDBError::NoConnection));
        assert!(!policy.should_retry(OpKind::Write, &FalkorDBError::NoConnection));
    }

    #[test]
    fn command_classification_table() {
        // Read-only commands are an explicit allowlist; everything else (incl. PROFILE, which runs
        // the query) is a write, and unknown commands fall back to the conservative Write default.
        assert_eq!(op_kind_for_command("GRAPH.RO_QUERY"), OpKind::ReadOnly);
        assert_eq!(op_kind_for_command("GRAPH.EXPLAIN"), OpKind::ReadOnly);
        assert_eq!(op_kind_for_command("GRAPH.QUERY"), OpKind::Write);
        assert_eq!(op_kind_for_command("GRAPH.PROFILE"), OpKind::Write);
        assert_eq!(op_kind_for_command("SOMETHING.NEW"), OpKind::Write);
    }

    #[test]
    fn backoff_yields_max_attempts_minus_one_delays() {
        use backon::BackoffBuilder as _;
        let delays: Vec<Duration> = RetryPolicy::read_only()
            .max_attempts(5)
            .backon_builder()
            .build()
            .collect();
        assert_eq!(
            delays.len(),
            4,
            "N total attempts means N-1 inter-attempt delays"
        );
    }

    #[test]
    fn backoff_exponential_without_jitter_grows_and_caps() {
        use backon::BackoffBuilder as _;
        let delays: Vec<Duration> = RetryPolicy::read_only()
            .max_attempts(6)
            .backoff(
                Backoff::exponential(Duration::from_millis(50))
                    .max_delay(Duration::from_millis(180))
                    .jitter(false),
            )
            .backon_builder()
            .build()
            .collect();
        // 50, 100, 200→180 (capped), 180, 180 — monotonic non-decreasing, never above the cap.
        let cap = Duration::from_millis(180);
        for pair in delays.windows(2) {
            assert!(
                pair[0] <= pair[1],
                "delays must be non-decreasing: {delays:?}"
            );
        }
        assert!(
            delays.iter().all(|d| *d <= cap),
            "no delay may exceed max_delay: {delays:?}"
        );
        assert!(
            delays[2] == cap && delays[delays.len() - 1] == cap,
            "tail must sit at the cap"
        );
    }

    #[test]
    fn backoff_full_jitter_never_exceeds_cap() {
        use backon::BackoffBuilder as _;
        // Many short attempts: every jittered delay must stay within [0, max_delay].
        let cap = Duration::from_millis(40);
        let delays: Vec<Duration> = RetryPolicy::read_only()
            .max_attempts(64)
            .backoff(
                Backoff::exponential(Duration::from_millis(10))
                    .max_delay(cap)
                    .jitter(true),
            )
            .backon_builder()
            .build()
            .collect();
        assert_eq!(delays.len(), 63);
        assert!(
            delays.iter().all(|d| *d <= cap),
            "full jitter must never exceed max_delay: {delays:?}"
        );
    }

    #[test]
    fn backoff_fixed_is_constant() {
        use backon::BackoffBuilder as _;
        let delays: Vec<Duration> = RetryPolicy::read_only()
            .max_attempts(4)
            .backoff(Backoff::fixed(Duration::from_millis(25)))
            .backon_builder()
            .build()
            .collect();
        assert_eq!(delays, vec![Duration::from_millis(25); 3]);
    }

    fn retryable_errors() -> Vec<FalkorDBError> {
        vec![
            FalkorDBError::ConnectionDown,
            FalkorDBError::NoConnection,
            FalkorDBError::EmptyConnection,
            FalkorDBError::SentinelConnection("sentinel down".into()),
        ]
    }

    fn non_retryable_errors() -> Vec<FalkorDBError> {
        vec![
            FalkorDBError::RedisError("ERR syntax error".into()),
            FalkorDBError::ParsingError("bad reply".into()),
            FalkorDBError::InvalidDataReceived,
            FalkorDBError::SentinelMastersCount,
            FalkorDBError::Timeout {
                operation: crate::WaitOperation::IndexCreation,
                timeout: Duration::from_secs(5),
            },
        ]
    }
}

/// Hermetic fault-injection over the blocking runner: a `Cell` counter records attempts while the
/// closure returns a scripted sequence of errors/successes, so the retry semantics are proven
/// without a server.
#[cfg(test)]
mod blocking_runner_tests {
    use super::*;
    use std::cell::Cell;

    /// A read-only policy with zero backoff so tests don't actually sleep between attempts.
    fn fast_read_only(max_attempts: u32) -> RetryPolicy {
        RetryPolicy::read_only()
            .max_attempts(max_attempts)
            .backoff(Backoff::fixed(Duration::ZERO))
    }

    #[test]
    fn disabled_attempts_exactly_once_even_on_transient_error() {
        let calls = Cell::new(0);
        let result: FalkorResult<()> =
            run_with_retry_blocking(&RetryPolicy::disabled(), OpKind::ReadOnly, || {
                calls.set(calls.get() + 1);
                Err(FalkorDBError::ConnectionDown)
            });
        assert!(matches!(result, Err(FalkorDBError::ConnectionDown)));
        assert_eq!(calls.get(), 1, "disabled policy must attempt exactly once");
    }

    #[test]
    fn read_only_retries_until_success() {
        let calls = Cell::new(0);
        let result = run_with_retry_blocking(&fast_read_only(3), OpKind::ReadOnly, || {
            calls.set(calls.get() + 1);
            if calls.get() < 3 {
                Err(FalkorDBError::ConnectionDown)
            } else {
                Ok(42)
            }
        });
        assert_eq!(result.expect("should eventually succeed"), 42);
        assert_eq!(calls.get(), 3);
    }

    #[test]
    fn write_is_never_retried() {
        let calls = Cell::new(0);
        let result: FalkorResult<()> =
            run_with_retry_blocking(&fast_read_only(5), OpKind::Write, || {
                calls.set(calls.get() + 1);
                Err(FalkorDBError::ConnectionDown)
            });
        assert!(matches!(result, Err(FalkorDBError::ConnectionDown)));
        assert_eq!(
            calls.get(),
            1,
            "writes must never be retried under ReadOnly scope"
        );
    }

    #[test]
    fn exhausts_max_attempts_then_returns_last_error() {
        let calls = Cell::new(0);
        let result: FalkorResult<()> =
            run_with_retry_blocking(&fast_read_only(4), OpKind::ReadOnly, || {
                calls.set(calls.get() + 1);
                Err(FalkorDBError::ConnectionDown)
            });
        assert!(matches!(result, Err(FalkorDBError::ConnectionDown)));
        assert_eq!(calls.get(), 4, "should attempt exactly max_attempts times");
    }

    #[test]
    fn non_transient_error_is_not_retried() {
        let calls = Cell::new(0);
        let result: FalkorResult<()> =
            run_with_retry_blocking(&fast_read_only(5), OpKind::ReadOnly, || {
                calls.set(calls.get() + 1);
                Err(FalkorDBError::RedisError("ERR syntax".into()))
            });
        assert!(matches!(result, Err(FalkorDBError::RedisError(_))));
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn connection_down_healed_into_no_connection_is_still_retried_for_reads() {
        // Mirrors the recovery path: ConnectionDown, then a healed NoConnection, then success.
        let calls = Cell::new(0);
        let result = run_with_retry_blocking(&fast_read_only(3), OpKind::ReadOnly, || {
            calls.set(calls.get() + 1);
            match calls.get() {
                1 => Err(FalkorDBError::ConnectionDown),
                2 => Err(FalkorDBError::NoConnection),
                _ => Ok("ok"),
            }
        });
        assert_eq!(result.expect("should succeed after healing"), "ok");
        assert_eq!(calls.get(), 3);
    }
}

/// Async parity of [`blocking_runner_tests`].
#[cfg(all(test, feature = "tokio"))]
mod async_runner_tests {
    use super::*;
    use std::cell::Cell;

    fn fast_read_only(max_attempts: u32) -> RetryPolicy {
        RetryPolicy::read_only()
            .max_attempts(max_attempts)
            .backoff(Backoff::fixed(Duration::ZERO))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disabled_attempts_exactly_once() {
        let calls = Cell::new(0);
        let result: FalkorResult<()> =
            run_with_retry_async(&RetryPolicy::disabled(), OpKind::ReadOnly, || {
                calls.set(calls.get() + 1);
                async { Err(FalkorDBError::ConnectionDown) }
            })
            .await;
        assert!(matches!(result, Err(FalkorDBError::ConnectionDown)));
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_only_retries_until_success() {
        let calls = Cell::new(0);
        let result = run_with_retry_async(&fast_read_only(3), OpKind::ReadOnly, || {
            calls.set(calls.get() + 1);
            let attempt = calls.get();
            async move {
                if attempt < 3 {
                    Err(FalkorDBError::ConnectionDown)
                } else {
                    Ok(7)
                }
            }
        })
        .await;
        assert_eq!(result.expect("should eventually succeed"), 7);
        assert_eq!(calls.get(), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_is_never_retried() {
        let calls = Cell::new(0);
        let result: FalkorResult<()> =
            run_with_retry_async(&fast_read_only(5), OpKind::Write, || {
                calls.set(calls.get() + 1);
                async { Err(FalkorDBError::ConnectionDown) }
            })
            .await;
        assert!(matches!(result, Err(FalkorDBError::ConnectionDown)));
        assert_eq!(calls.get(), 1, "writes must never be retried");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn exhausts_max_attempts_then_returns_last_error() {
        let calls = Cell::new(0);
        let result: FalkorResult<()> =
            run_with_retry_async(&fast_read_only(4), OpKind::ReadOnly, || {
                calls.set(calls.get() + 1);
                async { Err(FalkorDBError::ConnectionDown) }
            })
            .await;
        assert!(matches!(result, Err(FalkorDBError::ConnectionDown)));
        assert_eq!(calls.get(), 4);
    }
}
