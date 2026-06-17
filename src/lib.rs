/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

#![recursion_limit = "256"]
#![allow(private_interfaces)]
#![allow(private_bounds)]
#![allow(mismatched_lifetime_syntaxes)]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![doc = include_str!("../README.md")]

mod client;
mod connection;
mod connection_info;
#[cfg(feature = "embedded")]
mod embedded;
mod error;
mod graph;
mod graph_schema;
mod parser;
mod response;
mod value;

/// A [`Result`] which only returns [`FalkorDBError`] as its E type
pub type FalkorResult<T> = Result<T, FalkorDBError>;

pub use client::{blocking::FalkorSyncClient, builder::FalkorClientBuilder, ConnectionStrategy};
pub use connection_info::FalkorConnectionInfo;
pub use error::FalkorDBError;
pub use graph::{
    blocking::SyncGraph,
    ops::{ConstraintOpBuilder, CopyGraphBuilder, IndexOpBuilder, WaitOperation, WaitOptions},
    query_builder::{ProcedureQueryBuilder, QueryBuilder},
};
pub use graph_schema::{GraphSchema, SchemaType};
pub use response::{
    constraint::{Constraint, ConstraintStatus, ConstraintType},
    execution_plan::ExecutionPlan,
    index::{FalkorIndex, IndexStatus, IndexType},
    lazy_result_set::LazyResultSet,
    row::Row,
    slowlog_entry::SlowlogEntry,
    QueryResult,
};
pub use value::{
    config::ConfigValue,
    graph_entities::{Edge, EntityType, Node},
    path::Path,
    point::Point,
    to_cypher_param, FalkorParams, FalkorValue, FromFalkorValue, IntoFalkorParam, IntoFalkorParams,
    RawParam,
};

#[cfg(feature = "serde")]
pub use response::typed_result_set::TypedLazyResultSet;
#[cfg(feature = "serde")]
pub use value::{from_falkor_row, from_falkor_value, FalkorValueDeserializer};

#[cfg(feature = "tokio")]
pub use client::asynchronous::FalkorAsyncClient;
#[cfg(feature = "tokio")]
pub use graph::asynchronous::AsyncGraph;
#[cfg(feature = "tokio")]
pub use graph::ops::{AsyncConstraintOpBuilder, AsyncCopyGraphBuilder, AsyncIndexOpBuilder};

#[cfg(feature = "embedded")]
pub use embedded::{EmbeddedConfig, EmbeddedServer};

#[cfg(test)]
pub(crate) mod test_utils {
    use super::*;

    pub(crate) struct TestSyncGraphHandle {
        pub(crate) inner: SyncGraph,
    }

    impl Drop for TestSyncGraphHandle {
        fn drop(&mut self) {
            self.inner.delete().ok();
        }
    }

    #[cfg(feature = "tokio")]
    pub(crate) struct TestAsyncGraphHandle {
        pub(crate) inner: AsyncGraph,
    }

    #[cfg(feature = "tokio")]
    impl Drop for TestAsyncGraphHandle {
        fn drop(&mut self) {
            tokio::task::block_in_place(|| {
                // Avoid copying the schema each time
                let mut graph_handle =
                    AsyncGraph::new(self.inner.get_client().clone(), self.inner.graph_name());
                tokio::runtime::Handle::current().block_on(async move {
                    graph_handle.delete().await.ok();
                })
            })
        }
    }

    pub(crate) fn create_test_client() -> FalkorSyncClient {
        FalkorClientBuilder::new()
            .build()
            .expect("Could not create client")
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn create_async_test_client() -> FalkorAsyncClient {
        FalkorClientBuilder::new_async()
            .build()
            .await
            .expect("Could not create client")
    }

    /// Name of the shared, read-only graph fixture loaded by
    /// `resources/populate_graph.py`.
    pub(crate) const IMDB_FIXTURE_GRAPH: &str = "imdb";

    /// Shown when the shared `imdb` fixture is missing, so that a fixture problem is never
    /// mistaken for a code regression introduced by a change.
    const IMDB_FIXTURE_HINT: &str = "the shared `imdb` test graph is empty or missing. Run \
        `resources/populate_graph.py` against the test server first (CI does this in the \
        coverage job's \"Populate test graph\" step). This indicates a missing test fixture, \
        NOT a code regression.";

    /// Number of `actor` nodes in the shared `imdb` fixture.
    ///
    /// A query or parsing failure here is a real connectivity/code problem (not a missing
    /// fixture), so it panics with the underlying error rather than being collapsed into a
    /// zero count. Only an actual count of `0` indicates an empty/missing fixture, which the
    /// caller turns into the [`IMDB_FIXTURE_HINT`] message.
    fn imdb_actor_count(graph: &mut SyncGraph) -> i64 {
        let mut result = graph
            .ro_query("MATCH (a:actor) RETURN count(a)")
            .execute()
            .expect("failed to query the imdb actor count (connection or query regression)");
        result
            .data
            .next()
            .and_then(|row| row.ok())
            .and_then(|row| row.try_get_at::<i64>(0).ok())
            .expect("imdb actor count query returned an unexpected shape")
    }

    /// Create a sync client and assert the shared `imdb` fixture is populated, so every
    /// fixture-dependent test fails fast with an actionable message (rather than a cryptic
    /// assertion or a vacuous pass on an empty graph) when the fixture is missing.
    pub(crate) fn imdb_test_client() -> FalkorSyncClient {
        let client = create_test_client();
        let mut graph = client.select_graph(IMDB_FIXTURE_GRAPH);
        assert!(imdb_actor_count(&mut graph) > 0, "{IMDB_FIXTURE_HINT}");
        client
    }

    /// Async counterpart of [`imdb_test_client`].
    #[cfg(feature = "tokio")]
    pub(crate) async fn imdb_async_test_client() -> FalkorAsyncClient {
        let client = create_async_test_client().await;
        let mut graph = client.select_graph(IMDB_FIXTURE_GRAPH);
        let mut result = graph
            .ro_query("MATCH (a:actor) RETURN count(a)")
            .execute()
            .await
            .expect("failed to query the imdb actor count (connection or query regression)");
        let count = result
            .data
            .next()
            .and_then(|row| row.ok())
            .and_then(|row| row.try_get_at::<i64>(0).ok())
            .expect("imdb actor count query returned an unexpected shape");
        assert!(count > 0, "{IMDB_FIXTURE_HINT}");
        client
    }

    pub(crate) fn open_empty_test_graph(graph_name: &str) -> TestSyncGraphHandle {
        let client = create_test_client();

        TestSyncGraphHandle {
            inner: client.select_graph(graph_name),
        }
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn open_empty_async_test_graph(graph_name: &str) -> TestAsyncGraphHandle {
        let client = create_async_test_client().await;

        TestAsyncGraphHandle {
            inner: client.select_graph(graph_name),
        }
    }

    const RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
    /// `GRAPH.COPY` is performed by a background fork on the server, which can take
    /// noticeably longer than index/constraint readiness under heavy load, so its
    /// re-issue loop is given a longer window than [`RETRY_TIMEOUT`].
    pub(crate) const COPY_RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// FalkorDB builds indices and validates constraints asynchronously, so a freshly
    /// created index/constraint may not be reported by the matching `list_*` call that
    /// immediately follows creation, especially while the server is under load. Retry
    /// `op` until `done` is satisfied or the timeout elapses, returning the last value.
    pub(crate) fn retry_until<T>(
        op: impl FnMut() -> T,
        done: impl Fn(&T) -> bool,
    ) -> T {
        retry_until_with_timeout(RETRY_TIMEOUT, op, done)
    }

    /// [`retry_until`] with an explicit overall timeout, for operations that need a
    /// different retry window than the shared default (e.g. the `GRAPH.COPY` re-issue
    /// loop, which uses [`COPY_RETRY_TIMEOUT`]).
    pub(crate) fn retry_until_with_timeout<T>(
        timeout: std::time::Duration,
        mut op: impl FnMut() -> T,
        done: impl Fn(&T) -> bool,
    ) -> T {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let value = op();
            if done(&value) || std::time::Instant::now() >= deadline {
                return value;
            }
            std::thread::sleep(RETRY_INTERVAL);
        }
    }

    /// Async counterpart of [`retry_until`]. Written with an explicit
    /// `FnMut(&mut AsyncGraph) -> Pin<Box<dyn Future>>` bound (rather than an async
    /// closure / `AsyncFnMut`) so it compiles on all stable toolchains.
    #[cfg(feature = "tokio")]
    pub(crate) async fn retry_until_async<T>(
        graph: &mut AsyncGraph,
        mut op: impl for<'a> FnMut(
            &'a mut AsyncGraph,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + 'a>>,
        done: impl Fn(&T) -> bool,
    ) -> T {
        let deadline = std::time::Instant::now() + RETRY_TIMEOUT;
        loop {
            let value = op(graph).await;
            if done(&value) || std::time::Instant::now() >= deadline {
                return value;
            }
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
    }

    /// Async counterpart of [`retry_until`] for self-contained operations that do
    /// not borrow a long-lived `&mut AsyncGraph` (so the future can capture
    /// everything it needs, e.g. a shared `&FalkorAsyncClient`). Used for server
    /// operations performed by a background fork (e.g. `GRAPH.COPY`) that must be
    /// re-issued until they take effect.
    #[cfg(feature = "tokio")]
    pub(crate) async fn retry_until_async_fn<T, Fut>(
        op: impl FnMut() -> Fut,
        done: impl Fn(&T) -> bool,
    ) -> T
    where
        Fut: std::future::Future<Output = T>,
    {
        retry_until_async_fn_with_timeout(RETRY_TIMEOUT, op, done).await
    }

    /// [`retry_until_async_fn`] with an explicit overall timeout, for operations that
    /// need a different retry window than the shared default (e.g. the `GRAPH.COPY`
    /// re-issue loop, which uses [`COPY_RETRY_TIMEOUT`]).
    #[cfg(feature = "tokio")]
    pub(crate) async fn retry_until_async_fn_with_timeout<T, Fut>(
        timeout: std::time::Duration,
        mut op: impl FnMut() -> Fut,
        done: impl Fn(&T) -> bool,
    ) -> T
    where
        Fut: std::future::Future<Output = T>,
    {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let value = op().await;
            if done(&value) || std::time::Instant::now() >= deadline {
                return value;
            }
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
    }
}

#[cfg(test)]
mod retry_tests {
    use super::test_utils::retry_until;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn retry_until_returns_immediately_when_already_done() {
        let calls = AtomicUsize::new(0);
        let value = retry_until(|| calls.fetch_add(1, Ordering::Relaxed) + 1, |v| *v == 1);
        assert_eq!(value, 1);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn retry_until_polls_until_condition_is_met() {
        let calls = AtomicUsize::new(0);
        let value = retry_until(|| calls.fetch_add(1, Ordering::Relaxed) + 1, |v| *v == 3);
        assert_eq!(value, 3);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread")]
    async fn retry_until_async_fn_polls_until_condition_is_met() {
        use super::test_utils::retry_until_async_fn;
        let calls = AtomicUsize::new(0);
        let value = retry_until_async_fn(
            || async { calls.fetch_add(1, Ordering::Relaxed) + 1 },
            |v| *v == 3,
        )
        .await;
        assert_eq!(value, 3);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }
}
