/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Comprehensive integration tests for the **embedded** FalkorDB server, across every
//! client flavour:
//!
//! - the synchronous client (always `Pooled`),
//! - the asynchronous client under `Pooled`, `Multiplexed { 8 }` (the default) and
//!   `Multiplexed { 1 }` (the strongest test of single-socket multiplexing).
//!
//! These tests spawn a real `redis-server` with the FalkorDB module loaded and talk to it
//! over a Unix-domain socket — the embedded code path exercises the same multiplexed
//! connection management as a remote deployment, minus Sentinel.
//!
//! ## Requirements / skipping
//!
//! Running an embedded server needs a `redis-server` binary and a FalkorDB module
//! (`falkordb.so`) that is *binary-compatible with that redis-server*. Because that pair
//! is platform-specific, these tests are **opt-in**:
//!
//! - Set `FALKORDB_MODULE_PATH` to a `falkordb.so` compatible with your `redis-server`
//!   (or place it in one of the common module locations the client searches).
//! - Optionally set `REDIS_SERVER_PATH`; otherwise `redis-server` is resolved from `PATH`.
//! - Set `SKIP_INTEGRATION_TESTS` to force-skip.
//!
//! When the module cannot be resolved the tests no-op (return early) so they stay green on
//! machines that lack a compatible build (e.g. macOS hosts with a Linux-only module).

#![cfg(feature = "embedded")]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use falkordb::{
    EmbeddedConfig, EmbeddedServer, EntityType, FalkorClientBuilder, FalkorConnectionInfo,
    IndexType,
};
use futures::StreamExt;

/// Common locations the module may live in, mirroring the client's own search list.
fn common_module_paths() -> Vec<PathBuf> {
    vec![
        PathBuf::from("/usr/lib/redis/modules/falkordb.so"),
        PathBuf::from("/usr/local/lib/redis/modules/falkordb.so"),
        PathBuf::from("/opt/homebrew/lib/redis/modules/falkordb.so"),
        PathBuf::from("./falkordb.so"),
    ]
}

/// Resolve a usable FalkorDB module path, or `None` when embedded tests should be skipped.
fn resolve_module_path() -> Option<PathBuf> {
    if std::env::var("SKIP_INTEGRATION_TESTS").is_ok() {
        return None;
    }
    if let Ok(path) = std::env::var("FALKORDB_MODULE_PATH") {
        let path = PathBuf::from(path);
        return path.exists().then_some(path);
    }
    common_module_paths().into_iter().find(|p| p.exists())
}

/// Build an [`EmbeddedConfig`] pointing at a resolved module, or `None` to skip. Sets a
/// short, explicit `/tmp` socket path that is unique per server instance (pid + an atomic
/// nonce) so concurrent tests never collide on the same socket, while staying well under
/// the OS path-length limit even when the system temp dir is long (e.g. on macOS/CI).
fn embedded_config() -> Option<EmbeddedConfig> {
    static SOCKET_NONCE: AtomicU32 = AtomicU32::new(0);
    let module = resolve_module_path()?;
    Some(EmbeddedConfig {
        redis_server_path: std::env::var("REDIS_SERVER_PATH").ok().map(PathBuf::from),
        falkordb_module_path: Some(module),
        socket_path: Some(PathBuf::from(format!(
            "/tmp/fdb-emb-{}-{}.sock",
            std::process::id(),
            SOCKET_NONCE.fetch_add(1, Ordering::Relaxed)
        ))),
        ..Default::default()
    })
}

/// Retry an operation until `done` is satisfied or the deadline elapses. FalkorDB builds
/// indices asynchronously, so a freshly created index may not be listed immediately.
fn retry_until<T>(
    mut op: impl FnMut() -> T,
    done: impl Fn(&T) -> bool,
) -> T {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let value = op();
        if done(&value) || std::time::Instant::now() >= deadline {
            return value;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------------------
// Synchronous flavour
// ---------------------------------------------------------------------------------------

#[test]
fn test_embedded_sync_full_surface() {
    let Some(config) = embedded_config() else {
        return;
    };
    let client = FalkorClientBuilder::new()
        .with_connection_info(FalkorConnectionInfo::Embedded(config))
        .build()
        .expect("embedded sync client should build");

    // The sync client is always pooled.
    assert!(client.connection_pool_size() >= 1);

    let mut graph = client.select_graph("embedded_sync");
    let _ = graph.delete();
    let mut graph = client.select_graph("embedded_sync");

    graph
        .query("CREATE (:Person {name: 'Alice', age: 30}), (:Person {name: 'Bob', age: 40})")
        .execute()
        .expect("write query should succeed");

    // Parameterized read.
    let mut res = graph
        .query("MATCH (p:Person) WHERE p.age >= $min_age RETURN p.age")
        .with_param("min_age", 35)
        .execute()
        .expect("parameterized query should succeed");
    let ages: Vec<i64> = res
        .data
        .by_ref()
        .map(|row| {
            row.expect("row should parse")
                .try_get_at::<i64>(0)
                .expect("column 0 should be an i64")
        })
        .collect();
    assert_eq!(ages, vec![40]);

    // Read-only query.
    let mut ro = graph
        .ro_query("MATCH (p:Person) RETURN count(p)")
        .execute()
        .expect("read-only query should succeed");
    let count = ro
        .data
        .next()
        .expect("expected a row")
        .expect("row should parse")
        .try_get_at::<i64>(0)
        .expect("column 0 should be an i64");
    assert_eq!(count, 2);

    // Index create + asynchronous listing.
    graph
        .create_index(IndexType::Range, EntityType::Node, "Person", &["age"], None)
        .expect("index creation should succeed");
    let indices = retry_until(
        || graph.list_indices().expect("list indices"),
        |res| !res.data.is_empty(),
    );
    assert!(!indices.data.is_empty(), "index should be listed");

    graph.delete().expect("cleanup");
}

// ---------------------------------------------------------------------------------------
// Asynchronous flavours
// ---------------------------------------------------------------------------------------

#[cfg(feature = "tokio")]
mod async_flavours {
    use super::*;
    use falkordb::{ConnectionStrategy, FalkorAsyncClient};
    use std::num::NonZeroU8;
    use std::sync::Arc;

    fn strategies() -> [ConnectionStrategy; 3] {
        [
            ConnectionStrategy::Pooled {
                size: NonZeroU8::new(4).unwrap(),
            },
            ConnectionStrategy::Multiplexed {
                connections: NonZeroU8::new(8).unwrap(),
            },
            ConnectionStrategy::Multiplexed {
                connections: NonZeroU8::new(1).unwrap(),
            },
        ]
    }

    async fn build_async(strategy: ConnectionStrategy) -> Option<FalkorAsyncClient> {
        let config = embedded_config()?;
        Some(
            FalkorClientBuilder::new_async()
                .with_connection_info(FalkorConnectionInfo::Embedded(config))
                .with_connection_strategy(strategy)
                .build()
                .await
                .expect("embedded async client should build for an available module"),
        )
    }

    /// Run the full async API surface against an embedded server under every strategy.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_embedded_async_full_surface_all_strategies() {
        if embedded_config().is_none() {
            return;
        }
        for strategy in strategies() {
            let client = build_async(strategy)
                .await
                .expect("embedded async client should build for an available module");

            // Embedded is non-Sentinel, so the requested strategy is used as-is (no
            // downgrade) and reported back.
            assert_eq!(client.connection_strategy(), strategy);

            let graph_name = format!("embedded_async_{}", strategy.connection_count().get());
            let _ = client.select_graph(&graph_name).delete().await;
            let mut graph = client.select_graph(&graph_name);

            graph
                .query("UNWIND range(1, 10) AS i CREATE (:N {v: i})")
                .execute()
                .await
                .expect("write query should succeed");

            let mut res = graph
                .query("MATCH (n:N) WHERE n.v > $threshold RETURN count(n)")
                .with_param("threshold", 5)
                .execute()
                .await
                .expect("parameterized query should succeed");
            let count = res
                .data
                .next()
                .await
                .expect("expected a row")
                .expect("row should parse")
                .try_get_at::<i64>(0)
                .expect("column 0 should be an i64");
            assert_eq!(count, 5, "strategy {strategy:?} parameterized count");

            let mut ro = graph
                .ro_query("MATCH (n:N) RETURN count(n)")
                .execute()
                .await
                .expect("read-only query should succeed");
            let count = ro
                .data
                .next()
                .await
                .expect("expected a row")
                .expect("row should parse")
                .try_get_at::<i64>(0)
                .expect("column 0 should be an i64");
            assert_eq!(count, 10, "strategy {strategy:?} read-only count");

            graph.delete().await.expect("cleanup");
        }
    }

    /// Fire many concurrent queries over a single multiplexed embedded socket and assert
    /// every response is routed back to the correct caller. This is the embedded analogue
    /// of the TCP response-routing parity test, and the strongest check that multiplexing
    /// works over a Unix socket.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_embedded_async_concurrent_single_socket() {
        const CONCURRENCY: i64 = 200;

        if embedded_config().is_none() {
            return;
        }
        let client = build_async(ConnectionStrategy::Multiplexed {
            connections: NonZeroU8::new(1).unwrap(),
        })
        .await
        .expect("embedded async client should build for an available module");
        let client = Arc::new(client);
        let graph_name = "embedded_async_concurrent";
        let _ = client.select_graph(graph_name).delete().await;

        let handles: Vec<_> = (0..CONCURRENCY)
            .map(|i| {
                let client = client.clone();
                tokio::spawn(async move {
                    let mut graph = client.select_graph(graph_name);
                    let mut res = graph
                        .query(format!("RETURN {i}"))
                        .execute()
                        .await
                        .expect("concurrent query should succeed");
                    res.data
                        .next()
                        .await
                        .expect("expected a row")
                        .expect("row should parse")
                        .try_get_at::<i64>(0)
                        .expect("column 0 should be an i64")
                })
            })
            .collect();

        for (expected, handle) in handles.into_iter().enumerate() {
            let actual = handle.await.expect("task should not panic");
            assert_eq!(
                actual, expected as i64,
                "embedded multiplexed: response routed to the wrong caller"
            );
        }

        client.select_graph(graph_name).delete().await.ok();
    }
}

// ---------------------------------------------------------------------------------------
// Server lifecycle (independent of any client)
// ---------------------------------------------------------------------------------------

#[test]
fn test_embedded_server_lifecycle() {
    let Some(config) = embedded_config() else {
        return;
    };
    let socket_path;
    {
        let server = EmbeddedServer::start(config).expect("embedded server should start");
        socket_path = server.socket_path().to_path_buf();
        assert!(
            socket_path.exists(),
            "socket should exist while server runs"
        );
        assert_eq!(
            server.connection_string(),
            format!("unix://{}", socket_path.display())
        );
    }
    // After drop the server is shut down and its socket cleaned up.
    assert!(
        !socket_path.exists(),
        "socket should be removed after the server is dropped"
    );
}
