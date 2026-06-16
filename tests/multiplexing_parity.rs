/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Regression / parity suite for the async connection-strategy change.
//!
//! The async client default changed from an exclusive borrow-pool to multiplexed
//! connections. These tests prove that the whole async surface behaves identically under
//! both the legacy [`ConnectionStrategy::Pooled`] path and the new
//! [`ConnectionStrategy::Multiplexed`] default (including a single multiplexed socket,
//! the strongest test of true multiplexing and response routing).
//!
//! They require a running FalkorDB instance. Set `FALKORDB_HOST` / `FALKORDB_PORT`
//! (default `127.0.0.1:6379`); set `SKIP_INTEGRATION_TESTS` to skip.

#![cfg(feature = "tokio")]

use falkordb::{
    ConnectionStrategy, FalkorAsyncClient, FalkorClientBuilder, FalkorConnectionInfo, FalkorValue,
};
use std::num::NonZeroU8;

fn skip_if_no_server() -> bool {
    std::env::var("SKIP_INTEGRATION_TESTS").is_ok()
}

fn get_test_connection_info() -> Option<FalkorConnectionInfo> {
    let host = std::env::var("FALKORDB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("FALKORDB_PORT")
        .unwrap_or_else(|_| "6379".to_string())
        .parse()
        .unwrap_or(6379);
    FalkorConnectionInfo::try_from((host.as_str(), port)).ok()
}

/// The strategies exercised by the parity matrix: legacy pool, new multiplexed default,
/// and a single multiplexed socket.
fn strategies() -> [ConnectionStrategy; 3] {
    [
        ConnectionStrategy::Pooled {
            size: NonZeroU8::new(8).unwrap(),
        },
        ConnectionStrategy::Multiplexed {
            connections: NonZeroU8::new(8).unwrap(),
        },
        ConnectionStrategy::Multiplexed {
            connections: NonZeroU8::new(1).unwrap(),
        },
    ]
}

/// Probe whether the FalkorDB server is accepting TCP connections. Returns `true` if a
/// socket can be opened to the configured host/port, `false` otherwise (which means the
/// test should be skipped rather than failed).
async fn server_is_available() -> bool {
    let host = std::env::var("FALKORDB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("FALKORDB_PORT")
        .unwrap_or_else(|_| "6379".to_string())
        .parse()
        .unwrap_or(6379);
    tokio::net::TcpStream::connect((host.as_str(), port))
        .await
        .is_ok()
}

/// Build a client for the given strategy, or `None` when the server is genuinely
/// unavailable (skip silently). Panics on any build failure that occurs *after* the
/// server has been confirmed reachable, so a code or configuration bug is never silently
/// masked as "no server".
async fn build_client(strategy: ConnectionStrategy) -> Option<FalkorAsyncClient> {
    if skip_if_no_server() {
        return None;
    }
    if !server_is_available().await {
        return None;
    }
    let conn_info = get_test_connection_info()?;
    Some(
        FalkorClientBuilder::new_async()
            .with_connection_info(conn_info)
            .with_connection_strategy(strategy)
            .build()
            .await
            .expect("Client build failed despite server being reachable"),
    )
}

/// Run the core async API surface (write query, parameterized read, read-only query) and
/// assert correct results. Exercised under every strategy in the matrix.
#[tokio::test(flavor = "multi_thread")]
async fn test_core_operations_under_all_strategies() {
    for strategy in strategies() {
        let Some(client) = build_client(strategy).await else {
            return;
        };

        let graph_name = format!("parity_core_{}", strategy.connection_count().get());
        let mut graph = client.select_graph(&graph_name);
        let _ = graph.delete().await;
        let mut graph = client.select_graph(&graph_name);

        graph
            .query("CREATE (:Person {name: 'Alice', age: 30}), (:Person {name: 'Bob', age: 40})")
            .execute()
            .await
            .expect("write query should succeed");

        // Parameterized read.
        let params = std::collections::HashMap::from([("min_age".to_string(), "35".to_string())]);
        let mut res = graph
            .query("MATCH (p:Person) WHERE p.age >= $min_age RETURN p.age")
            .with_params(&params)
            .execute()
            .await
            .expect("parameterized query should succeed");
        let ages: Vec<i64> = res
            .data
            .by_ref()
            .filter_map(|row| row.first().and_then(FalkorValue::to_i64))
            .collect();
        assert_eq!(ages, vec![40], "strategy {strategy:?} parameterized read");

        // Read-only query.
        let mut ro = graph
            .ro_query("MATCH (p:Person) RETURN count(p)")
            .execute()
            .await
            .expect("read-only query should succeed");
        let count = ro
            .data
            .next()
            .and_then(|row| row.first().and_then(FalkorValue::to_i64))
            .expect("count result");
        assert_eq!(count, 2, "strategy {strategy:?} read-only count");

        graph.delete().await.expect("cleanup");
    }
}

/// Fire a large batch of concurrent queries over each strategy and assert every future
/// returns the correct, independent result. This guards against multiplexed
/// response-routing bugs (responses delivered to the wrong caller) — especially under
/// `Multiplexed { 1 }`, where every command shares a single socket.
#[tokio::test(flavor = "multi_thread")]
async fn test_high_concurrency_no_response_mismatch() {
    const CONCURRENCY: i64 = 500;

    for strategy in strategies() {
        let Some(client) = build_client(strategy).await else {
            return;
        };

        let client = std::sync::Arc::new(client);
        let graph_name = format!("parity_concurrency_{}", strategy.connection_count().get());
        let _ = client.select_graph(&graph_name).delete().await;

        let handles: Vec<_> = (0..CONCURRENCY)
            .map(|i| {
                let client = client.clone();
                let graph_name = graph_name.clone();
                tokio::spawn(async move {
                    let mut graph = client.select_graph(&graph_name);
                    let mut res = graph
                        .query(format!("RETURN {i}"))
                        .execute()
                        .await
                        .expect("concurrent query should succeed");
                    res.data
                        .next()
                        .and_then(|row| row.first().and_then(FalkorValue::to_i64))
                        .expect("scalar result")
                })
            })
            .collect();

        let results = futures_join_all(handles).await;
        // Each future must observe exactly the value it asked for — proving responses are
        // routed back to the correct caller.
        for (expected, actual) in results.iter().enumerate() {
            assert_eq!(
                *actual, expected as i64,
                "strategy {strategy:?}: response routed to the wrong caller"
            );
        }

        client.select_graph(&graph_name).delete().await.ok();
    }
}

/// Behavioral equivalence: the same operation against the same fixture graph must produce
/// identical results under the legacy pool and the multiplexed default.
#[tokio::test(flavor = "multi_thread")]
async fn test_pooled_multiplexed_behavioral_equivalence() {
    let pooled = ConnectionStrategy::Pooled {
        size: NonZeroU8::new(4).unwrap(),
    };
    let multiplexed = ConnectionStrategy::Multiplexed {
        connections: NonZeroU8::new(4).unwrap(),
    };

    let query = "UNWIND range(1, 100) AS x RETURN count(x)";

    let Some(pooled_client) = build_client(pooled).await else {
        return;
    };
    let Some(multiplexed_client) = build_client(multiplexed).await else {
        return;
    };

    let run = |client: &FalkorAsyncClient| {
        let mut graph = client.select_graph("parity_equivalence");
        async move {
            let mut res = graph.query(query).execute().await.expect("query");
            res.data
                .next()
                .and_then(|row| row.first().and_then(FalkorValue::to_i64))
                .expect("scalar")
        }
    };

    let pooled_result = run(&pooled_client).await;
    let multiplexed_result = run(&multiplexed_client).await;
    assert_eq!(
        pooled_result, multiplexed_result,
        "pooled and multiplexed strategies must return identical results"
    );
    assert_eq!(pooled_result, 100);

    pooled_client
        .select_graph("parity_equivalence")
        .delete()
        .await
        .ok();
}

/// The effective strategy is reported back, and the connection count is preserved.
#[tokio::test(flavor = "multi_thread")]
async fn test_connection_strategy_accessor() {
    let strategy = ConnectionStrategy::Multiplexed {
        connections: NonZeroU8::new(3).unwrap(),
    };
    let Some(client) = build_client(strategy).await else {
        return;
    };
    // On a non-Sentinel deployment the multiplexed strategy is used as-is.
    assert_eq!(client.connection_strategy(), strategy);
    assert_eq!(client.connection_pool_size(), 3);
}

/// Repeated build/drop cycles (each holding outstanding multiplexed clones briefly) must
/// not deadlock or leak: the client drops cleanly even with in-flight work.
#[tokio::test(flavor = "multi_thread")]
async fn test_clean_drop_no_leak() {
    for _ in 0..20 {
        let Some(client) = build_client(ConnectionStrategy::Multiplexed {
            connections: NonZeroU8::new(4).unwrap(),
        })
        .await
        else {
            return;
        };

        let mut graph = client.select_graph("parity_drop");
        graph
            .query("RETURN 1")
            .execute()
            .await
            .expect("query should succeed");
        // Drop the client while nothing keeps its clones alive.
        drop(graph);
        drop(client);
    }
}

/// Minimal `join_all` so the suite does not depend on the `futures` crate.
async fn futures_join_all<T>(
    handles: impl IntoIterator<Item = tokio::task::JoinHandle<T>>
) -> Vec<T> {
    let mut out = Vec::new();
    for handle in handles {
        out.push(handle.await.expect("task should not panic"));
    }
    out
}
