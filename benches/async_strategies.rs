/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Benchmarks comparing the async connection strategies ([`Pooled`] vs
//! [`Multiplexed`]). These are developer/PR-time tools and are **not** part of the
//! required CI gates.
//!
//! They require a running FalkorDB instance. Configure with `FALKORDB_HOST` /
//! `FALKORDB_PORT` (defaults: `127.0.0.1:6379`). When no server is reachable the
//! benchmarks skip their work so they remain runnable in serverless CI.
//!
//! Run with:
//!
//! ```sh
//! docker run -d --name falkordb-bench -p 6379:6379 falkordb/falkordb:latest
//! cargo bench --features tokio --bench async_strategies
//! ```
//!
//! [`Pooled`]: falkordb::ConnectionStrategy::Pooled
//! [`Multiplexed`]: falkordb::ConnectionStrategy::Multiplexed

use std::num::NonZeroU8;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use falkordb::{ConnectionStrategy, FalkorAsyncClient, FalkorDBError};
use tokio::runtime::Runtime;

mod common;
use common::{build_client, STRATEGY_COUNTS};

/// Strategies compared across the matrix: pooled and multiplexed at each connection count.
fn strategy_matrix() -> Vec<(&'static str, ConnectionStrategy)> {
    let mut out = Vec::new();
    for n in STRATEGY_COUNTS {
        let count = NonZeroU8::new(n).unwrap();
        out.push(("pooled", ConnectionStrategy::Pooled { size: count }));
        out.push((
            "multiplexed",
            ConnectionStrategy::Multiplexed { connections: count },
        ));
    }
    out
}

/// Run `concurrency` short read queries concurrently and wait for all of them.
async fn run_concurrent_reads(
    client: Arc<FalkorAsyncClient>,
    graph_name: &str,
    concurrency: usize,
) {
    let handles: Vec<_> = (0..concurrency)
        .map(|_| {
            let client = client.clone();
            let graph_name = graph_name.to_string();
            tokio::spawn(async move {
                let mut graph = client.select_graph(&graph_name);
                match graph.ro_query("RETURN 1").execute().await {
                    Ok(_) => {}
                    // FalkorDB sheds load once its pending-query queue is full. That is
                    // server-side backpressure (not a client/strategy failure), so at high
                    // concurrency we tolerate it instead of aborting the whole bench run.
                    Err(FalkorDBError::RedisError(msg))
                        if msg.contains("pending queries exceeded") => {}
                    Err(e) => panic!("benchmark query should succeed: {e:?}"),
                }
            })
        })
        .collect();
    for handle in handles {
        handle.await.expect("benchmark task should not panic");
    }
}

fn bench_throughput(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");

    // Probe once: if no server is reachable, skip the whole benchmark group.
    if runtime.block_on(connection_probe()).is_none() {
        eprintln!(
            "async_strategies: no FalkorDB reachable (set FALKORDB_HOST/FALKORDB_PORT); skipping"
        );
        return;
    }

    let concurrency_levels = [1usize, 8, 64, 256];
    let mut group = c.benchmark_group("async_read_throughput");

    for (label, strategy) in strategy_matrix() {
        let client = match runtime.block_on(build_client(strategy)) {
            Some(client) => Arc::new(client),
            None => continue,
        };
        let graph_name = format!("bench_{label}_{}", strategy.connection_count().get());
        // Warm the graph so the benchmark measures steady-state command latency.
        runtime.block_on(async {
            let mut graph = client.select_graph(&graph_name);
            let _ = graph.query("RETURN 1").execute().await;
        });

        for &concurrency in &concurrency_levels {
            group.throughput(Throughput::Elements(concurrency as u64));
            let id = BenchmarkId::new(
                format!("{label}_{}", strategy.connection_count().get()),
                concurrency,
            );
            group.bench_with_input(id, &concurrency, |b, &concurrency| {
                b.to_async(&runtime)
                    .iter(|| run_concurrent_reads(client.clone(), &graph_name, concurrency));
            });
        }

        runtime.block_on(async {
            let mut graph = client.select_graph(&graph_name);
            let _ = graph.delete().await;
        });
    }

    group.finish();
}

async fn connection_probe() -> Option<()> {
    build_client(ConnectionStrategy::Multiplexed {
        connections: NonZeroU8::new(1).unwrap(),
    })
    .await
    .map(|_| ())
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
