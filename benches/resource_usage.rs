/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Resource-usage benchmark: **peak memory (RSS)** and **CPU time** per async connection
//! strategy ([`Pooled`] vs [`Multiplexed`]). This complements `async_strategies.rs`, which
//! measures wall-clock throughput/latency with criterion.
//!
//! Why a separate, non-criterion harness?
//!
//! - Peak RSS (`getrusage`'s `ru_maxrss`) is a process-wide **high-water mark** that cannot
//!   be reset between iterations. To compare strategies fairly each one is run in its own
//!   subprocess, so its peak RSS is isolated.
//! - When invoked with no `BENCH_STRATEGY` env var the harness acts as a *driver*: it
//!   re-execs itself once per strategy (clean process each time) and prints a table.
//! - When `BENCH_STRATEGY` is set (e.g. `pooled:32`, `multiplexed:1`) it runs that single
//!   strategy and prints one row of metrics.
//!
//! Each run drives a fixed total number of short read queries at a fixed concurrency, so
//! the CPU/memory cost of each strategy's machinery (borrow-pool + per-command task spawn
//! vs. round-robin multiplexing) is what differs.
//!
//! Requires a running FalkorDB (`FALKORDB_HOST` / `FALKORDB_PORT`, defaults
//! `127.0.0.1:6379`); when none is reachable it prints a notice and exits cleanly.
//!
//! Run with:
//!
//! ```sh
//! docker run -d --name falkordb-bench -p 6379:6379 falkordb/falkordb:latest
//! cargo bench --features tokio --bench resource_usage
//! ```
//!
//! [`Pooled`]: falkordb::ConnectionStrategy::Pooled
//! [`Multiplexed`]: falkordb::ConnectionStrategy::Multiplexed

use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::{Duration, Instant};

use falkordb::{ConnectionStrategy, FalkorAsyncClient};
use tokio::runtime::Runtime;

mod common;
use common::{build_client, STRATEGY_COUNTS};

/// Total queries executed per strategy run.
const TOTAL_QUERIES: usize = 20_000;
/// Number of queries kept in flight at once.
const CONCURRENCY: usize = 64;

/// Strategies compared: pooled and multiplexed at each connection count.
fn strategy_matrix() -> Vec<(String, ConnectionStrategy)> {
    let mut out = Vec::new();
    for n in STRATEGY_COUNTS {
        let count = NonZeroU8::new(n).unwrap();
        out.push((
            format!("pooled:{n}"),
            ConnectionStrategy::Pooled { size: count },
        ));
        out.push((
            format!("multiplexed:{n}"),
            ConnectionStrategy::Multiplexed { connections: count },
        ));
    }
    out
}

/// Parse a `kind:count` spec such as `multiplexed:8` into a [`ConnectionStrategy`].
fn parse_strategy(spec: &str) -> Option<ConnectionStrategy> {
    let (kind, count) = spec.split_once(':')?;
    let count = NonZeroU8::new(count.parse().ok()?)?;
    match kind {
        "pooled" => Some(ConnectionStrategy::Pooled { size: count }),
        "multiplexed" => Some(ConnectionStrategy::Multiplexed { connections: count }),
        _ => None,
    }
}

/// Drive `TOTAL_QUERIES` short read queries, keeping `CONCURRENCY` in flight.
async fn run_workload(
    client: Arc<FalkorAsyncClient>,
    graph_name: &str,
) {
    let mut remaining = TOTAL_QUERIES;
    while remaining > 0 {
        let batch = remaining.min(CONCURRENCY);
        let handles: Vec<_> = (0..batch)
            .map(|_| {
                let client = client.clone();
                let graph_name = graph_name.to_string();
                tokio::spawn(async move {
                    let mut graph = client.select_graph(&graph_name);
                    graph
                        .ro_query("RETURN 1")
                        .execute()
                        .await
                        .expect("resource benchmark query should succeed");
                })
            })
            .collect();
        for handle in handles {
            handle
                .await
                .expect("resource benchmark task should not panic");
        }
        remaining -= batch;
    }
}

/// CPU time consumed by this process so far (user, system).
#[cfg(unix)]
fn cpu_time() -> (Duration, Duration) {
    // SAFETY: `getrusage` only writes into the provided zeroed `rusage` struct.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage
    };
    let to_duration = |tv: libc::timeval| {
        Duration::from_secs(tv.tv_sec as u64) + Duration::from_micros(tv.tv_usec as u64)
    };
    (to_duration(usage.ru_utime), to_duration(usage.ru_stime))
}

#[cfg(not(unix))]
fn cpu_time() -> (Duration, Duration) {
    (Duration::ZERO, Duration::ZERO)
}

/// Peak resident set size of this process, in mebibytes.
#[cfg(unix)]
fn peak_rss_mib() -> f64 {
    // SAFETY: `getrusage` only writes into the provided zeroed `rusage` struct.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage
    };
    // `ru_maxrss` is bytes on macOS/BSD and kibibytes on Linux.
    let max_rss = usage.ru_maxrss as f64;
    if cfg!(target_os = "macos") {
        max_rss / (1024.0 * 1024.0)
    } else {
        max_rss / 1024.0
    }
}

#[cfg(not(unix))]
fn peak_rss_mib() -> f64 {
    f64::NAN
}

/// Run a single strategy and print one row of metrics. Returns `false` if no server.
fn run_single(
    label: &str,
    strategy: ConnectionStrategy,
) -> bool {
    let runtime = Runtime::new().expect("tokio runtime");

    let client = match runtime.block_on(build_client(strategy)) {
        Some(client) => Arc::new(client),
        None => return false,
    };
    let graph_name = format!("resbench_{}", label.replace(':', "_"));

    // Warm the graph and connections so steady-state cost is measured.
    runtime.block_on(async {
        let mut graph = client.select_graph(&graph_name);
        let _ = graph.query("RETURN 1").execute().await;
        run_workload(client.clone(), &graph_name).await;
    });

    let (u0, s0) = cpu_time();
    let wall_start = Instant::now();
    runtime.block_on(run_workload(client.clone(), &graph_name));
    let wall = wall_start.elapsed();
    let (u1, s1) = cpu_time();

    runtime.block_on(async {
        let mut graph = client.select_graph(&graph_name);
        let _ = graph.delete().await;
    });

    let cpu_user = u1.saturating_sub(u0);
    let cpu_sys = s1.saturating_sub(s0);
    let throughput = TOTAL_QUERIES as f64 / wall.as_secs_f64();

    println!(
        "{label:<16} {:>10.1} {:>12.1} {:>12.1} {:>12.1} {:>14.0}",
        peak_rss_mib(),
        cpu_user.as_secs_f64() * 1000.0,
        cpu_sys.as_secs_f64() * 1000.0,
        wall.as_secs_f64() * 1000.0,
        throughput,
    );
    true
}

/// Re-exec this benchmark binary once per strategy so each gets an isolated process
/// (clean peak-RSS measurement).
fn run_driver() {
    // Probe once for a reachable server.
    let runtime = Runtime::new().expect("tokio runtime");
    if runtime
        .block_on(build_client(ConnectionStrategy::Multiplexed {
            connections: NonZeroU8::new(1).unwrap(),
        }))
        .is_none()
    {
        eprintln!(
            "resource_usage: no FalkorDB reachable (set FALKORDB_HOST/FALKORDB_PORT); skipping"
        );
        return;
    }
    drop(runtime);

    println!(
        "{:<16} {:>10} {:>12} {:>12} {:>12} {:>14}",
        "strategy", "peak_rss_MiB", "cpu_user_ms", "cpu_sys_ms", "wall_ms", "queries/sec"
    );

    let exe = std::env::current_exe().expect("current exe");
    for (label, _) in strategy_matrix() {
        let status = std::process::Command::new(&exe)
            .env("BENCH_STRATEGY", &label)
            .status()
            .expect("spawn strategy subprocess");
        if !status.success() {
            eprintln!("resource_usage: subprocess for {label} failed");
        }
    }
}

fn main() {
    match std::env::var("BENCH_STRATEGY") {
        Ok(spec) => {
            let strategy = parse_strategy(&spec).unwrap_or_else(|| {
                eprintln!("resource_usage: invalid BENCH_STRATEGY={spec:?}");
                std::process::exit(2);
            });
            if !run_single(&spec, strategy) {
                eprintln!("resource_usage: no FalkorDB reachable; skipping {spec}");
            }
        }
        Err(_) => run_driver(),
    }
}
