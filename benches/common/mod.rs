/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Helpers shared by the async-strategy benchmarks (`async_strategies` and
//! `resource_usage`). Kept in a non-target subdirectory module so both benches can
//! `mod common;` it without Cargo treating it as its own benchmark.

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use falkordb::{ConnectionStrategy, FalkorAsyncClient, FalkorClientBuilder, FalkorConnectionInfo};

/// Connection counts compared across both benchmarks.
pub const STRATEGY_COUNTS: [u8; 3] = [1, 8, 32];

/// Resolve the target host/port from `FALKORDB_HOST` / `FALKORDB_PORT` (defaults
/// `127.0.0.1:6379`).
///
/// Panics if `FALKORDB_PORT` is set but not a valid port, so a misconfigured benchmark
/// fails loudly instead of silently falling back to the default target.
fn target() -> (String, u16) {
    let host = std::env::var("FALKORDB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = match std::env::var("FALKORDB_PORT") {
        Ok(port) => port
            .parse()
            .expect("FALKORDB_PORT must be a valid u16 port number"),
        Err(_) => 6379,
    };
    (host, port)
}

/// The configured connection info. Panics if the host/port cannot form a valid
/// [`FalkorConnectionInfo`], surfacing configuration errors rather than hiding them.
fn connection_info() -> FalkorConnectionInfo {
    let (host, port) = target();
    FalkorConnectionInfo::try_from((host.as_str(), port))
        .expect("Could not build a FalkorConnectionInfo from FALKORDB_HOST/FALKORDB_PORT")
}

/// Whether the configured server is accepting TCP connections. Used to decide whether to
/// skip (server absent — benches stay runnable in serverless CI) versus fail (server
/// present but the client errors).
fn server_is_reachable() -> bool {
    let (host, port) = target();
    // Resolve and probe with a bounded timeout so an unroutable FALKORDB_HOST skips the
    // benches instead of stalling for the full OS TCP timeout.
    match (host.as_str(), port).to_socket_addrs() {
        Ok(addrs) => addrs
            .into_iter()
            .any(|addr| TcpStream::connect_timeout(&addr, Duration::from_secs(2)).is_ok()),
        Err(_) => false,
    }
}

/// Build an async client for `strategy`, or `None` when no server is reachable (so the
/// benches skip cleanly in serverless CI).
///
/// When the server *is* reachable, a build failure is a real error (config, auth, or a
/// regression) and panics rather than being silently turned into a skipped case.
pub async fn build_client(strategy: ConnectionStrategy) -> Option<FalkorAsyncClient> {
    if !server_is_reachable() {
        return None;
    }
    let client = FalkorClientBuilder::new_async()
        .with_connection_info(connection_info())
        .with_connection_strategy(strategy)
        .build()
        .await
        .expect("FalkorDB is reachable but building the async client failed");
    Some(client)
}
