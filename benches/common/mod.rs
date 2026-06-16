/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Helpers shared by the async-strategy benchmarks (`async_strategies` and
//! `resource_usage`). Kept in a non-target subdirectory module so both benches can
//! `mod common;` it without Cargo treating it as its own benchmark.

use falkordb::{ConnectionStrategy, FalkorAsyncClient, FalkorClientBuilder, FalkorConnectionInfo};

/// Connection counts compared across both benchmarks.
pub const STRATEGY_COUNTS: [u8; 3] = [1, 8, 32];

/// Resolve the target server from `FALKORDB_HOST` / `FALKORDB_PORT` (defaults
/// `127.0.0.1:6379`), or `None` when the address is unparseable.
pub fn connection_info() -> Option<FalkorConnectionInfo> {
    let host = std::env::var("FALKORDB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("FALKORDB_PORT")
        .unwrap_or_else(|_| "6379".to_string())
        .parse()
        .unwrap_or(6379);
    FalkorConnectionInfo::try_from((host.as_str(), port)).ok()
}

/// Build an async client for `strategy`, or `None` when no server is reachable.
pub async fn build_client(strategy: ConnectionStrategy) -> Option<FalkorAsyncClient> {
    let conn_info = connection_info()?;
    FalkorClientBuilder::new_async()
        .with_connection_info(conn_info)
        .with_connection_strategy(strategy)
        .build()
        .await
        .ok()
}
