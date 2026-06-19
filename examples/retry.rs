/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Demonstrates the opt-in retry policy that re-issues eligible operations on transient
//! connection failures.
//!
//! A [`RetryPolicy`] is **disabled by default**, so a client built without one attempts every
//! operation exactly once (the previous behavior). When a policy is configured, read-only /
//! idempotent operations that fail with a transient connection error are automatically retried
//! with bounded backoff. **Writes are never retried**, so enabling a policy can never duplicate a
//! write.
//!
//! Set `FALKORDB_CONNECTION` to point at another server (for example `falkor://127.0.0.1:6379`);
//! it defaults to a local single node.

use falkordb::{Backoff, FalkorClientBuilder, FalkorResult, RetryPolicy};
use std::time::Duration;

fn main() -> FalkorResult<()> {
    let connection_info = std::env::var("FALKORDB_CONNECTION")
        .unwrap_or_else(|_| "falkor://127.0.0.1:6379".to_string());

    // Retry read-only operations on transient connection failures: up to 4 attempts total
    // (1 initial try + 3 retries), with exponential backoff starting at 50ms and capped at 1s.
    let policy = RetryPolicy::read_only()
        .max_attempts(4)
        .backoff(Backoff::exponential(Duration::from_millis(50)).max_delay(Duration::from_secs(1)));

    let client = FalkorClientBuilder::new()
        .with_connection_info(connection_info.as_str().try_into()?)
        .with_retry_policy(policy)
        .build()?;

    let mut graph = client.select_graph("retry_example");

    // Writes are NEVER auto-retried, even with a policy enabled, so a write can never be applied
    // twice. Classification is by the API you call: `query()` is always treated as a write.
    graph
        .query("CREATE (:Greeting {text: 'hello'})")
        .execute()?;

    // Read-only queries ARE eligible for retry. If the connection drops transiently here, the
    // client re-borrows a healed connection and re-issues the query (up to `max_attempts`), instead
    // of surfacing the error on the first blip. Use `ro_query()` (not `query()`) for retryable reads.
    let mut greetings = graph
        .ro_query("MATCH (g:Greeting) RETURN g.text")
        .execute()?;

    for row in greetings.data.by_ref() {
        println!("read-only result: {row:?}");
    }

    graph.delete()?;

    Ok(())
}
