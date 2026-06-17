/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Demonstrates the multiplexed async connection strategy: many concurrent queries
//! share a small number of auto-reconnecting sockets, each carrying several in-flight
//! commands at once.
//!
//! Run a FalkorDB instance first (e.g. `docker run -p 6379:6379 falkordb/falkordb`), then
//! `cargo run --example multiplexed_async --features tokio`.

use falkordb::{ConnectionStrategy, FalkorClientBuilder, FalkorResult};
use std::num::NonZeroU8;
use std::sync::Arc;
use tokio::task::JoinSet;

// Usage of the asynchronous client REQUIRES the multi-threaded rt
#[tokio::main(flavor = "multi_thread")]
async fn main() -> FalkorResult<()> {
    // Explicitly select the multiplexed strategy with 4 shared sockets. This is also the
    // default for `new_async()` (with 8 sockets), so `with_connection_strategy` here is
    // purely to make the choice visible.
    let client = Arc::new(
        FalkorClientBuilder::new_async()
            .with_connection_info("falkor://127.0.0.1:6379".try_into()?)
            .with_connection_strategy(ConnectionStrategy::Multiplexed {
                connections: NonZeroU8::new(4).unwrap(),
            })
            .build()
            .await?,
    );

    println!(
        "effective strategy: {:?} ({} connections)",
        client.connection_strategy(),
        client.connection_pool_size()
    );

    // Fire many independent queries concurrently. With multiplexing, these are pipelined
    // over the shared sockets rather than each waiting for an exclusive connection.
    let mut join_set = JoinSet::new();
    for i in 0..64 {
        let client = client.clone();
        join_set.spawn(async move {
            let mut graph = client.select_graph("multiplexed_demo");
            let mut res = graph.ro_query("RETURN 1").execute().await?;
            let value = res
                .data
                .next()
                .and_then(|row| row.ok().and_then(|r| r.try_get_at::<i64>(0).ok()));
            FalkorResult::Ok((i, value))
        });
    }

    let mut completed = 0;
    while let Some(joined) = join_set.join_next().await {
        let (_i, _value) = joined.expect("task should not panic")?;
        completed += 1;
    }
    println!("completed {completed} concurrent queries over 4 multiplexed sockets");

    client.select_graph("multiplexed_demo").delete().await.ok();

    Ok(())
}
