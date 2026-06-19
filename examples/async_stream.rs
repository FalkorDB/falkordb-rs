/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

#![recursion_limit = "256"]

//! Async streaming results with `futures::StreamExt` / `TryStreamExt`.
//!
//! Run with: `cargo run --example async_stream --features tokio`
//!
//! Requires a running FalkorDB instance (defaults to `127.0.0.1:6379`).

use falkordb::{FalkorClientBuilder, FalkorResult};
use futures::{StreamExt, TryStreamExt};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> FalkorResult<()> {
    let client = FalkorClientBuilder::new_async()
        .with_connection_info("falkor://127.0.0.1:6379".try_into()?)
        .build()
        .await?;
    let mut graph = client.select_graph("async_stream_example");
    let _ = graph.delete().await;

    graph
        .query("UNWIND range(1, 5) AS i CREATE (:Movie {year: 1990 + i})")
        .execute()
        .await?;

    // ── 1. Collect a whole typed stream in one line ─────────────────────────
    // `data` is a `Stream`; map each row to a typed value and `try_collect`.
    let years: Vec<i64> = graph
        .query("MATCH (m:Movie) RETURN m.year AS year ORDER BY year")
        .execute()
        .await?
        .data
        .map(|row| row?.try_get::<i64>("year"))
        .try_collect()
        .await?;
    println!("years: {years:?}");

    // ── 2. Fan out a follow-up query per row, with bounded concurrency ──────
    // The graph handle is `Send + Clone` (it shares one schema cache), so each unit of work just
    // clones it. `buffer_unordered` keeps at most 8 follow-up queries in flight at once.
    let mut next_years: Vec<i64> = graph
        .query("MATCH (m:Movie) RETURN m.year AS year")
        .execute()
        .await?
        .data
        .map(|row| {
            let mut g = graph.clone();
            async move {
                let year: i64 = row?.try_get("year")?;
                let mut r = g
                    .query(format!("RETURN {year} + 1 AS next"))
                    .execute()
                    .await?;
                r.data
                    .try_next()
                    .await?
                    .expect("a row")
                    .try_get::<i64>("next")
            }
        })
        .buffer_unordered(8)
        .try_collect()
        .await?;
    next_years.sort_unstable();
    println!("next years: {next_years:?}");

    graph.delete().await?;
    Ok(())
}
