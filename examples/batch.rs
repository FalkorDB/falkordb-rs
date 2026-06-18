/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Batch / pipelined execution: send many queries in one round-trip.
//!
//! Run with: `cargo run --example batch`
//!
//! Requires a running FalkorDB instance (defaults to `127.0.0.1:6379`).

use falkordb::{FalkorClientBuilder, FalkorConnectionInfo, FalkorResult};

fn main() -> FalkorResult<()> {
    let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()?;
    let client = FalkorClientBuilder::new()
        .with_connection_info(connection_info)
        .build()?;
    let mut graph = client.select_graph("batch_example");
    let _ = graph.delete();

    // ── Queue many writes + a read, dispatched in ONE round-trip ────────────
    let mut batch = graph.batch();
    for year in 1990..=1995 {
        batch
            .query("CREATE (:Movie {title: $t, year: $y})")
            .with_param("t", format!("Movie {year}"))
            .with_param("y", year);
    }
    // A read in the same batch sees the writes above (the schema is refreshed if needed).
    batch.ro_query("MATCH (m:Movie) RETURN count(m) AS n");

    let results = batch.execute()?;
    println!("{} queries ran in one round-trip", results.len());

    // The last item is the count read.
    let count: i64 = results[results.len() - 1]
        .as_ref()
        .expect("count query succeeded")
        .data[0]
        .try_get("n")?;
    println!("movies created: {count}");

    // ── Per-item error attribution: one bad query doesn't sink the others ───
    let mut batch = graph.batch();
    batch.query("CREATE (:Movie {title: 'Heat', year: 1995})");
    batch.query("THIS IS NOT VALID CYPHER"); // this one fails …
    batch.ro_query("MATCH (m:Movie) RETURN count(m) AS n"); // … this one still runs

    for (i, item) in batch.execute()?.into_iter().enumerate() {
        match item {
            Ok(_) => println!("query {i}: ok"),
            Err(e) => println!("query {i}: failed ({e})"),
        }
    }

    graph.delete()?;
    Ok(())
}
