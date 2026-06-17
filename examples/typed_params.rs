/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Type-safe, injection-proof query parameters.
//!
//! Run with: `cargo run --example typed_params`
//!
//! Requires a running FalkorDB instance (defaults to `127.0.0.1:6379`).

use falkordb::{FalkorClientBuilder, FalkorConnectionInfo, FalkorValue};
use std::collections::BTreeMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()?;
    let client = FalkorClientBuilder::new()
        .with_connection_info(connection_info)
        .build()?;

    let mut graph = client.select_graph("typed_params_example");

    // Start from a clean slate so the example is idempotent across runs.
    let _ = graph.delete();

    // ── Before: hand-quoting and escaping every value yourself ──────────────
    // let mut params = HashMap::new();
    // params.insert("title".to_string(), "'The Matrix'".to_string()); // manual quotes!
    // params.insert("year".to_string(), 1999.to_string());
    // graph.query("CREATE (:Movie {title: $title, year: $year})").with_params(&params)...

    // ── After: typed values, encoding handled for you ───────────────────────
    graph
        .query("CREATE (:Movie {title: $title, year: $year, rating: $rating})")
        .with_param("title", "The Matrix")
        .with_param("year", 1999)
        .with_param("rating", 8.7)
        .execute()?;

    // A string that would be a Cypher-injection attempt is stored as an inert literal.
    graph
        .query("CREATE (:Movie {title: $title, year: $year})")
        .with_param("title", "'; MATCH (n) DETACH DELETE n //")
        .with_param("year", 2003)
        .execute()?;

    // Collections work too: `WHERE x IN $list`.
    let mut titles = graph
        .query("MATCH (m:Movie) WHERE m.year IN $years RETURN m.title ORDER BY m.year")
        .with_param("years", [1999, 2003])
        .execute()?;
    for row in titles.data.by_ref() {
        if let Some(FalkorValue::String(title)) = row.into_iter().next() {
            println!("matched: {title}");
        }
    }

    // Points can't be bound directly — pass the components as a map and wrap with `point()`.
    let coords = BTreeMap::from([("latitude", 32.07), ("longitude", 34.79)]);
    let mut located = graph
        .query("RETURN point($p)")
        .with_param("p", coords)
        .execute()?;
    if let Some(row) = located.data.next() {
        println!("point: {:?}", row.into_iter().next());
    }

    graph.delete()?;
    Ok(())
}
