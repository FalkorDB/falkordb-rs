/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Typed result mapping with `serde`.
//!
//! Run with: `cargo run --example typed_mapping --features serde`
//!
//! Requires a running FalkorDB instance (defaults to `127.0.0.1:6379`).

use falkordb::{FalkorClientBuilder, FalkorConnectionInfo, FalkorValue};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Movie {
    title: String,
    year: i64,
    #[serde(default)]
    rating: Option<f64>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()?;
    let client = FalkorClientBuilder::new()
        .with_connection_info(connection_info)
        .build()?;

    let mut graph = client.select_graph("typed_mapping_example");

    // Start from a clean slate so the example is idempotent across repeated runs.
    // The graph may not exist yet, so ignore a "missing graph" error here.
    let _ = graph.delete();

    graph
        .query("CREATE (:Movie {title: 'Heat', year: 1995, rating: 8.3})")
        .execute()?;

    // Without typed mapping you would hand-match every `FalkorValue` variant.
    // With the `serde` feature, deserialize a returned node straight into a struct:
    let mut result = graph.query("MATCH (m:Movie) RETURN m").execute()?;
    for row in result.data.by_ref() {
        if let Some(node) = row.into_iter().next() {
            let movie: Movie = node.deserialize_into()?;
            println!("{} ({}) rating={:?}", movie.title, movie.year, movie.rating);
        }
    }

    // Scalars and collections work too:
    let mut titles = graph.query("MATCH (m:Movie) RETURN m.title").execute()?;
    for row in titles.data.by_ref() {
        if let Some(FalkorValue::String(_)) = row.first() {
            let title: String = row.into_iter().next().unwrap().deserialize_into()?;
            println!("title = {title}");
        }
    }

    graph.delete()?;
    Ok(())
}
