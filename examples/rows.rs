/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Header-aware result rows: reading columns by name or index, with strict typed access.
//!
//! Run with: `cargo run --example rows`
//!
//! Requires a running FalkorDB instance (defaults to `127.0.0.1:6379`).

use falkordb::{FalkorClientBuilder, FalkorConnectionInfo, FalkorValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()?;
    let client = FalkorClientBuilder::new()
        .with_connection_info(connection_info)
        .build()?;

    let mut graph = client.select_graph("rows_example");

    // Start from a clean slate so the example is idempotent across runs.
    let _ = graph.delete();

    graph
        .query(
            "CREATE (:Movie {title: 'The Matrix', year: 1999, rating: 8.7}),
                    (:Movie {title: 'Heat', year: 1995, rating: 8.3})",
        )
        .execute()?;

    // ── Read columns by alias, converted to the type you ask for ────────────
    let mut result = graph
        .query(
            "MATCH (m:Movie)
             RETURN m.title AS title, m.year AS year, m.rating AS rating
             ORDER BY m.year",
        )
        .execute()?;

    // `data` yields `FalkorResult<Row>`, so a row that fails to parse is a real `Err` you can `?`.
    for row in result.data.by_ref() {
        let row = row?;
        let title: String = row.try_get("title")?;
        let year: i64 = row.try_get("year")?;
        let rating: f64 = row.try_get("rating")?;
        println!("{title} ({year}) — rated {rating}");
    }

    // ── Read by index, and borrow the raw value without converting ──────────
    let mut counted = graph.query("MATCH (m:Movie) RETURN count(m)").execute()?;
    if let Some(row) = counted.data.next() {
        let row = row?;
        let count: i64 = row.try_get_at(0)?;
        println!("movies: {count}");
        // `get_at` borrows the underlying `FalkorValue` without converting or cloning.
        assert_eq!(row.get_at(0), Some(&FalkorValue::I64(count)));
    }

    // ── Collect a whole result set; `collect` short-circuits on the first error ──
    let titles: Vec<String> = graph
        .query("MATCH (m:Movie) RETURN m.title AS title ORDER BY m.title")
        .execute()?
        .data
        .map(|row| row?.try_get::<String>("title"))
        .collect::<Result<_, _>>()?;
    println!("titles: {titles:?}");

    // ── Turn a row into a column -> value map ───────────────────────────────
    let mut as_map = graph
        .query("MATCH (m:Movie) RETURN m.title AS title, m.year AS year ORDER BY m.year LIMIT 1")
        .execute()?;
    if let Some(row) = as_map.data.next() {
        let map = row?.into_map();
        println!("oldest movie as a map: {map:?}");
    }

    graph.delete()?;
    Ok(())
}
