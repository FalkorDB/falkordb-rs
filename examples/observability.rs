/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Demonstrates the `tracing` span enrichment (run with `--features tracing`).
//!
//! With the `tracing` feature enabled, the query- and procedure-execution spans carry structured,
//! low-cardinality fields you can slice on — the graph (`db.namespace`), the command
//! (`db.operation.name`), whether it is read-only, the connection strategy, and a privacy-safe
//! `db.query.fingerprint`. The raw query text and parameter values are never recorded by default.
//!
//! This installs a simple `tracing-subscriber` that prints each span (and its fields) as it closes,
//! so you can see the enriched execution spans for the write and the read below. Run with:
//! `cargo run --example observability --features tracing`.

use falkordb::{FalkorClientBuilder, FalkorResult};
use tracing_subscriber::fmt::format::FmtSpan;

fn main() -> FalkorResult<()> {
    // Print each span and its recorded fields when it closes. The execution spans are at TRACE
    // level, so enable TRACE to see them (a real app would scope this to the `falkordb` target).
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_span_events(FmtSpan::CLOSE)
        .with_target(true)
        .init();

    let connection_info = std::env::var("FALKORDB_CONNECTION")
        .unwrap_or_else(|_| "falkor://127.0.0.1:6379".to_string());

    // `with_query_logging(true)` would additionally record the raw Cypher as `db.query.text`;
    // it is off by default for privacy, so only the redacted fingerprint is recorded.
    let client = FalkorClientBuilder::new()
        .with_connection_info(connection_info.as_str().try_into()?)
        .build()?;

    let mut graph = client.select_graph("observability_example");

    // A write: its span carries `db.operation.name = "GRAPH.QUERY"`, `db.falkordb.read_only = false`.
    graph
        .query("CREATE (:Movie {title: 'The Matrix', year: 1999})")
        .execute()?;

    // A read: its span carries `db.operation.name = "GRAPH.RO_QUERY"`, `read_only = true`, and a
    // `db.query.fingerprint` that is independent of the inlined `'The Matrix'` literal.
    let mut titles = graph
        .ro_query("MATCH (m:Movie) WHERE m.title = 'The Matrix' RETURN m.year")
        .execute()?;

    for row in titles.data.by_ref() {
        println!("read-only result: {row:?}");
    }

    graph.delete()?;

    Ok(())
}
