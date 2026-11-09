//! Runs queries through `falkordb` built as a plain dependency (see `Cargo.toml`), against the
//! server at `FALKORDB_HOST`:`FALKORDB_PORT`. With the `tracing` feature every query computes a
//! fingerprint, which compiles the literal-redaction regex, so a regex feature `falkordb` doesn't
//! request makes the first query panic here even when the crate's own tests pass.

use falkordb::{FalkorClientBuilder, FalkorConnectionInfo, FalkorValue};

fn main() {
    let host = std::env::var("FALKORDB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("FALKORDB_PORT").unwrap_or_else(|_| "6379".to_string());
    let connection_info: FalkorConnectionInfo = format!("falkor://{host}:{port}")
        .as_str()
        .try_into()
        .expect("invalid connection info");
    let client = FalkorClientBuilder::new()
        .with_connection_info(connection_info)
        .build()
        .expect("failed to build the client");
    let mut graph = client.select_graph("consumer_smoke");

    // Every kind of literal the redaction handles, so each branch of its regex is compiled and used.
    let mut result = graph
        .query("RETURN 1 AS n, 2.5e3 AS f, 'text' AS s, TRUE AS t, false AS b, NULL AS z")
        .execute()
        .expect("the query failed");
    let row = result
        .data
        .next()
        .expect("the query returned no row")
        .expect("the row failed to decode");
    assert_eq!(row.get("n"), Some(&FalkorValue::I64(1)));
    assert_eq!(row.get("t"), Some(&FalkorValue::Bool(true)));
    println!("consumer-smoke: ok");
}
