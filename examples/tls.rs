/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Demonstrates connecting to FalkorDB over TLS.
//!
//! TLS is provided by the underlying `redis` crate and is opt-in behind a cargo
//! feature: `rustls` (used here, pure Rust) or `native-tls` — and the `tokio-*`
//! variants for the async client. Use a `falkors://` connection URL (it maps to
//! `rediss://`); everything else is identical to a plain connection.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example tls --features rustls
//! ```
//!
//! Set `FALKORDB_TLS_CONNECTION` to override the default `falkors://127.0.0.1:6379`.

use falkordb::{FalkorClientBuilder, FalkorResult};

fn main() -> FalkorResult<()> {
    let connection_info = std::env::var("FALKORDB_TLS_CONNECTION")
        .unwrap_or_else(|_| "falkors://127.0.0.1:6379".to_string());

    // A `falkors://` URL negotiates TLS using the enabled backend (`rustls` here).
    let client = FalkorClientBuilder::new()
        .with_connection_info(connection_info.as_str().try_into()?)
        .build()?;

    let mut graph = client.select_graph("tls_example");

    let mut greetings = graph
        .query("CREATE (g:Greeting {text: 'hello over TLS'}) RETURN g.text")
        .execute()?;

    for row in greetings.data.by_ref() {
        println!("{row:?}");
    }

    graph.delete()?;

    Ok(())
}
