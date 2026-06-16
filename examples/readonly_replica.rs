/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Demonstrates routing read-only queries to replica nodes.
//!
//! When the client is pointed at a Redis Sentinel deployment that exposes
//! readable replicas, `ro_query` / `call_procedure_ro` are automatically served
//! from a replica, taking read load off the primary. Against a single-node
//! deployment there are no replicas, so read-only queries transparently fall
//! back to the primary and behave exactly as before.
//!
//! Set `FALKORDB_CONNECTION` to a Sentinel endpoint (for example
//! `falkor://127.0.0.1:26379`) to see reads routed to replicas; otherwise it
//! defaults to a local single node.

use falkordb::{FalkorClientBuilder, FalkorResult};

fn main() -> FalkorResult<()> {
    let connection_info = std::env::var("FALKORDB_CONNECTION")
        .unwrap_or_else(|_| "falkor://127.0.0.1:6379".to_string());

    let client = FalkorClientBuilder::new()
        .with_connection_info(connection_info.as_str().try_into()?)
        .build()?;

    // Reports whether read-only queries are routed to replica nodes. This is
    // `true` only for Sentinel deployments that expose readable replicas.
    if client.reads_from_replicas() {
        println!("Read-only queries are routed to replica nodes.");
    } else {
        println!("No readable replicas detected; read-only queries use the primary.");
    }

    let mut graph = client.select_graph("readonly_example");

    // Writes always go to the primary.
    graph
        .query("CREATE (:Greeting {text: 'hello'})")
        .execute()?;

    // Read-only queries are served from a replica when one is available.
    let mut greetings = graph
        .ro_query("MATCH (g:Greeting) RETURN g.text")
        .execute()?;

    for row in greetings.data.by_ref() {
        println!("read-only result: {row:?}");
    }

    graph.delete()?;

    Ok(())
}
