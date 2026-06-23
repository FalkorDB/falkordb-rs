/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Demonstrates opt-in routing of read-only queries to replica nodes.
//!
//! Routing reads to replicas is **opt-in**, because a FalkorDB replica applies writes only after
//! the primary has committed them — a read served from a replica can be slightly **stale**. By
//! default every read goes to the primary, so you never observe replication lag unless you ask for
//! it. Opt in either:
//!
//! * **per client** — `FalkorClientBuilder::with_read_preference(ReadPreference::PreferReplica)`
//!   makes every read-only query prefer a replica, or
//! * **per query** — `ro_query(..).prefer_replica()` opts a single query in (and
//!   `.primary_only()` forces a single query back onto the primary for read-your-writes paths).
//!
//! `PreferReplica` transparently falls back to the primary when no replica is available (for
//! example a single-node deployment), so the same code runs everywhere.
//!
//! Set `FALKORDB_CONNECTION` to a Sentinel endpoint (for example
//! `falkor://127.0.0.1:26379`) to see reads routed to replicas; otherwise it
//! defaults to a local single node.

use falkordb::{FalkorClientBuilder, FalkorResult, ReadPreference};

fn main() -> FalkorResult<()> {
    let connection_info = std::env::var("FALKORDB_CONNECTION")
        .unwrap_or_else(|_| "falkor://127.0.0.1:6379".to_string());

    // Opt this client into replica reads by default. Drop `with_read_preference` (or pass
    // `ReadPreference::Primary`) to keep every read on the primary.
    let client = FalkorClientBuilder::new()
        .with_connection_info(connection_info.as_str().try_into()?)
        .with_read_preference(ReadPreference::PreferReplica)
        .build()?;

    // `replica_reads_available()` reports capability (a replica pool exists), while
    // `read_preference()` reports the client's default policy.
    if client.replica_reads_available() {
        println!("Replica connections are available; read-only queries may be served from them.");
    } else {
        println!("No readable replicas detected; read-only queries use the primary.");
    }
    println!("Default read preference: {:?}", client.read_preference());

    let mut graph = client.select_graph("readonly_example");

    // Writes always go to the primary.
    graph
        .query("CREATE (:Greeting {text: 'hello'})")
        .execute()?;

    // This read-only query follows the client default (PreferReplica) — served from a replica when
    // one is available, otherwise transparently from the primary.
    let mut greetings = graph
        .ro_query("MATCH (g:Greeting) RETURN g.text")
        .execute()?;
    for row in greetings.data.by_ref() {
        println!("replica-preferring read: {row:?}");
    }

    // A read-your-writes path can force the primary for a single query, ignoring the client default.
    let mut fresh = graph
        .ro_query("MATCH (g:Greeting) RETURN g.text")
        .primary_only()
        .execute()?;
    for row in fresh.data.by_ref() {
        println!("primary (fresh) read: {row:?}");
    }

    graph.delete()?;

    Ok(())
}
