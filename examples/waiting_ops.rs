/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Demonstrates waiting for background operations to take effect.
//!
//! Some FalkorDB operations finish *after* the command that starts them returns:
//! creating or dropping an index/constraint populates it on a background worker,
//! and `GRAPH.COPY` can fail transiently while the server cannot `fork`. The eager
//! methods (`create_index`, `create_unique_constraint`, `copy_graph`, …) stay
//! fire-and-forget; each also has an additive `*_op` builder that keeps the
//! non-blocking `.execute()` terminal and adds explicit, opt-in `.wait()` /
//! `.wait_with(WaitOptions)` terminals.
//!
//! Set `FALKORDB_CONNECTION` to override the default `falkor://127.0.0.1:6379`.

use std::time::Duration;

use falkordb::{
    EntityType, FalkorClientBuilder, FalkorDBError, FalkorResult, IndexType, WaitOptions,
};

fn main() -> FalkorResult<()> {
    let connection_info = std::env::var("FALKORDB_CONNECTION")
        .unwrap_or_else(|_| "falkor://127.0.0.1:6379".to_string());

    let client = FalkorClientBuilder::new()
        .with_connection_info(connection_info.as_str().try_into()?)
        .build()?;

    let mut graph = client.select_graph("waiting_ops_example");

    // Fire-and-forget, exactly like `create_index` (returns as soon as the server accepts it).
    graph
        .create_index_op(IndexType::Range, EntityType::Node, "Person", &["age"], None)
        .execute()?;

    // Block until the index is actually operational (default 30s readiness timeout).
    graph
        .create_index_op(
            IndexType::Range,
            EntityType::Node,
            "Person",
            &["name"],
            None,
        )
        .wait()?;

    // A unique constraint reports a *distinct* error if existing data violates it.
    match graph
        .create_unique_constraint_op(EntityType::Node, "Person", &["email"])
        .wait_with(WaitOptions::with_timeout(Duration::from_secs(10)))
    {
        Ok(()) => println!("constraint is enforced"),
        Err(FalkorDBError::ConstraintFailed { .. }) => println!("data violates the constraint"),
        Err(other) => return Err(other),
    }

    // Copy a graph, retrying transient `could not fork` failures with backoff.
    let _copy = client
        .copy_graph_op("waiting_ops_example", "waiting_ops_backup")
        .wait()?;

    graph.delete()?;

    Ok(())
}
