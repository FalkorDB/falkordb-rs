/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Creating vector indexes with the typed helpers and `VectorSimilarity`.
//!
//! Run with: `cargo run --example vector_index` (needs a FalkorDB server on 127.0.0.1:6379).

use falkordb::{
    EntityType, FalkorClientBuilder, FalkorResult, IndexStatus, IndexType, VectorSimilarity,
};

fn main() -> FalkorResult<()> {
    let client = FalkorClientBuilder::new()
        .with_connection_info("falkor://127.0.0.1:6379".try_into()?)
        .build()?;

    let mut graph = client.select_graph("vector_index_example");
    let _ = graph.delete();

    // Seed a couple of nodes with 4-dimensional embeddings.
    graph
        .query(
            "CREATE (:Product {name: 'a', embedding: vecf32([0.1, 0.2, 0.3, 0.4])}),
                    (:Product {name: 'b', embedding: vecf32([0.9, 0.8, 0.7, 0.6])})",
        )
        .execute()?;

    // The typed helper builds a correct `OPTIONS { dimension: 4, similarityFunction: 'euclidean' }`
    // clause for you — no hand-built options map, and no risk of the quoted-key Cypher the server
    // rejects. This eager form is fire-and-forget (it returns as soon as the server accepts it).
    graph.create_edge_vector_index("SIMILAR_TO", &["weight"], 4, VectorSimilarity::Cosine)?;

    // Indexes build asynchronously. The typed helpers also have `_op` builders that integrate with
    // the wait ergonomics: `.wait()` blocks until the index is actually operational (and `.execute()`
    // is the non-blocking equivalent), so you don't have to poll `list_indices()` yourself.
    graph
        .create_node_vector_index_op("Product", &["embedding"], 4, VectorSimilarity::Euclidean)
        .wait()?;

    // After `.wait()` returns, the node index is guaranteed to be operational.
    let indices = graph.list_indices()?;
    let has_vector_index = indices.data.iter().any(|index| {
        index.entity_type == EntityType::Node
            && index.status == IndexStatus::Active
            && index
                .field_types
                .get("embedding")
                .is_some_and(|types| types.contains(&IndexType::Vector))
    });
    println!("Product.embedding vector index operational: {has_vector_index}");

    // You then run vector similarity searches in Cypher (e.g. `CALL db.idx.vector.queryNodes(...)`
    // or by ordering on a distance function), using `vecf32([...])` to build query vectors.

    graph.delete()?;
    Ok(())
}
