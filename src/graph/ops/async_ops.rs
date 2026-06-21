/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Async builder terminals for the background-operation helpers.

use super::{
    classify_copy_result, owned_properties, poll_async, property_refs, ConstraintOp, IndexOp, Step,
    Wait, WaitOperation, WaitOptions,
};
use crate::graph::vector_index_options;
use crate::{
    AsyncGraph, ConstraintType, EntityType, FalkorAsyncClient, FalkorResult, IndexType,
    QueryResult, RowStream, VectorSimilarity,
};
use std::collections::HashMap;
use std::fmt::Display;

/// Builder for creating or dropping an index on an [`AsyncGraph`].
///
/// Created via [`AsyncGraph::create_index_op`] / [`AsyncGraph::drop_index_op`]. Use
/// [`execute`](Self::execute) to issue the command without waiting, or [`wait`](Self::wait) /
/// [`wait_with`](Self::wait_with) to await until the index is operational (create) or gone (drop).
///
/// Dropping the `wait` future cancels only client-side polling; the server keeps running the
/// background operation.
#[must_use = "an index op builder does nothing unless `.execute()` or `.wait()` is awaited"]
pub struct AsyncIndexOpBuilder<'a> {
    graph: &'a mut AsyncGraph,
    op: IndexOp,
}

impl<'a> AsyncIndexOpBuilder<'a> {
    pub(crate) fn new(
        graph: &'a mut AsyncGraph,
        op: IndexOp,
    ) -> Self {
        Self { graph, op }
    }

    /// Issues the index command without waiting (identical to the eager `create_index`/`drop_index`).
    pub async fn execute(self) -> FalkorResult<QueryResult<RowStream>> {
        run_index_command(self.graph, &self.op).await
    }

    /// Issues the command, then awaits (with default [`WaitOptions`]) until it has taken effect.
    pub async fn wait(self) -> FalkorResult<()> {
        self.wait_with(WaitOptions::default()).await
    }

    /// Issues the command, then awaits until it has taken effect or `options.timeout` elapses.
    pub async fn wait_with(
        self,
        options: WaitOptions,
    ) -> FalkorResult<()> {
        let wait = self.op.to_wait();
        run_index_command(self.graph, &self.op).await?;
        wait_async(self.graph, &options, wait).await
    }
}

/// Builder for creating or dropping a constraint on an [`AsyncGraph`].
///
/// Created via [`AsyncGraph::create_unique_constraint_op`],
/// [`AsyncGraph::create_mandatory_constraint_op`] or [`AsyncGraph::drop_constraint_op`]. A `wait`
/// on a create terminal returns [`crate::FalkorDBError::ConstraintFailed`] if the constraint
/// reaches the terminal `FAILED` state.
///
/// For a unique constraint, `wait` polls `DB.CONSTRAINTS` until the constraint itself becomes
/// operational. The server activates a unique constraint only after its backing range index is
/// enabled, so polling the constraint already accounts for the backing index becoming ready — no
/// separate index-readiness step is required.
#[must_use = "a constraint op builder does nothing unless `.execute()` or `.wait()` is awaited"]
pub struct AsyncConstraintOpBuilder<'a> {
    graph: &'a mut AsyncGraph,
    op: ConstraintOp,
}

impl<'a> AsyncConstraintOpBuilder<'a> {
    pub(crate) fn new(
        graph: &'a mut AsyncGraph,
        op: ConstraintOp,
    ) -> Self {
        Self { graph, op }
    }

    /// Issues the constraint command without waiting.
    pub async fn execute(self) -> FalkorResult<redis::Value> {
        run_constraint_command(self.graph, &self.op).await
    }

    /// Issues the command, then awaits (with default [`WaitOptions`]) until it has taken effect.
    pub async fn wait(self) -> FalkorResult<()> {
        self.wait_with(WaitOptions::default()).await
    }

    /// Issues the command, then awaits until it has taken effect or `options.timeout` elapses.
    pub async fn wait_with(
        self,
        options: WaitOptions,
    ) -> FalkorResult<()> {
        let wait = self.op.to_wait();
        run_constraint_command(self.graph, &self.op).await?;
        wait_async(self.graph, &options, wait).await
    }
}

/// Builder for copying a graph via a [`FalkorAsyncClient`].
///
/// Created via [`FalkorAsyncClient::copy_graph_op`]. [`execute`](Self::execute) performs a single
/// copy (identical to `copy_graph`); [`wait`](Self::wait) retries while the server reports a
/// transient `could not fork` failure.
#[must_use = "a copy graph builder does nothing unless `.execute()` or `.wait()` is awaited"]
pub struct AsyncCopyGraphBuilder<'a> {
    client: &'a FalkorAsyncClient,
    source: String,
    destination: String,
}

impl<'a> AsyncCopyGraphBuilder<'a> {
    pub(crate) fn new(
        client: &'a FalkorAsyncClient,
        source: String,
        destination: String,
    ) -> Self {
        Self {
            client,
            source,
            destination,
        }
    }

    /// Performs a single copy attempt (identical to [`FalkorAsyncClient::copy_graph`]).
    pub async fn execute(self) -> FalkorResult<AsyncGraph> {
        self.client
            .copy_graph(&self.source, &self.destination)
            .await
    }

    /// Copies the graph, retrying transient `could not fork` failures with the default copy
    /// [`WaitOptions`] (60s). Non-transient errors are returned immediately and the destination
    /// is never deleted.
    pub async fn wait(self) -> FalkorResult<AsyncGraph> {
        self.wait_with(WaitOptions::for_copy()).await
    }

    /// Copies the graph, retrying transient failures until success or `options.timeout`.
    pub async fn wait_with(
        self,
        options: WaitOptions,
    ) -> FalkorResult<AsyncGraph> {
        let mut this = self;
        poll_async(&mut this, &options, WaitOperation::GraphCopy, |this| {
            Box::pin(async move {
                Ok(classify_copy_result(
                    this.client
                        .copy_graph(&this.source, &this.destination)
                        .await,
                ))
            })
        })
        .await
    }
}

async fn run_index_command(
    graph: &mut AsyncGraph,
    op: &IndexOp,
) -> FalkorResult<QueryResult<RowStream>> {
    match op {
        IndexOp::Create {
            index_type,
            entity_type,
            label,
            properties,
            options,
        } => {
            graph
                .create_index(
                    *index_type,
                    *entity_type,
                    label,
                    properties,
                    options.as_ref(),
                )
                .await
        }
        IndexOp::Drop {
            index_type,
            entity_type,
            label,
            properties,
        } => {
            graph
                .drop_index(*index_type, *entity_type, label, properties)
                .await
        }
    }
}

async fn run_constraint_command(
    graph: &mut AsyncGraph,
    op: &ConstraintOp,
) -> FalkorResult<redis::Value> {
    match op {
        ConstraintOp::CreateUnique {
            entity_type,
            label,
            properties,
        } => {
            graph
                .create_unique_constraint(*entity_type, label.clone(), &property_refs(properties))
                .await
        }
        ConstraintOp::CreateMandatory {
            entity_type,
            label,
            properties,
        } => {
            graph
                .create_mandatory_constraint(*entity_type, label, &property_refs(properties))
                .await
        }
        ConstraintOp::Drop {
            constraint_type,
            entity_type,
            label,
            properties,
        } => {
            graph
                .drop_constraint(
                    *constraint_type,
                    *entity_type,
                    label,
                    &property_refs(properties),
                )
                .await
        }
    }
}

async fn evaluate_async(
    graph: &mut AsyncGraph,
    wait: &Wait,
) -> FalkorResult<Step<()>> {
    match wait {
        Wait::Index(index_wait) => Ok(index_wait.step(&graph.list_indices().await?.data)),
        Wait::Constraint(constraint_wait) => {
            Ok(constraint_wait.step(&graph.list_constraints().await?.data))
        }
    }
}

async fn wait_async(
    graph: &mut AsyncGraph,
    options: &WaitOptions,
    wait: Wait,
) -> FalkorResult<()> {
    let operation = wait.operation();
    let mut state = (graph, wait);
    poll_async(&mut state, options, operation, |state| {
        let (graph, wait) = state;
        Box::pin(evaluate_async(graph, wait))
    })
    .await
}

impl AsyncGraph {
    /// Returns a builder for creating an index, supporting `.execute()` (non-blocking) and
    /// `.wait()` (await until operational). Arguments mirror [`AsyncGraph::create_index`].
    pub fn create_index_op<P: Display>(
        &mut self,
        index_field_type: IndexType,
        entity_type: EntityType,
        label: &str,
        properties: &[P],
        options: Option<&HashMap<String, String>>,
    ) -> AsyncIndexOpBuilder<'_> {
        AsyncIndexOpBuilder::new(
            self,
            IndexOp::Create {
                index_type: index_field_type,
                entity_type,
                label: label.to_string(),
                properties: owned_properties(properties),
                options: options.cloned(),
            },
        )
    }

    /// Returns a builder for creating a **vector** index on a node label, supporting `.execute()`
    /// (non-blocking) and `.wait()` (await until the index is operational). The typed counterpart of
    /// [`create_index_op`](Self::create_index_op) for vectors; mirrors
    /// [`AsyncGraph::create_node_vector_index`].
    pub fn create_node_vector_index_op<P: Display>(
        &mut self,
        label: &str,
        properties: &[P],
        dimension: u32,
        similarity_function: VectorSimilarity,
    ) -> AsyncIndexOpBuilder<'_> {
        AsyncIndexOpBuilder::new(
            self,
            IndexOp::Create {
                index_type: IndexType::Vector,
                entity_type: EntityType::Node,
                label: label.to_string(),
                properties: owned_properties(properties),
                options: Some(vector_index_options(dimension, similarity_function)),
            },
        )
    }

    /// Returns a builder for creating a **vector** index on a relationship type, supporting
    /// `.execute()` (non-blocking) and `.wait()` (await until operational). The typed counterpart of
    /// [`create_index_op`](Self::create_index_op) for vectors; mirrors
    /// [`AsyncGraph::create_edge_vector_index`].
    pub fn create_edge_vector_index_op<P: Display>(
        &mut self,
        relation: &str,
        properties: &[P],
        dimension: u32,
        similarity_function: VectorSimilarity,
    ) -> AsyncIndexOpBuilder<'_> {
        AsyncIndexOpBuilder::new(
            self,
            IndexOp::Create {
                index_type: IndexType::Vector,
                entity_type: EntityType::Edge,
                label: relation.to_string(),
                properties: owned_properties(properties),
                options: Some(vector_index_options(dimension, similarity_function)),
            },
        )
    }

    /// Returns a builder for dropping an index, supporting `.execute()` (non-blocking) and
    /// `.wait()` (await until gone). Arguments mirror [`AsyncGraph::drop_index`].
    pub fn drop_index_op<P: Display>(
        &mut self,
        index_field_type: IndexType,
        entity_type: EntityType,
        label: &str,
        properties: &[P],
    ) -> AsyncIndexOpBuilder<'_> {
        AsyncIndexOpBuilder::new(
            self,
            IndexOp::Drop {
                index_type: index_field_type,
                entity_type,
                label: label.to_string(),
                properties: owned_properties(properties),
            },
        )
    }

    /// Returns a builder for creating a unique constraint, supporting `.execute()` (non-blocking)
    /// and `.wait()` (await until operational, or `FAILED`). Mirrors
    /// [`AsyncGraph::create_unique_constraint`].
    pub fn create_unique_constraint_op(
        &mut self,
        entity_type: EntityType,
        label: &str,
        properties: &[&str],
    ) -> AsyncConstraintOpBuilder<'_> {
        AsyncConstraintOpBuilder::new(
            self,
            ConstraintOp::CreateUnique {
                entity_type,
                label: label.to_string(),
                properties: owned_properties(properties),
            },
        )
    }

    /// Returns a builder for creating a mandatory constraint, supporting `.execute()` and
    /// `.wait()`. Mirrors [`AsyncGraph::create_mandatory_constraint`].
    pub fn create_mandatory_constraint_op(
        &mut self,
        entity_type: EntityType,
        label: &str,
        properties: &[&str],
    ) -> AsyncConstraintOpBuilder<'_> {
        AsyncConstraintOpBuilder::new(
            self,
            ConstraintOp::CreateMandatory {
                entity_type,
                label: label.to_string(),
                properties: owned_properties(properties),
            },
        )
    }

    /// Returns a builder for dropping a constraint, supporting `.execute()` and `.wait()` (await
    /// until the constraint is gone). Mirrors [`AsyncGraph::drop_constraint`].
    pub fn drop_constraint_op(
        &mut self,
        constraint_type: ConstraintType,
        entity_type: EntityType,
        label: &str,
        properties: &[&str],
    ) -> AsyncConstraintOpBuilder<'_> {
        AsyncConstraintOpBuilder::new(
            self,
            ConstraintOp::Drop {
                constraint_type,
                entity_type,
                label: label.to_string(),
                properties: owned_properties(properties),
            },
        )
    }
}

impl FalkorAsyncClient {
    /// Returns a builder for copying a graph, supporting `.execute()` (single attempt) and
    /// `.wait()` (retry transient `could not fork` failures). Mirrors
    /// [`FalkorAsyncClient::copy_graph`].
    pub fn copy_graph_op(
        &self,
        graph_to_clone: &str,
        new_graph_name: &str,
    ) -> AsyncCopyGraphBuilder<'_> {
        AsyncCopyGraphBuilder::new(self, graph_to_clone.to_string(), new_graph_name.to_string())
    }
}
