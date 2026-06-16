/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Blocking builder terminals for the background-operation helpers.

use super::{
    classify_copy_result, owned_properties, poll_sync, property_refs, ConstraintOp, IndexOp, Wait,
    WaitOperation, WaitOptions,
};
use crate::{
    ConstraintType, EntityType, FalkorResult, FalkorSyncClient, IndexType, LazyResultSet,
    QueryResult, SyncGraph,
};
use std::collections::HashMap;
use std::fmt::Display;

/// Builder for creating or dropping an index on a [`SyncGraph`].
///
/// Created via [`SyncGraph::create_index_op`] / [`SyncGraph::drop_index_op`]. Use
/// [`execute`](Self::execute) to issue the command without waiting, or [`wait`](Self::wait) /
/// [`wait_with`](Self::wait_with) to block until the index is operational (create) or gone (drop).
#[must_use = "an index op builder does nothing unless `.execute()` or `.wait()` is called"]
pub struct IndexOpBuilder<'a> {
    graph: &'a mut SyncGraph,
    op: IndexOp,
}

impl<'a> IndexOpBuilder<'a> {
    pub(crate) fn new(
        graph: &'a mut SyncGraph,
        op: IndexOp,
    ) -> Self {
        Self { graph, op }
    }

    /// Issues the index command without waiting (identical to the eager `create_index`/`drop_index`).
    pub fn execute(self) -> FalkorResult<QueryResult<LazyResultSet<'a>>> {
        run_index_command(self.graph, &self.op)
    }

    /// Issues the command, then blocks (with default [`WaitOptions`]) until it has taken effect.
    pub fn wait(self) -> FalkorResult<()> {
        self.wait_with(WaitOptions::default())
    }

    /// Issues the command, then blocks until it has taken effect or `options.timeout` elapses.
    pub fn wait_with(
        self,
        options: WaitOptions,
    ) -> FalkorResult<()> {
        let wait = self.op.to_wait();
        run_index_command(self.graph, &self.op)?;
        wait_sync(self.graph, &options, &wait)
    }
}

/// Builder for creating or dropping a constraint on a [`SyncGraph`].
///
/// Created via [`SyncGraph::create_unique_constraint_op`],
/// [`SyncGraph::create_mandatory_constraint_op`] or [`SyncGraph::drop_constraint_op`]. A `wait`
/// on a create terminal returns [`crate::FalkorDBError::ConstraintFailed`] if the constraint
/// reaches the terminal `FAILED` state (e.g. existing data violates a unique constraint).
///
/// For a unique constraint, `wait` polls `DB.CONSTRAINTS` until the constraint itself becomes
/// operational. The server activates a unique constraint only after its backing range index is
/// enabled, so polling the constraint already accounts for the backing index becoming ready — no
/// separate index-readiness step is required.
#[must_use = "a constraint op builder does nothing unless `.execute()` or `.wait()` is called"]
pub struct ConstraintOpBuilder<'a> {
    graph: &'a mut SyncGraph,
    op: ConstraintOp,
}

impl<'a> ConstraintOpBuilder<'a> {
    pub(crate) fn new(
        graph: &'a mut SyncGraph,
        op: ConstraintOp,
    ) -> Self {
        Self { graph, op }
    }

    /// Issues the constraint command without waiting.
    pub fn execute(self) -> FalkorResult<redis::Value> {
        run_constraint_command(self.graph, &self.op)
    }

    /// Issues the command, then blocks (with default [`WaitOptions`]) until it has taken effect.
    pub fn wait(self) -> FalkorResult<()> {
        self.wait_with(WaitOptions::default())
    }

    /// Issues the command, then blocks until it has taken effect or `options.timeout` elapses.
    pub fn wait_with(
        self,
        options: WaitOptions,
    ) -> FalkorResult<()> {
        let wait = self.op.to_wait();
        run_constraint_command(self.graph, &self.op)?;
        wait_sync(self.graph, &options, &wait)
    }
}

/// Builder for copying a graph via a [`FalkorSyncClient`].
///
/// Created via [`FalkorSyncClient::copy_graph_op`]. [`execute`](Self::execute) performs a single
/// copy (identical to `copy_graph`); [`wait`](Self::wait) retries while the server reports a
/// transient `could not fork` failure.
#[must_use = "a copy graph builder does nothing unless `.execute()` or `.wait()` is called"]
pub struct CopyGraphBuilder<'a> {
    client: &'a FalkorSyncClient,
    source: String,
    destination: String,
}

impl<'a> CopyGraphBuilder<'a> {
    pub(crate) fn new(
        client: &'a FalkorSyncClient,
        source: String,
        destination: String,
    ) -> Self {
        Self {
            client,
            source,
            destination,
        }
    }

    /// Performs a single copy attempt (identical to [`FalkorSyncClient::copy_graph`]).
    pub fn execute(self) -> FalkorResult<SyncGraph> {
        self.client.copy_graph(&self.source, &self.destination)
    }

    /// Copies the graph, retrying transient `could not fork` failures with the default copy
    /// [`WaitOptions`] (60s). Non-transient errors are returned immediately and the destination
    /// is never deleted.
    pub fn wait(self) -> FalkorResult<SyncGraph> {
        self.wait_with(WaitOptions::for_copy())
    }

    /// Copies the graph, retrying transient failures until success or `options.timeout`.
    pub fn wait_with(
        self,
        options: WaitOptions,
    ) -> FalkorResult<SyncGraph> {
        poll_sync(&options, WaitOperation::GraphCopy, || {
            Ok(classify_copy_result(
                self.client.copy_graph(&self.source, &self.destination),
            ))
        })
    }
}

fn run_index_command<'a>(
    graph: &'a mut SyncGraph,
    op: &IndexOp,
) -> FalkorResult<QueryResult<LazyResultSet<'a>>> {
    match op {
        IndexOp::Create {
            index_type,
            entity_type,
            label,
            properties,
            options,
        } => graph.create_index(
            *index_type,
            *entity_type,
            label,
            properties,
            options.as_ref(),
        ),
        IndexOp::Drop {
            index_type,
            entity_type,
            label,
            properties,
        } => graph.drop_index(*index_type, *entity_type, label, properties),
    }
}

fn run_constraint_command(
    graph: &mut SyncGraph,
    op: &ConstraintOp,
) -> FalkorResult<redis::Value> {
    match op {
        ConstraintOp::CreateUnique {
            entity_type,
            label,
            properties,
        } => {
            graph.create_unique_constraint(*entity_type, label.clone(), &property_refs(properties))
        }
        ConstraintOp::CreateMandatory {
            entity_type,
            label,
            properties,
        } => graph.create_mandatory_constraint(*entity_type, label, &property_refs(properties)),
        ConstraintOp::Drop {
            constraint_type,
            entity_type,
            label,
            properties,
        } => graph.drop_constraint(
            *constraint_type,
            *entity_type,
            label,
            &property_refs(properties),
        ),
    }
}

fn wait_sync(
    graph: &mut SyncGraph,
    options: &WaitOptions,
    wait: &Wait,
) -> FalkorResult<()> {
    poll_sync(options, wait.operation(), || match wait {
        Wait::Index(index_wait) => Ok(index_wait.step(&graph.list_indices()?.data)),
        Wait::Constraint(constraint_wait) => {
            Ok(constraint_wait.step(&graph.list_constraints()?.data))
        }
    })
}

impl SyncGraph {
    /// Returns a builder for creating an index, supporting `.execute()` (non-blocking) and
    /// `.wait()` (block until the index is operational). Arguments mirror [`SyncGraph::create_index`].
    pub fn create_index_op<P: Display>(
        &mut self,
        index_field_type: IndexType,
        entity_type: EntityType,
        label: &str,
        properties: &[P],
        options: Option<&HashMap<String, String>>,
    ) -> IndexOpBuilder<'_> {
        IndexOpBuilder::new(
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

    /// Returns a builder for dropping an index, supporting `.execute()` (non-blocking) and
    /// `.wait()` (block until the index is gone). Arguments mirror [`SyncGraph::drop_index`].
    pub fn drop_index_op<P: Display>(
        &mut self,
        index_field_type: IndexType,
        entity_type: EntityType,
        label: &str,
        properties: &[P],
    ) -> IndexOpBuilder<'_> {
        IndexOpBuilder::new(
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
    /// and `.wait()` (block until operational, or `FAILED`). Mirrors
    /// [`SyncGraph::create_unique_constraint`].
    pub fn create_unique_constraint_op(
        &mut self,
        entity_type: EntityType,
        label: &str,
        properties: &[&str],
    ) -> ConstraintOpBuilder<'_> {
        ConstraintOpBuilder::new(
            self,
            ConstraintOp::CreateUnique {
                entity_type,
                label: label.to_string(),
                properties: owned_properties(properties),
            },
        )
    }

    /// Returns a builder for creating a mandatory constraint, supporting `.execute()` and
    /// `.wait()`. Mirrors [`SyncGraph::create_mandatory_constraint`].
    pub fn create_mandatory_constraint_op(
        &mut self,
        entity_type: EntityType,
        label: &str,
        properties: &[&str],
    ) -> ConstraintOpBuilder<'_> {
        ConstraintOpBuilder::new(
            self,
            ConstraintOp::CreateMandatory {
                entity_type,
                label: label.to_string(),
                properties: owned_properties(properties),
            },
        )
    }

    /// Returns a builder for dropping a constraint, supporting `.execute()` and `.wait()` (block
    /// until the constraint is gone). Mirrors [`SyncGraph::drop_constraint`].
    pub fn drop_constraint_op(
        &mut self,
        constraint_type: ConstraintType,
        entity_type: EntityType,
        label: &str,
        properties: &[&str],
    ) -> ConstraintOpBuilder<'_> {
        ConstraintOpBuilder::new(
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

impl FalkorSyncClient {
    /// Returns a builder for copying a graph, supporting `.execute()` (single attempt) and
    /// `.wait()` (retry transient `could not fork` failures). Mirrors
    /// [`FalkorSyncClient::copy_graph`].
    pub fn copy_graph_op(
        &self,
        graph_to_clone: &str,
        new_graph_name: &str,
    ) -> CopyGraphBuilder<'_> {
        CopyGraphBuilder::new(self, graph_to_clone.to_string(), new_graph_name.to_string())
    }
}
