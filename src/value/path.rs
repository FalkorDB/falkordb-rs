/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{Edge, FalkorDBError, FalkorResult, GraphSchema, Node};

/// Represents a path between two nodes, contains all the nodes, and the relationships between them along the path
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Path {
    /// The nodes along the path, ordered
    pub nodes: Vec<Node>,
    /// The relationships between the nodes in the path, ordered
    pub relationships: Vec<Edge>,
}

impl Path {
    pub(crate) fn parse(
        value: redis::Value,
        graph_schema: &mut GraphSchema,
    ) -> FalkorResult<Self> {
        let [nodes, relationships]: [redis::Value; 2] = value
            .into_sequence()
            .map_err(|_| FalkorDBError::ParsingArray)?
            .try_into()
            .map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 2 elements for path",
                )
            })?;

        Ok(Self {
            nodes: nodes
                .into_sequence()
                .map_err(|_| FalkorDBError::ParsingArray)?
                .into_iter()
                .flat_map(|node| Node::parse(node, graph_schema))
                .collect(),
            relationships: relationships
                .into_sequence()
                .map_err(|_| FalkorDBError::ParsingArray)?
                .into_iter()
                .flat_map(|edge| Edge::parse(edge, graph_schema))
                .collect(),
        })
    }
}
