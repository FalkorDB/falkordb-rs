/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{Edge, FalkorDBError, FalkorParsable, FalkorResult, FalkorValue, GraphSchema, Node};

/// Represents a path between two nodes, contains all the nodes, and the relationships between them along the path
#[derive(Clone, Debug, PartialEq)]
pub struct Path {
    /// The nodes along the path, ordered
    pub nodes: Vec<Node>,
    /// The relationships between the nodes in the path, ordered
    pub relationships: Vec<Edge>,
}

impl FalkorParsable for Path {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &mut GraphSchema,
    ) -> FalkorResult<Self> {
        let [nodes, relationships]: [FalkorValue; 2] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        Ok(Self {
            nodes: nodes
                .into_vec()?
                .into_iter()
                .flat_map(|node| Node::from_falkor_value(node, graph_schema))
                .collect(),
            relationships: relationships
                .into_vec()?
                .into_iter()
                .flat_map(|edge| Edge::from_falkor_value(edge, graph_schema))
                .collect(),
        })
    }
}
