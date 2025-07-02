/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{Edge, FalkorDBError, FalkorResult, GraphSchema, Node, parser::redis_value_as_vec};

/// Represents a path between two nodes, contains all the nodes, and the relationships between them along the path
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Path {
    /// The nodes along the path, ordered
    pub nodes: Vec<Node>,
    /// The relationships between the nodes in the path, ordered
    pub relationships: Vec<Edge>,
}

impl Path {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Path", skip_all, level = "debug")
    )]
    pub(crate) fn parse(
        value: redis::Value,
        graph_schema: &mut GraphSchema,
    ) -> FalkorResult<Self> {
        let [nodes, relationships]: [redis::Value; 2] =
            redis_value_as_vec(value).and_then(|vec_val| {
                vec_val.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "Expected exactly 2 elements for path",
                    )
                })
            })?;

        Ok(Self {
            nodes: redis_value_as_vec(nodes)?
                .into_iter()
                .flat_map(|node| Node::parse(node, graph_schema))
                .collect(),
            relationships: redis_value_as_vec(relationships)?
                .into_iter()
                .flat_map(|edge| Edge::parse(edge, graph_schema))
                .collect(),
        })
    }
}
