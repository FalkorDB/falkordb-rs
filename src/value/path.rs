/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::error::FalkorDBError;
use crate::value::graph_entities::{Edge, Node};
use crate::value::FalkorValue;

#[derive(Clone, Debug)]
pub struct Path {
    nodes: Vec<Node>,
    relationships: Vec<Edge>,
}

impl Path {
    pub fn parse(value: FalkorValue) -> anyhow::Result<Path> {
        let [nodes, relationships]: [FalkorValue; 2] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingError)?;
        let (nodes, relationships) = (nodes.into_vec()?, relationships.into_vec()?);

        let mut parsed_nodes = Vec::with_capacity(nodes.len());
        for node_raw in nodes {
            parsed_nodes.push(node_raw.into_node()?);
        }

        let mut parsed_edges = Vec::with_capacity(relationships.len());
        for edge_raw in relationships {
            parsed_edges.push(edge_raw.into_edge()?);
        }

        Ok(Path {
            nodes: parsed_nodes,
            relationships: parsed_edges,
        })
    }
}
