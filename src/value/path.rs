/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection, Edge, FalkorDBError, FalkorParsable, FalkorValue,
    Node, SyncGraphSchema,
};
use anyhow::Result;

/// TODO: not exactly sure what this represents
#[derive(Clone, Debug, PartialEq)]
pub struct Path {
    pub nodes: Vec<Node>,
    pub relationships: Vec<Edge>,
}

impl FalkorParsable for Path {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &SyncGraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> Result<Self> {
        let [nodes, relationships]: [FalkorValue; 2] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;
        let (nodes, relationships) = (nodes.into_vec()?, relationships.into_vec()?);

        let mut parsed_nodes = Vec::with_capacity(nodes.len());
        for node_raw in nodes {
            parsed_nodes.push(FalkorParsable::from_falkor_value(
                node_raw,
                graph_schema,
                conn,
            )?);
        }

        let mut parsed_edges = Vec::with_capacity(relationships.len());
        for edge_raw in relationships {
            parsed_edges.push(FalkorParsable::from_falkor_value(
                edge_raw,
                graph_schema,
                conn,
            )?);
        }

        Ok(Path {
            nodes: parsed_nodes,
            relationships: parsed_edges,
        })
    }
}

#[cfg(feature = "tokio")]
impl crate::FalkorAsyncParseable for Path {
    async fn from_falkor_value_async(
        value: FalkorValue,
        graph_schema: &crate::AsyncGraphSchema,
        conn: crate::FalkorAsyncConnection,
    ) -> Result<Self> {
        let [nodes, relationships]: [FalkorValue; 2] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;
        let (nodes, relationships) = (nodes.into_vec()?, relationships.into_vec()?);

        let mut parsed_nodes = Vec::with_capacity(nodes.len());
        for node_raw in nodes {
            parsed_nodes
                .push(Node::from_falkor_value_async(node_raw, graph_schema, conn.clone()).await?);
        }

        let mut parsed_edges = Vec::with_capacity(relationships.len());
        for edge_raw in relationships {
            parsed_edges
                .push(Edge::from_falkor_value_async(edge_raw, graph_schema, conn.clone()).await?);
        }

        Ok(Path {
            nodes: parsed_nodes,
            relationships: parsed_edges,
        })
    }
}
