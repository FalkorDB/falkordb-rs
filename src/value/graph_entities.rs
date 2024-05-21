/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::connection::blocking::BorrowedSyncConnection;
use crate::error::FalkorDBError;
use crate::graph::schema::{GraphSchema, SchemaType};
use crate::value::map::parse_map_with_schema;
use crate::value::FalkorValue;
use anyhow::Result;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Node {
    pub entity_id: i64,
    pub labels: Vec<i64>,
    pub properties: HashMap<String, FalkorValue>,
}

impl Node {
    pub(crate) fn parse(
        value: FalkorValue,
        graph_schema: &GraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> Result<Self> {
        let [entity_id, labels, properties]: [FalkorValue; 3] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingNodeElementCount)?;
        let labels = labels.into_vec()?;

        let mut parsed_labels = Vec::with_capacity(labels.len());
        for label in labels {
            parsed_labels.push(label.to_i64().ok_or(FalkorDBError::ParsingError)?);
        }

        Ok(Node {
            entity_id: entity_id.to_i64().ok_or(FalkorDBError::ParsingError)?,
            labels: parsed_labels,
            properties: parse_map_with_schema(
                properties,
                graph_schema,
                conn,
                SchemaType::Properties,
            )?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Edge {
    pub entity_id: i64,
    pub labels: i64,
    pub src_node_id: i64,
    pub dst_node_id: i64,
    pub properties: HashMap<String, FalkorValue>,
}

impl Edge {
    pub(crate) fn parse(
        value: FalkorValue,
        graph_schema: &GraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> Result<Self> {
        let [entity_id, labels, src_node_id, dst_node_id, properties]: [FalkorValue; 5] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingError)?;

        Ok(Edge {
            entity_id: entity_id.to_i64().ok_or(FalkorDBError::ParsingError)?,
            labels: labels.to_i64().ok_or(FalkorDBError::ParsingError)?,
            src_node_id: src_node_id.to_i64().ok_or(FalkorDBError::ParsingError)?,
            dst_node_id: dst_node_id.to_i64().ok_or(FalkorDBError::ParsingError)?,
            properties: parse_map_with_schema(
                properties,
                graph_schema,
                conn,
                SchemaType::Properties,
            )?,
        })
    }
}
