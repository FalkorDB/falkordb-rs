/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use super::utils::parse_labels;
use crate::{
    connection::blocking::BorrowedSyncConnection, value::map::parse_map_with_schema, FalkorDBError,
    FalkorParsable, FalkorValue, SchemaType, SyncGraphSchema,
};
use anyhow::Result;
use std::collections::{HashMap, HashSet};

#[cfg(feature = "tokio")]
use crate::value::{map::parse_map_with_schema_async, utils_async::parse_labels_async};

#[derive(Clone, Debug)]
pub struct Node {
    pub entity_id: i64,
    pub labels: Vec<String>,
    pub properties: HashMap<String, FalkorValue>,
}

impl FalkorParsable for Node {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &SyncGraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> Result<Self> {
        let [entity_id, labels, properties]: [FalkorValue; 3] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingNodeElementCount)?;
        let labels = labels.into_vec()?;

        let mut ids_hashset = HashSet::with_capacity(labels.len());
        for label in labels.iter() {
            ids_hashset.insert(
                label
                    .to_i64()
                    .ok_or(FalkorDBError::ParsingCompactIdUnknown)?,
            );
        }

        let parsed_labels = parse_labels(labels, graph_schema, conn, SchemaType::Labels)?;
        Ok(Node {
            entity_id: entity_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
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

#[cfg(feature = "tokio")]
impl crate::FalkorAsyncParseable for Node {
    async fn from_falkor_value_async(
        value: FalkorValue,
        graph_schema: &crate::AsyncGraphSchema,
        conn: &mut crate::FalkorAsyncConnection,
    ) -> Result<Self> {
        let [entity_id, labels, properties]: [FalkorValue; 3] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingNodeElementCount)?;
        let labels = labels.into_vec()?;

        let mut ids_hashset = HashSet::with_capacity(labels.len());
        for label in labels.iter() {
            ids_hashset.insert(
                label
                    .to_i64()
                    .ok_or(FalkorDBError::ParsingCompactIdUnknown)?,
            );
        }

        let parsed_labels =
            parse_labels_async(labels, graph_schema, conn, SchemaType::Labels).await?;
        Ok(Node {
            entity_id: entity_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
            labels: parsed_labels,
            properties: parse_map_with_schema_async(
                properties,
                graph_schema,
                conn,
                SchemaType::Properties,
            )
            .await?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Edge {
    pub entity_id: i64,
    pub relationship_type: String,
    pub src_node_id: i64,
    pub dst_node_id: i64,
    pub properties: HashMap<String, FalkorValue>,
}

impl FalkorParsable for Edge {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &SyncGraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> Result<Self> {
        let [entity_id, relations, src_node_id, dst_node_id, properties]: [FalkorValue; 5] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingError)?;

        let relation = relations.to_i64().ok_or(FalkorDBError::ParsingI64)?;
        if let Some(relationship) = graph_schema.relationships().read().get(&relation).cloned() {
            return Ok(Edge {
                entity_id: entity_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
                relationship_type: relationship,
                src_node_id: src_node_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
                dst_node_id: dst_node_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
                properties: parse_map_with_schema(
                    properties,
                    graph_schema,
                    conn,
                    SchemaType::Properties,
                )?,
            });
        }

        match graph_schema.refresh(
            SchemaType::Relationships,
            conn,
            Some(&HashSet::from([relation])),
        )? {
            None => Err(FalkorDBError::ParsingCompactIdUnknown)?,
            Some(id) => Ok(Edge {
                entity_id: entity_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
                relationship_type: id
                    .get(&relation)
                    .cloned()
                    .ok_or(FalkorDBError::ParsingCompactIdUnknown)?,
                src_node_id: src_node_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
                dst_node_id: dst_node_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
                properties: parse_map_with_schema(
                    properties,
                    graph_schema,
                    conn,
                    SchemaType::Properties,
                )?,
            }),
        }
    }
}

#[cfg(feature = "tokio")]
impl crate::FalkorAsyncParseable for Edge {
    async fn from_falkor_value_async(
        value: FalkorValue,
        graph_schema: &crate::AsyncGraphSchema,
        conn: &mut crate::FalkorAsyncConnection,
    ) -> Result<Self> {
        let [entity_id, relations, src_node_id, dst_node_id, properties]: [FalkorValue; 5] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingError)?;

        let relation = relations.to_i64().ok_or(FalkorDBError::ParsingI64)?;
        if let Some(relationship) = graph_schema
            .relationships()
            .read()
            .await
            .get(&relation)
            .cloned()
        {
            return Ok(Edge {
                entity_id: entity_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
                relationship_type: relationship,
                src_node_id: src_node_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
                dst_node_id: dst_node_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
                properties: parse_map_with_schema_async(
                    properties,
                    graph_schema,
                    conn,
                    SchemaType::Properties,
                )
                .await?,
            });
        }

        match graph_schema
            .refresh(
                SchemaType::Relationships,
                conn,
                Some(&HashSet::from([relation])),
            )
            .await?
        {
            None => Err(FalkorDBError::ParsingCompactIdUnknown)?,
            Some(id) => Ok(Edge {
                entity_id: entity_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
                relationship_type: id
                    .get(&relation)
                    .cloned()
                    .ok_or(FalkorDBError::ParsingCompactIdUnknown)?,
                src_node_id: src_node_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
                dst_node_id: dst_node_id.to_i64().ok_or(FalkorDBError::ParsingI64)?,
                properties: parse_map_with_schema_async(
                    properties,
                    graph_schema,
                    conn,
                    SchemaType::Properties,
                )
                .await?,
            }),
        }
    }
}
