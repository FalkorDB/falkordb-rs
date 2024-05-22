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
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct Node {
    pub entity_id: i64,
    pub labels: Vec<String>,
    pub properties: HashMap<String, FalkorValue>,
}

pub(crate) fn parse_labels(
    raw_ids: Vec<FalkorValue>,
    graph_schema: &GraphSchema,
    conn: &mut BorrowedSyncConnection,
    schema_type: SchemaType,
) -> Result<Vec<String>> {
    let mut ids_hashset = HashSet::with_capacity(raw_ids.len());
    for label in raw_ids.iter() {
        ids_hashset.insert(label.to_i64().ok_or(FalkorDBError::ParsingI64)?);
    }

    match match graph_schema.verify_id_set(&ids_hashset, schema_type) {
        None => graph_schema.refresh(schema_type, conn, Some(&ids_hashset))?,
        relevant_ids => relevant_ids,
    } {
        Some(relevant_ids) => {
            let mut parsed_ids = Vec::with_capacity(raw_ids.len());
            for id in raw_ids {
                parsed_ids.push(
                    id.to_i64()
                        .ok_or(FalkorDBError::ParsingI64)
                        .and_then(|id| {
                            relevant_ids
                                .get(&id)
                                .cloned()
                                .ok_or(FalkorDBError::ParsingCompactIdUnknown)
                        })?,
                );
            }

            Ok(parsed_ids)
        }
        _ => Err(FalkorDBError::ParsingError)?,
    }
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

#[derive(Clone, Debug)]
pub struct Edge {
    pub entity_id: i64,
    pub relationship_type: String,
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
