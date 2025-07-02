/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    FalkorDBError, FalkorResult, FalkorValue, GraphSchema, SchemaType,
    parser::{redis_value_as_int, redis_value_as_vec},
};
use std::collections::HashMap;

/// Whether this element is a node or edge in the graph
#[derive(Copy, Clone, Debug, Eq, PartialEq, strum::EnumString, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum EntityType {
    /// A node in the graph
    Node,
    /// An edge in the graph, meaning a relationship between two nodes
    #[strum(serialize = "RELATIONSHIP")]
    Edge,
}

/// A node in the graph, containing a unique id, various labels describing it, and its own property.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Node {
    /// The internal entity ID
    pub entity_id: i64,
    /// A [`Vec`] of the labels this node answers to
    pub labels: Vec<String>,
    /// A [`HashMap`] of the properties in key-val form
    pub properties: HashMap<String, FalkorValue>,
}

impl Node {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Node", skip_all, level = "debug")
    )]
    pub(crate) fn parse(
        value: redis::Value,
        graph_schema: &mut GraphSchema,
    ) -> FalkorResult<Self> {
        let [entity_id, labels, properties]: [redis::Value; 3] = redis_value_as_vec(value)
            .and_then(|val_vec| {
                TryInto::try_into(val_vec).map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "Expected exactly 3 elements in node object",
                    )
                })
            })?;

        Ok(Node {
            entity_id: redis_value_as_int(entity_id)?,
            labels: graph_schema.parse_id_vec(redis_value_as_vec(labels)?, SchemaType::Labels)?,
            properties: graph_schema.parse_properties_map(properties)?,
        })
    }
}

/// An edge in the graph, representing a relationship between two [`Node`]s.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Edge {
    /// The internal entity ID
    pub entity_id: i64,
    /// What type is this relationship
    pub relationship_type: String,
    /// The entity ID of the origin node
    pub src_node_id: i64,
    /// The entity ID of the destination node
    pub dst_node_id: i64,
    /// A [`HashMap`] of the properties in key-val form
    pub properties: HashMap<String, FalkorValue>,
}

impl Edge {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Edge", skip_all, level = "debug")
    )]
    pub(crate) fn parse(
        value: redis::Value,
        graph_schema: &mut GraphSchema,
    ) -> FalkorResult<Self> {
        let [
            entity_id,
            relationship_id_raw,
            src_node_id,
            dst_node_id,
            properties,
        ]: [redis::Value; 5] = redis_value_as_vec(value).and_then(|val_vec| {
            val_vec.try_into().map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 5 elements in edge object",
                )
            })
        })?;

        Ok(Edge {
            entity_id: redis_value_as_int(entity_id)?,
            relationship_type: redis_value_as_int(relationship_id_raw)
                .and_then(|id| graph_schema.parse_single_id(id, SchemaType::Relationships))?,
            src_node_id: redis_value_as_int(src_node_id)?,
            dst_node_id: redis_value_as_int(dst_node_id)?,
            properties: graph_schema.parse_properties_map(properties)?,
        })
    }
}
