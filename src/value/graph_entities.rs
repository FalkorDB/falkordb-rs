/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    parser::{redis_value_as_int, redis_value_as_vec},
    FalkorDBError, FalkorResult, FalkorValue, GraphSchema, SchemaType,
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
        let [entity_id, relationship_id_raw, src_node_id, dst_node_id, properties]: [redis::Value;
            5] = redis_value_as_vec(value).and_then(|val_vec| {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_type_node() {
        assert_eq!(EntityType::Node.to_string(), "NODE");
    }

    #[test]
    fn test_entity_type_edge() {
        assert_eq!(EntityType::Edge.to_string(), "RELATIONSHIP");
    }

    #[test]
    fn test_entity_type_from_string() {
        use std::str::FromStr;
        assert_eq!(EntityType::from_str("NODE").unwrap(), EntityType::Node);
        assert_eq!(
            EntityType::from_str("RELATIONSHIP").unwrap(),
            EntityType::Edge
        );
    }

    #[test]
    fn test_entity_type_clone() {
        let node = EntityType::Node;
        let node_clone = node;
        assert_eq!(node, node_clone);
    }

    #[test]
    fn test_entity_type_debug() {
        assert!(format!("{:?}", EntityType::Node).contains("Node"));
        assert!(format!("{:?}", EntityType::Edge).contains("Edge"));
    }

    #[test]
    fn test_node_default() {
        let node = Node::default();
        assert_eq!(node.entity_id, 0);
        assert!(node.labels.is_empty());
        assert!(node.properties.is_empty());
    }

    #[test]
    fn test_node_clone() {
        let mut properties = HashMap::new();
        properties.insert("name".to_string(), FalkorValue::String("Alice".to_string()));

        let node = Node {
            entity_id: 1,
            labels: vec!["Person".to_string()],
            properties: properties.clone(),
        };

        let node_clone = node.clone();
        assert_eq!(node, node_clone);
        assert_eq!(node.entity_id, node_clone.entity_id);
        assert_eq!(node.labels, node_clone.labels);
        assert_eq!(node.properties, node_clone.properties);
    }

    #[test]
    fn test_node_debug() {
        let node = Node {
            entity_id: 42,
            labels: vec!["Test".to_string()],
            properties: HashMap::new(),
        };
        let debug_str = format!("{:?}", node);
        assert!(debug_str.contains("42"));
        assert!(debug_str.contains("Test"));
    }

    #[test]
    fn test_edge_default() {
        let edge = Edge::default();
        assert_eq!(edge.entity_id, 0);
        assert_eq!(edge.relationship_type, "");
        assert_eq!(edge.src_node_id, 0);
        assert_eq!(edge.dst_node_id, 0);
        assert!(edge.properties.is_empty());
    }

    #[test]
    fn test_edge_clone() {
        let mut properties = HashMap::new();
        properties.insert("since".to_string(), FalkorValue::I64(2020));

        let edge = Edge {
            entity_id: 1,
            relationship_type: "KNOWS".to_string(),
            src_node_id: 1,
            dst_node_id: 2,
            properties: properties.clone(),
        };

        let edge_clone = edge.clone();
        assert_eq!(edge, edge_clone);
        assert_eq!(edge.entity_id, edge_clone.entity_id);
        assert_eq!(edge.relationship_type, edge_clone.relationship_type);
        assert_eq!(edge.src_node_id, edge_clone.src_node_id);
        assert_eq!(edge.dst_node_id, edge_clone.dst_node_id);
        assert_eq!(edge.properties, edge_clone.properties);
    }

    #[test]
    fn test_edge_debug() {
        let edge = Edge {
            entity_id: 42,
            relationship_type: "LIKES".to_string(),
            src_node_id: 1,
            dst_node_id: 2,
            properties: HashMap::new(),
        };
        let debug_str = format!("{:?}", edge);
        assert!(debug_str.contains("42"));
        assert!(debug_str.contains("LIKES"));
        assert!(debug_str.contains("1"));
        assert!(debug_str.contains("2"));
    }
}
