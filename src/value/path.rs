/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{parser::redis_value_as_vec, Edge, FalkorDBError, FalkorResult, GraphSchema, Node};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_default() {
        let path = Path::default();
        assert!(path.nodes.is_empty());
        assert!(path.relationships.is_empty());
    }

    #[test]
    fn test_path_clone() {
        let node1 = Node {
            entity_id: 1,
            labels: vec!["Person".to_string()],
            properties: std::collections::HashMap::new(),
        };
        let node2 = Node {
            entity_id: 2,
            labels: vec!["Person".to_string()],
            properties: std::collections::HashMap::new(),
        };
        let edge = Edge {
            entity_id: 1,
            relationship_type: "KNOWS".to_string(),
            src_node_id: 1,
            dst_node_id: 2,
            properties: std::collections::HashMap::new(),
        };

        let path = Path {
            nodes: vec![node1, node2],
            relationships: vec![edge],
        };

        let path_clone = path.clone();
        assert_eq!(path, path_clone);
        assert_eq!(path.nodes.len(), path_clone.nodes.len());
        assert_eq!(path.relationships.len(), path_clone.relationships.len());
    }

    #[test]
    fn test_path_debug() {
        let path = Path {
            nodes: vec![],
            relationships: vec![],
        };
        let debug_str = format!("{:?}", path);
        assert!(debug_str.contains("Path"));
        assert!(debug_str.contains("nodes"));
        assert!(debug_str.contains("relationships"));
    }

    #[test]
    fn test_path_equality() {
        let path1 = Path::default();
        let path2 = Path::default();
        assert_eq!(path1, path2);
    }

    #[test]
    fn test_path_with_nodes_and_edges() {
        let node = Node {
            entity_id: 1,
            labels: vec!["Test".to_string()],
            properties: std::collections::HashMap::new(),
        };
        let edge = Edge {
            entity_id: 1,
            relationship_type: "TEST_REL".to_string(),
            src_node_id: 1,
            dst_node_id: 2,
            properties: std::collections::HashMap::new(),
        };

        let path = Path {
            nodes: vec![node],
            relationships: vec![edge],
        };

        assert_eq!(path.nodes.len(), 1);
        assert_eq!(path.relationships.len(), 1);
        assert_eq!(path.nodes[0].entity_id, 1);
        assert_eq!(path.relationships[0].entity_id, 1);
    }
}
