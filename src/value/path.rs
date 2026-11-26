/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    parser::{redis_value_as_vec, ParserTypeMarker},
    Edge, FalkorDBError, FalkorResult, GraphSchema, Node,
};

/// `value` without its type marker, if it is the typed value `[marker, inner]`.
///
/// The server sends a path's two halves as typed arrays of typed entities:
/// `[[6, [[8, node], ...]], [6, [[7, edge], ...]]]`. Bare arrays of bare entities are read too.
#[cfg_attr(not(feature = "tokio"), allow(dead_code))]
pub(crate) fn untagged(
    value: &redis::Value,
    marker: ParserTypeMarker,
) -> &redis::Value {
    match value {
        redis::Value::Array(pair) => match pair.as_slice() {
            [redis::Value::Int(tag), inner] if *tag == marker as i64 => inner,
            _ => value,
        },
        _ => value,
    }
}

/// The owned form of [`untagged`].
fn into_untagged(
    value: redis::Value,
    marker: ParserTypeMarker,
) -> redis::Value {
    match value {
        redis::Value::Array(mut pair) if matches!(pair.as_slice(), [redis::Value::Int(tag), _] if *tag == marker as i64) => {
            pair.pop().expect("a typed value has two elements")
        }
        other => other,
    }
}

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
            nodes: redis_value_as_vec(into_untagged(nodes, ParserTypeMarker::Array))?
                .into_iter()
                .map(|node| Node::parse(into_untagged(node, ParserTypeMarker::Node), graph_schema))
                .collect::<FalkorResult<Vec<_>>>()?,
            relationships: redis_value_as_vec(into_untagged(
                relationships,
                ParserTypeMarker::Array,
            ))?
            .into_iter()
            .map(|edge| Edge::parse(into_untagged(edge, ParserTypeMarker::Edge), graph_schema))
            .collect::<FalkorResult<Vec<_>>>()?,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::graph_schema::SchemaNames;

    fn int(value: i64) -> redis::Value {
        redis::Value::Int(value)
    }

    fn array(values: Vec<redis::Value>) -> redis::Value {
        redis::Value::Array(values)
    }

    fn bulk(text: &str) -> redis::Value {
        redis::Value::BulkString(text.as_bytes().to_vec())
    }

    /// `RETURN p` for `p = (:A {d: 1.5})-[:R {w: 0.5}]->(:B {s: 'x'})`, as FalkorDB 4.22 and 6.2
    /// send it with `--compact`: a typed value whose two halves are typed arrays (6) of typed
    /// nodes (8) and edges (7).
    pub(crate) fn server_path() -> redis::Value {
        let node = |id, label, key, value: redis::Value| {
            array(vec![
                int(8),
                array(vec![
                    int(id),
                    array(vec![int(label)]),
                    array(vec![array(vec![int(key), int(2), value])]),
                ]),
            ])
        };
        let edge = array(vec![
            int(7),
            array(vec![
                int(0),
                int(0),
                int(0),
                int(1),
                array(vec![array(vec![int(2), int(5), bulk("0.5")])]),
            ]),
        ]);
        array(vec![
            int(9),
            array(vec![
                array(vec![
                    int(6),
                    array(vec![node(0, 0, 0, bulk("a")), node(1, 1, 1, bulk("x"))]),
                ]),
                array(vec![int(6), array(vec![edge])]),
            ]),
        ])
    }

    #[test]
    fn test_path_parse_reads_the_server_format() {
        let mut graph_schema = GraphSchema::from_reply_names(
            "test_path_server_format",
            1,
            SchemaNames {
                labels: vec!["A".to_string(), "B".to_string()],
                relationship_types: vec!["R".to_string()],
                property_keys: vec!["d".to_string(), "s".to_string(), "w".to_string()],
            },
        );
        let value = crate::parser::parse_raw_redis_value(server_path(), &mut graph_schema)
            .expect("the path parses");
        let crate::FalkorValue::Path(path) = value else {
            panic!("expected a path, got {value:?}");
        };
        assert_eq!(
            path.nodes
                .iter()
                .map(|node| node.labels.clone())
                .collect::<Vec<_>>(),
            [vec!["A".to_string()], vec!["B".to_string()]]
        );
        assert_eq!(
            path.nodes[1].properties["s"],
            crate::FalkorValue::String("x".into())
        );
        assert_eq!(path.relationships.len(), 1);
        assert_eq!(path.relationships[0].relationship_type, "R");
        assert_eq!(
            (
                path.relationships[0].src_node_id,
                path.relationships[0].dst_node_id
            ),
            (0, 1)
        );
        assert_eq!(
            path.relationships[0].properties["w"],
            crate::FalkorValue::F64(0.5)
        );
    }

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
    fn test_path_parse_propagates_node_error() {
        // A node value must be a 3-element array; this one has a single element, so `Node::parse`
        // fails. `Path::parse` must surface that error instead of silently dropping the node and
        // returning a truncated path.
        let malformed_node = redis::Value::Array(vec![redis::Value::Int(1)]);
        let path_value = redis::Value::Array(vec![
            redis::Value::Array(vec![malformed_node]),
            redis::Value::Array(vec![]),
        ]);

        let mut graph_schema = GraphSchema::new(
            "test_path_parse_propagates_node_error",
            crate::client::blocking::create_empty_inner_sync_client(),
        );

        let result = Path::parse(path_value, &mut graph_schema);
        assert!(
            matches!(
                result,
                Err(FalkorDBError::ParsingArrayToStructElementCount(_))
            ),
            "expected the node parse error to propagate, got {result:?}"
        );
    }

    #[test]
    fn test_path_parse_propagates_relationship_error() {
        // The relationships side must propagate errors too: an edge value that is not the expected
        // array shape must fail the whole path parse rather than being dropped.
        let malformed_edge = redis::Value::Int(7);
        let path_value = redis::Value::Array(vec![
            redis::Value::Array(vec![]),
            redis::Value::Array(vec![malformed_edge]),
        ]);

        let mut graph_schema = GraphSchema::new(
            "test_path_parse_propagates_relationship_error",
            crate::client::blocking::create_empty_inner_sync_client(),
        );

        assert!(Path::parse(path_value, &mut graph_schema).is_err());
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
