/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::redis_ext::{
    redis_value_as_bool, redis_value_as_double, redis_value_as_int, redis_value_as_string,
};
use crate::{Edge, FalkorDBError, FalkorResult, FalkorValue, GraphSchema, Node, Path, Point};
use std::collections::HashMap;

pub(crate) fn type_val_from_value(
    value: redis::Value
) -> Result<(i64, redis::Value), FalkorDBError> {
    let [type_marker, val]: [redis::Value; 2] = value
        .into_sequence()
        .map_err(|_| FalkorDBError::ParsingArray)?
        .try_into()
        .map_err(|_| {
            FalkorDBError::ParsingArrayToStructElementCount(
                "Expected exactly 2 elements: type marker, and value",
            )
        })?;

    Ok((redis_value_as_int(type_marker)?, val))
}

fn parse_regular_falkor_map(
    value: redis::Value,
    graph_schema: &mut GraphSchema,
) -> FalkorResult<HashMap<String, FalkorValue>> {
    value
        .into_map_iter()
        .map_err(|_| FalkorDBError::ParsingFMap)?
        .try_fold(HashMap::new(), |mut out_map, (key, val)| {
            let [type_marker, val]: [redis::Value; 2] = val
                .into_sequence()
                .ok()
                .and_then(|val_seq| val_seq.try_into().ok())
                .ok_or(FalkorDBError::ParsingFMap)?;

            out_map.insert(
                redis_value_as_string(key)?,
                parse_type(redis_value_as_int(type_marker)?, val, graph_schema)?,
            );
            Ok(out_map)
        })
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Parse Element With Type Marker", skip_all)
)]
pub(crate) fn parse_type(
    type_marker: i64,
    val: redis::Value,
    graph_schema: &mut GraphSchema,
) -> Result<FalkorValue, FalkorDBError> {
    let res = match type_marker {
        1 => FalkorValue::None,
        2 => FalkorValue::String(redis_value_as_string(val)?),
        3 => FalkorValue::I64(redis_value_as_int(val)?),
        4 => FalkorValue::Bool(redis_value_as_bool(val)?),
        5 => FalkorValue::F64(redis_value_as_double(val)?),
        6 => FalkorValue::Array(
            val.into_sequence()
                .map_err(|_| FalkorDBError::ParsingArray)?
                .into_iter()
                .flat_map(|item| {
                    type_val_from_value(item)
                        .and_then(|(type_marker, val)| parse_type(type_marker, val, graph_schema))
                })
                .collect(),
        ),
        // The following types are sent as an array and require specific parsing functions
        7 => FalkorValue::Edge(Edge::parse(val, graph_schema)?),
        8 => FalkorValue::Node(Node::parse(val, graph_schema)?),
        9 => FalkorValue::Path(Path::parse(val, graph_schema)?),
        10 => FalkorValue::Map(parse_regular_falkor_map(val, graph_schema)?),
        11 => FalkorValue::Point(Point::parse(val)?),
        _ => Err(FalkorDBError::ParsingUnknownType)?,
    };

    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        client::blocking::create_empty_inner_client,
        graph_schema::tests::open_readonly_graph_with_modified_schema,
    };

    #[test]
    fn test_parse_edge() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            7,
            redis::Value::Bulk(vec![
                redis::Value::Int(100), // edge id
                redis::Value::Int(0),   // edge type
                redis::Value::Int(51),  // src node
                redis::Value::Int(52),  // dst node
                redis::Value::Bulk(vec![
                    redis::Value::Bulk(vec![
                        redis::Value::Int(0),
                        redis::Value::Int(3),
                        redis::Value::Int(20),
                    ]),
                    redis::Value::Bulk(vec![
                        redis::Value::Int(1),
                        redis::Value::Int(4),
                        redis::Value::Status("false".to_string()),
                    ]),
                ]),
            ]),
            &mut graph.graph_schema,
        );
        assert!(res.is_ok());

        let falkor_edge = res.unwrap();

        let FalkorValue::Edge(edge) = falkor_edge else {
            panic!("Was not of type edge")
        };
        assert_eq!(edge.entity_id, 100);
        assert_eq!(edge.relationship_type, "very".to_string());
        assert_eq!(edge.src_node_id, 51);
        assert_eq!(edge.dst_node_id, 52);

        assert_eq!(edge.properties.len(), 2);
        assert_eq!(edge.properties.get("age"), Some(&FalkorValue::I64(20)));
        assert_eq!(
            edge.properties.get("is_boring"),
            Some(&FalkorValue::String("false".to_string()))
        );
    }

    #[test]
    fn test_parse_node() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            8,
            redis::Value::Bulk(vec![
                redis::Value::Int(51),                                                // node id
                redis::Value::Bulk(vec![redis::Value::Int(0), redis::Value::Int(1)]), // node type
                redis::Value::Bulk(vec![
                    redis::Value::Bulk(vec![
                        redis::Value::Int(0),
                        redis::Value::Int(3),
                        redis::Value::Int(15),
                    ]),
                    redis::Value::Bulk(vec![
                        redis::Value::Int(2),
                        redis::Value::Int(2),
                        redis::Value::Status("the something".to_string()),
                    ]),
                    redis::Value::Bulk(vec![
                        redis::Value::Int(3),
                        redis::Value::Int(5),
                        redis::Value::Status("105.5".to_string()),
                    ]),
                ]),
            ]),
            &mut graph.graph_schema,
        );
        assert!(res.is_ok());

        let falkor_node = res.unwrap();
        let FalkorValue::Node(node) = falkor_node else {
            panic!("Was not of type node")
        };

        assert_eq!(node.entity_id, 51);
        assert_eq!(node.labels, vec!["much".to_string(), "actor".to_string()]);
        assert_eq!(node.properties.len(), 3);
        assert_eq!(node.properties.get("age"), Some(&FalkorValue::I64(15)));
        assert_eq!(
            node.properties.get("something_else"),
            Some(&FalkorValue::String("the something".to_string()))
        );
        assert_eq!(
            node.properties.get("secs_since_login"),
            Some(&FalkorValue::String("105.5".to_string()))
        );
    }

    #[test]
    fn test_parse_path() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            9,
            redis::Value::Bulk(vec![
                redis::Value::Bulk(vec![
                    redis::Value::Bulk(vec![
                        redis::Value::Int(51),
                        redis::Value::Bulk(vec![redis::Value::Int(0)]),
                        redis::Value::Bulk(vec![]),
                    ]),
                    redis::Value::Bulk(vec![
                        redis::Value::Int(52),
                        redis::Value::Bulk(vec![redis::Value::Int(0)]),
                        redis::Value::Bulk(vec![]),
                    ]),
                    redis::Value::Bulk(vec![
                        redis::Value::Int(53),
                        redis::Value::Bulk(vec![redis::Value::Int(0)]),
                        redis::Value::Bulk(vec![]),
                    ]),
                ]),
                redis::Value::Bulk(vec![
                    redis::Value::Bulk(vec![
                        redis::Value::Int(100),
                        redis::Value::Int(0),
                        redis::Value::Int(51),
                        redis::Value::Int(52),
                        redis::Value::Bulk(vec![]),
                    ]),
                    redis::Value::Bulk(vec![
                        redis::Value::Int(101),
                        redis::Value::Int(1),
                        redis::Value::Int(52),
                        redis::Value::Int(53),
                        redis::Value::Bulk(vec![]),
                    ]),
                ]),
            ]),
            &mut graph.graph_schema,
        );
        assert!(res.is_ok());

        let falkor_path = res.unwrap();
        let FalkorValue::Path(path) = falkor_path else {
            panic!("Is not of type path")
        };

        assert_eq!(path.nodes.len(), 3);
        assert_eq!(path.nodes[0].entity_id, 51);
        assert_eq!(path.nodes[1].entity_id, 52);
        assert_eq!(path.nodes[2].entity_id, 53);

        assert_eq!(path.relationships.len(), 2);
        assert_eq!(path.relationships[0].entity_id, 100);
        assert_eq!(path.relationships[1].entity_id, 101);

        assert_eq!(path.relationships[0].src_node_id, 51);
        assert_eq!(path.relationships[0].dst_node_id, 52);

        assert_eq!(path.relationships[1].src_node_id, 52);
        assert_eq!(path.relationships[1].dst_node_id, 53);
    }

    #[test]
    fn test_parse_map() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            10,
            redis::Value::Bulk(vec![
                redis::Value::Status("key0".to_string()),
                redis::Value::Bulk(vec![
                    redis::Value::Int(2),
                    redis::Value::Status("val0".to_string()),
                ]),
                redis::Value::Status("key1".to_string()),
                redis::Value::Bulk(vec![redis::Value::Int(3), redis::Value::Int(1)]),
                redis::Value::Status("key2".to_string()),
                redis::Value::Bulk(vec![
                    redis::Value::Int(4),
                    redis::Value::Status("true".to_string()),
                ]),
            ]),
            &mut graph.graph_schema,
        );
        assert!(res.is_ok());

        let falkor_map = res.unwrap();
        let FalkorValue::Map(map) = falkor_map else {
            panic!("Is not of type map")
        };

        assert_eq!(map.len(), 3);
        assert_eq!(
            map.get("key0"),
            Some(&FalkorValue::String("val0".to_string()))
        );
        assert_eq!(map.get("key1"), Some(&FalkorValue::I64(1)));
        assert_eq!(
            map.get("key2"),
            Some(&FalkorValue::String("true".to_string()))
        );
    }

    #[test]
    fn test_parse_point() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            11,
            redis::Value::Bulk(vec![
                redis::Value::Status("102.0".to_string()),
                redis::Value::Status("15.2".to_string()),
            ]),
            &mut graph.graph_schema,
        );
        assert!(res.is_ok());

        let falkor_point = res.unwrap();
        let FalkorValue::Point(point) = falkor_point else {
            panic!("Is not of type point")
        };
        assert_eq!(point.latitude, 102.0);
        assert_eq!(point.longitude, 15.2);
    }

    #[test]
    fn test_map_not_a_vec() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_client());

        let res =
            parse_regular_falkor_map(redis::Value::Status("Hello".to_string()), &mut graph_schema);

        assert!(res.is_err())
    }

    #[test]
    fn test_map_vec_odd_element_count() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_client());

        let res = parse_regular_falkor_map(
            redis::Value::Bulk(vec![redis::Value::Nil; 7]),
            &mut graph_schema,
        );

        assert!(res.is_err())
    }

    #[test]
    fn test_map_val_element_is_not_array() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_client());

        let res = parse_regular_falkor_map(
            redis::Value::Bulk(vec![
                redis::Value::Status("Key".to_string()),
                redis::Value::Status("false".to_string()),
            ]),
            &mut graph_schema,
        );

        assert!(res.is_err())
    }

    #[test]
    fn test_map_val_element_has_only_1_element() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_client());

        let res = parse_regular_falkor_map(
            redis::Value::Bulk(vec![
                redis::Value::Status("Key".to_string()),
                redis::Value::Bulk(vec![redis::Value::Int(7)]),
            ]),
            &mut graph_schema,
        );

        assert!(res.is_err())
    }

    #[test]
    fn test_map_val_element_has_ge_2_elements() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_client());

        let res = parse_regular_falkor_map(
            redis::Value::Bulk(vec![
                redis::Value::Status("Key".to_string()),
                redis::Value::Bulk(vec![redis::Value::Int(3); 3]),
            ]),
            &mut graph_schema,
        );

        assert!(res.is_err())
    }

    #[test]
    fn test_map_val_element_mismatch_type_marker() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_client());

        let res = parse_regular_falkor_map(
            redis::Value::Bulk(vec![
                redis::Value::Status("Key".to_string()),
                redis::Value::Bulk(vec![
                    redis::Value::Int(3),
                    redis::Value::Status("true".to_string()),
                ]),
            ]),
            &mut graph_schema,
        );

        assert!(res.is_err())
    }

    #[test]
    fn test_map_ok_values() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_client());

        let res = parse_regular_falkor_map(
            redis::Value::Bulk(vec![
                redis::Value::Status("IntKey".to_string()),
                redis::Value::Bulk(vec![redis::Value::Int(3), redis::Value::Int(1)]),
                redis::Value::Status("BoolKey".to_string()),
                redis::Value::Bulk(vec![
                    redis::Value::Int(4),
                    redis::Value::Status("true".to_string()),
                ]),
            ]),
            &mut graph_schema,
        )
        .expect("Could not parse map");

        assert_eq!(res.get("IntKey"), Some(FalkorValue::I64(1)).as_ref());
        assert_eq!(
            res.get("BoolKey"),
            Some(FalkorValue::String("true".to_string())).as_ref()
        );
    }
}
