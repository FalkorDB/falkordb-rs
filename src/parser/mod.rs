/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    value::vec32::Vec32, ConfigValue, Edge, FalkorDBError, FalkorResult, FalkorValue, GraphSchema,
    Node, Path, Point,
};
use std::collections::HashMap;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub enum ParserTypeMarker {
    None = 1,
    String = 2,
    I64 = 3,
    Bool = 4,
    F64 = 5,
    Array = 6,
    Edge = 7,
    Node = 8,
    Path = 9,
    Map = 10,
    Point = 11,
    Vec32 = 12,
}

impl TryFrom<i64> for ParserTypeMarker {
    type Error = FalkorDBError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(match value {
            1 => Self::None,
            2 => Self::String,
            3 => Self::I64,
            4 => Self::Bool,
            5 => Self::F64,
            6 => Self::Array,
            7 => Self::Edge,
            8 => Self::Node,
            9 => Self::Path,
            10 => Self::Map,
            11 => Self::Point,
            12 => Self::Vec32,
            _ => Err(FalkorDBError::ParsingUnknownType)?,
        })
    }
}

pub fn redis_value_as_string(value: redis::Value) -> FalkorResult<String> {
    match value {
        redis::Value::BulkString(data) => {
            String::from_utf8(data).map_err(|_| FalkorDBError::ParsingString)
        }
        redis::Value::SimpleString(data) => Ok(data),
        redis::Value::VerbatimString { format: _, text } => Ok(text),
        _ => Err(FalkorDBError::ParsingString),
    }
}

#[allow(clippy::needless_pass_by_value)]
pub fn redis_value_as_int(value: redis::Value) -> FalkorResult<i64> {
    match value {
        redis::Value::Int(int_val) => Ok(int_val),
        _ => Err(FalkorDBError::ParsingI64),
    }
}

pub fn redis_value_as_bool(value: redis::Value) -> FalkorResult<bool> {
    redis_value_as_string(value).and_then(|string_val| match string_val.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(FalkorDBError::ParsingBool),
    })
}

pub fn redis_value_as_double(value: redis::Value) -> FalkorResult<f64> {
    redis_value_as_string(value)
        .and_then(|string_val| string_val.parse().map_err(|_| FalkorDBError::ParsingF64))
}

pub fn redis_value_as_float(value: redis::Value) -> FalkorResult<f32> {
    redis_value_as_string(value)
        .and_then(|string_val| string_val.parse().map_err(|_| FalkorDBError::ParsingF32))
}

pub fn redis_value_as_vec(value: redis::Value) -> FalkorResult<Vec<redis::Value>> {
    match value {
        redis::Value::Array(bulk_val) => Ok(bulk_val),
        _ => Err(FalkorDBError::ParsingArray),
    }
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Parse Redis Info", skip_all, level = "info")
)]
pub fn parse_redis_info(res: redis::Value) -> FalkorResult<HashMap<String, String>> {
    redis_value_as_string(res)
        .map(|info| {
            info.split("\r\n")
                .map(|info_item| info_item.split(':').collect::<Vec<_>>())
                .flat_map(TryInto::<[&str; 2]>::try_into)
                .map(|[key, val]| (key.to_string(), val.to_string()))
                .collect()
        })
        .map_err(|_| FalkorDBError::ParsingString)
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Parse Config Hashmap", skip_all, level = "info")
)]
pub fn parse_config_hashmap(value: redis::Value) -> FalkorResult<HashMap<String, ConfigValue>> {
    let config = redis_value_as_vec(value)?;

    if config.len() == 2 {
        let [key, val]: [redis::Value; 2] = config.try_into().map_err(|_| {
            FalkorDBError::ParsingArrayToStructElementCount(
                "Expected exactly 2 elements for configuration option",
            )
        })?;

        return redis_value_as_string(key)
            .and_then(|key| ConfigValue::try_from(val).map(|val| HashMap::from([(key, val)])));
    }

    Ok(config
        .into_iter()
        .flat_map(|config| {
            redis_value_as_vec(config).and_then(|as_vec| {
                let [key, val]: [redis::Value; 2] = as_vec.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "Expected exactly 2 elements for configuration option",
                    )
                })?;

                Result::<_, FalkorDBError>::Ok((
                    redis_value_as_string(key)?,
                    ConfigValue::try_from(val)?,
                ))
            })
        })
        .collect::<HashMap<String, ConfigValue>>())
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Parse Falkor Enum", skip_all, level = "trace")
)]
pub fn parse_falkor_enum<T: for<'a> TryFrom<&'a str, Error = impl ToString>>(
    value: redis::Value
) -> FalkorResult<T> {
    type_val_from_value(value)
        .and_then(|(type_marker, val)| {
            if type_marker == ParserTypeMarker::String {
                redis_value_as_string(val)
            } else {
                Err(FalkorDBError::ParsingArray)
            }
        })
        .and_then(|val_string| {
            T::try_from(val_string.as_str())
                .map_err(|err| FalkorDBError::InvalidEnumType(err.to_string()))
        })
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(
        name = "Falkor Typed String From Redis Value",
        skip_all,
        level = "trace"
    )
)]
pub fn redis_value_as_typed_string(value: redis::Value) -> FalkorResult<String> {
    type_val_from_value(value).and_then(|(type_marker, val)| {
        if type_marker == ParserTypeMarker::String {
            redis_value_as_string(val)
        } else {
            Err(FalkorDBError::ParsingString)
        }
    })
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "String Vec From Redis Value", skip_all, level = "debug")
)]
pub fn redis_value_as_typed_string_vec(value: redis::Value) -> FalkorResult<Vec<String>> {
    type_val_from_value(value)
        .and_then(|(type_marker, val)| {
            if type_marker == ParserTypeMarker::Array {
                redis_value_as_vec(val)
            } else {
                Err(FalkorDBError::ParsingArray)
            }
        })
        .map(|val_vec| {
            val_vec
                .into_iter()
                .flat_map(redis_value_as_string)
                .collect()
        })
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "String Vec From Untyped Value", skip_all, level = "trace")
)]
pub fn redis_value_as_untyped_string_vec(value: redis::Value) -> FalkorResult<Vec<String>> {
    redis_value_as_vec(value)
        .map(|as_vec| as_vec.into_iter().flat_map(redis_value_as_string).collect())
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Parse Header", skip_all, level = "info")
)]
pub fn parse_header(header: redis::Value) -> FalkorResult<Vec<String>> {
    // Convert the header into a sequence
    let header_sequence = redis_value_as_vec(header)?;

    // Initialize a vector with the capacity of the header sequence length
    let header_sequence_len = header_sequence.len();

    header_sequence.into_iter().try_fold(
        Vec::with_capacity(header_sequence_len),
        |mut result, item| {
            // Convert the item into a sequence
            let item_sequence = redis_value_as_vec(item)?;

            // Determine the key based on the length of the item sequence
            let key = if item_sequence.len() == 2 {
                // Extract the key from a 2-element array
                let [_val, key]: [redis::Value; 2] = item_sequence.try_into().map_err(|_| {
                    FalkorDBError::ParsingHeader(
                        "Could not get 2-sized array despite there being 2 elements",
                    )
                })?;
                key
            } else {
                // Get the first element from the item sequence
                item_sequence.into_iter().next().ok_or({
                    FalkorDBError::ParsingHeader("Expected at least one item in header vector")
                })?
            };

            // Convert the key to a string and push it to the result vector
            result.push(redis_value_as_string(key)?);
            Ok(result)
        },
    )
}
#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Parse Raw Redis Value", skip_all, level = "debug")
)]
pub fn parse_raw_redis_value(
    value: redis::Value,
    graph_schema: &mut GraphSchema,
) -> FalkorResult<FalkorValue> {
    type_val_from_value(value)
        .and_then(|(type_marker, val)| parse_type(type_marker, val, graph_schema))
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "TypeVal From Value", skip_all, level = "trace")
)]
pub fn type_val_from_value(
    value: redis::Value
) -> Result<(ParserTypeMarker, redis::Value), FalkorDBError> {
    redis_value_as_vec(value).and_then(|val_vec| {
        val_vec
            .try_into()
            .map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 2 elements: type marker, and value",
                )
            })
            .and_then(|[type_marker_raw, val]: [redis::Value; 2]| {
                redis_value_as_int(type_marker_raw)
                    .and_then(ParserTypeMarker::try_from)
                    .map(|type_marker| (type_marker, val))
            })
    })
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Parse Regular Falkor Map", skip_all, level = "debug")
)]
fn parse_regular_falkor_map(
    value: redis::Value,
    graph_schema: &mut GraphSchema,
) -> FalkorResult<HashMap<String, FalkorValue>> {
    value
        .into_map_iter()
        .map_err(|_| FalkorDBError::ParsingMap)?
        .try_fold(HashMap::new(), |mut out_map, (key, val)| {
            out_map.insert(
                redis_value_as_string(key)?,
                parse_raw_redis_value(val, graph_schema)?,
            );
            Ok(out_map)
        })
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Parse Element With Type Marker", skip_all, level = "trace")
)]
pub fn parse_type(
    type_marker: ParserTypeMarker,
    val: redis::Value,
    graph_schema: &mut GraphSchema,
) -> Result<FalkorValue, FalkorDBError> {
    let res = match type_marker {
        ParserTypeMarker::None => FalkorValue::None,
        ParserTypeMarker::String => FalkorValue::String(redis_value_as_string(val)?),
        ParserTypeMarker::I64 => FalkorValue::I64(redis_value_as_int(val)?),
        ParserTypeMarker::Bool => FalkorValue::Bool(redis_value_as_bool(val)?),
        ParserTypeMarker::F64 => FalkorValue::F64(redis_value_as_double(val)?),
        ParserTypeMarker::Array => {
            FalkorValue::Array(redis_value_as_vec(val).and_then(|val_vec| {
                let len = val_vec.len();
                val_vec
                    .into_iter()
                    .try_fold(Vec::with_capacity(len), |mut acc, item| {
                        acc.push(parse_raw_redis_value(item, graph_schema)?);
                        Ok(acc)
                    })
            })?)
        }
        ParserTypeMarker::Edge => FalkorValue::Edge(Edge::parse(val, graph_schema)?),
        ParserTypeMarker::Node => FalkorValue::Node(Node::parse(val, graph_schema)?),
        ParserTypeMarker::Path => FalkorValue::Path(Path::parse(val, graph_schema)?),
        ParserTypeMarker::Map => FalkorValue::Map(parse_regular_falkor_map(val, graph_schema)?),
        ParserTypeMarker::Point => FalkorValue::Point(Point::parse(val)?),
        ParserTypeMarker::Vec32 => FalkorValue::Vec32(Vec32::parse(val)?),
    };

    Ok(res)
}

pub trait SchemaParsable: Sized {
    fn parse(
        value: redis::Value,
        graph_schema: &mut GraphSchema,
    ) -> FalkorResult<Self>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        client::blocking::create_empty_inner_sync_client, graph::HasGraphSchema,
        graph_schema::tests::open_readonly_graph_with_modified_schema, FalkorDBError,
    };

    #[test]
    fn test_parse_header_valid_single_key() {
        let header =
            redis::Value::Array(vec![redis::Value::Array(vec![redis::Value::BulkString(
                b"key1".to_vec(),
            )])]);
        let result = parse_header(header);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["key1".to_string()]);
    }

    #[test]
    fn test_parse_header_valid_multiple_keys() {
        let header = redis::Value::Array(vec![
            redis::Value::Array(vec![
                redis::Value::BulkString(b"type".to_vec()),
                redis::Value::BulkString(b"header1".to_vec()),
            ]),
            redis::Value::Array(vec![redis::Value::BulkString(b"key2".to_vec())]),
        ]);
        let result = parse_header(header);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            vec!["header1".to_string(), "key2".to_string()]
        );
    }

    #[test]
    fn test_parse_header_empty_header() {
        let header = redis::Value::Array(vec![]);
        let result = parse_header(header);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::<String>::new());
    }

    #[test]
    fn test_parse_header_empty_vec() {
        let header = redis::Value::Array(vec![redis::Value::Array(vec![])]);
        let result = parse_header(header);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            FalkorDBError::ParsingHeader("Expected at least one item in header vector")
        );
    }

    #[test]
    fn test_parse_header_many_elements() {
        let header = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"just_some_header".to_vec()),
            redis::Value::BulkString(b"header1".to_vec()),
            redis::Value::BulkString(b"extra".to_vec()),
        ])]);
        let result = parse_header(header);
        assert!(result.is_ok());
        assert_eq!(result.unwrap()[0], "just_some_header");
    }

    #[test]
    fn test_parse_edge() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            ParserTypeMarker::Edge,
            redis::Value::Array(vec![
                redis::Value::Int(100), // edge id
                redis::Value::Int(0),   // edge type
                redis::Value::Int(51),  // src node
                redis::Value::Int(52),  // dst node
                redis::Value::Array(vec![
                    redis::Value::Array(vec![
                        redis::Value::Int(0),
                        redis::Value::Int(3),
                        redis::Value::Int(20),
                    ]),
                    redis::Value::Array(vec![
                        redis::Value::Int(1),
                        redis::Value::Int(4),
                        redis::Value::SimpleString("false".to_string()),
                    ]),
                ]),
            ]),
            graph.get_graph_schema_mut(),
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
            Some(&FalkorValue::Bool(false))
        );
    }

    #[test]
    fn test_parse_node() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            ParserTypeMarker::Node,
            redis::Value::Array(vec![
                redis::Value::Int(51), // node id
                redis::Value::Array(vec![redis::Value::Int(0), redis::Value::Int(1)]), // node type
                redis::Value::Array(vec![
                    redis::Value::Array(vec![
                        redis::Value::Int(0),
                        redis::Value::Int(3),
                        redis::Value::Int(15),
                    ]),
                    redis::Value::Array(vec![
                        redis::Value::Int(2),
                        redis::Value::Int(2),
                        redis::Value::SimpleString("the something".to_string()),
                    ]),
                    redis::Value::Array(vec![
                        redis::Value::Int(3),
                        redis::Value::Int(5),
                        redis::Value::SimpleString("105.5".to_string()),
                    ]),
                ]),
            ]),
            graph.get_graph_schema_mut(),
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
            Some(&FalkorValue::F64(105.5))
        );
    }

    #[test]
    fn test_parse_path() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            ParserTypeMarker::Path,
            redis::Value::Array(vec![
                redis::Value::Array(vec![
                    redis::Value::Array(vec![
                        redis::Value::Int(51),
                        redis::Value::Array(vec![redis::Value::Int(0)]),
                        redis::Value::Array(vec![]),
                    ]),
                    redis::Value::Array(vec![
                        redis::Value::Int(52),
                        redis::Value::Array(vec![redis::Value::Int(0)]),
                        redis::Value::Array(vec![]),
                    ]),
                    redis::Value::Array(vec![
                        redis::Value::Int(53),
                        redis::Value::Array(vec![redis::Value::Int(0)]),
                        redis::Value::Array(vec![]),
                    ]),
                ]),
                redis::Value::Array(vec![
                    redis::Value::Array(vec![
                        redis::Value::Int(100),
                        redis::Value::Int(0),
                        redis::Value::Int(51),
                        redis::Value::Int(52),
                        redis::Value::Array(vec![]),
                    ]),
                    redis::Value::Array(vec![
                        redis::Value::Int(101),
                        redis::Value::Int(1),
                        redis::Value::Int(52),
                        redis::Value::Int(53),
                        redis::Value::Array(vec![]),
                    ]),
                ]),
            ]),
            graph.get_graph_schema_mut(),
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
            ParserTypeMarker::Map,
            redis::Value::Array(vec![
                redis::Value::SimpleString("key0".to_string()),
                redis::Value::Array(vec![
                    redis::Value::Int(2),
                    redis::Value::SimpleString("val0".to_string()),
                ]),
                redis::Value::SimpleString("key1".to_string()),
                redis::Value::Array(vec![redis::Value::Int(3), redis::Value::Int(1)]),
                redis::Value::SimpleString("key2".to_string()),
                redis::Value::Array(vec![
                    redis::Value::Int(4),
                    redis::Value::SimpleString("true".to_string()),
                ]),
            ]),
            graph.get_graph_schema_mut(),
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
        assert_eq!(map.get("key2"), Some(&FalkorValue::Bool(true)));
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_parse_point() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            ParserTypeMarker::Point,
            redis::Value::Array(vec![
                redis::Value::SimpleString("102.0".to_string()),
                redis::Value::SimpleString("15.2".to_string()),
            ]),
            graph.get_graph_schema_mut(),
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
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_sync_client());

        let res = parse_regular_falkor_map(
            redis::Value::SimpleString("Hello".to_string()),
            &mut graph_schema,
        );

        assert!(res.is_err());
    }

    #[test]
    fn test_map_vec_odd_element_count() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_sync_client());

        let res = parse_regular_falkor_map(
            redis::Value::Array(vec![redis::Value::Nil; 7]),
            &mut graph_schema,
        );

        assert!(res.is_err());
    }

    #[test]
    fn test_map_val_element_is_not_array() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_sync_client());

        let res = parse_regular_falkor_map(
            redis::Value::Array(vec![
                redis::Value::SimpleString("Key".to_string()),
                redis::Value::SimpleString("false".to_string()),
            ]),
            &mut graph_schema,
        );

        assert!(res.is_err());
    }

    #[test]
    fn test_map_val_element_has_only_1_element() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_sync_client());

        let res = parse_regular_falkor_map(
            redis::Value::Array(vec![
                redis::Value::SimpleString("Key".to_string()),
                redis::Value::Array(vec![redis::Value::Int(7)]),
            ]),
            &mut graph_schema,
        );

        assert!(res.is_err());
    }

    #[test]
    fn test_map_val_element_has_ge_2_elements() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_sync_client());

        let res = parse_regular_falkor_map(
            redis::Value::Array(vec![
                redis::Value::SimpleString("Key".to_string()),
                redis::Value::Array(vec![redis::Value::Int(3); 3]),
            ]),
            &mut graph_schema,
        );

        assert!(res.is_err());
    }

    #[test]
    fn test_map_val_element_mismatch_type_marker() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_sync_client());

        let res = parse_regular_falkor_map(
            redis::Value::Array(vec![
                redis::Value::SimpleString("Key".to_string()),
                redis::Value::Array(vec![
                    redis::Value::Int(3),
                    redis::Value::SimpleString("true".to_string()),
                ]),
            ]),
            &mut graph_schema,
        );

        assert!(res.is_err());
    }

    #[test]
    fn test_map_ok_values() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_inner_sync_client());

        let res = parse_regular_falkor_map(
            redis::Value::Array(vec![
                redis::Value::SimpleString("IntKey".to_string()),
                redis::Value::Array(vec![redis::Value::Int(3), redis::Value::Int(1)]),
                redis::Value::SimpleString("BoolKey".to_string()),
                redis::Value::Array(vec![
                    redis::Value::Int(4),
                    redis::Value::SimpleString("true".to_string()),
                ]),
            ]),
            &mut graph_schema,
        )
        .expect("Could not parse map");

        assert_eq!(res.get("IntKey"), Some(FalkorValue::I64(1)).as_ref());
        assert_eq!(res.get("BoolKey"), Some(FalkorValue::Bool(true)).as_ref());
    }
}
