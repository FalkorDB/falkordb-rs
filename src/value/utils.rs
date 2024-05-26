/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection, FalkorDBError, FalkorParsable, FalkorValue,
    Point, SchemaType, SyncGraphSchema,
};
use anyhow::Result;
use std::collections::HashSet;

pub(crate) fn parse_labels(
    raw_ids: Vec<FalkorValue>,
    graph_schema: &SyncGraphSchema,
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

pub(crate) fn type_val_from_value(value: FalkorValue) -> Result<(i64, FalkorValue)> {
    let [type_marker, val]: [FalkorValue; 2] = value
        .into_vec()?
        .try_into()
        .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;
    let type_marker = type_marker.to_i64().ok_or(FalkorDBError::ParsingI64)?;

    Ok((type_marker, val))
}

pub(crate) fn parse_type(
    type_marker: i64,
    val: FalkorValue,
    graph_schema: &SyncGraphSchema,
    conn: &mut BorrowedSyncConnection,
) -> Result<FalkorValue> {
    let res = match type_marker {
        1 => FalkorValue::None,
        2 => FalkorValue::FString(val.into_string()?),
        3 => FalkorValue::Int64(val.to_i64().ok_or(FalkorDBError::ParsingI64)?),
        4 => FalkorValue::FBool(val.to_bool().ok_or(FalkorDBError::ParsingBool)?),
        5 => FalkorValue::F64(val.try_into()?),
        6 => FalkorValue::FArray({
            let val = val.into_vec()?;
            let mut parsed_vec = Vec::with_capacity(val.len());
            for item in val {
                let (type_marker, val) = type_val_from_value(item)?;
                parsed_vec.push(parse_type(type_marker, val, graph_schema, conn)?);
            }
            parsed_vec
        }),
        // The following types are sent as an array and require specific parsing functions
        7 => FalkorValue::FEdge(FalkorParsable::from_falkor_value(val, graph_schema, conn)?),
        8 => FalkorValue::FNode(FalkorParsable::from_falkor_value(val, graph_schema, conn)?),
        9 => FalkorValue::FPath(FalkorParsable::from_falkor_value(val, graph_schema, conn)?),
        10 => FalkorValue::FMap(FalkorParsable::from_falkor_value(val, graph_schema, conn)?),
        11 => FalkorValue::FPoint(Point::parse(val)?),
        _ => Err(FalkorDBError::ParsingUnknownType)?,
    };

    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_test_client;
    use std::collections::HashMap;

    fn open_readonly_graph() -> (SyncGraphSchema, BorrowedSyncConnection) {
        let client = create_test_client();
        let schema = client.open_graph("imdb").graph_schema.clone();
        let conn = client
            .borrow_connection()
            .expect("Could not borrow_connection");

        {
            let write_lock = schema.properties();
            *write_lock.write() = HashMap::from([
                (0, "age".to_string()),
                (1, "is_boring".to_string()),
                (2, "something_else".to_string()),
                (3, "secs_since_login".to_string()),
            ]);
        }

        {
            let write_lock = schema.labels();
            *write_lock.write() =
                HashMap::from([(0, "much".to_string()), (1, "actor".to_string())]);
        }

        {
            let write_lock = schema.relationships();
            *write_lock.write() = HashMap::from([(0, "very".to_string()), (1, "wow".to_string())]);
        }

        (schema, conn)
    }
    #[test]
    fn test_parse_edge() {
        let (schema, mut conn) = open_readonly_graph();

        let res = parse_type(
            7,
            FalkorValue::FArray(vec![
                FalkorValue::Int64(100), // edge id
                FalkorValue::Int64(0),   // edge type
                FalkorValue::Int64(51),  // src node
                FalkorValue::Int64(52),  // dst node
                FalkorValue::FArray(vec![
                    FalkorValue::FArray(vec![
                        FalkorValue::Int64(0),
                        FalkorValue::Int64(3),
                        FalkorValue::Int64(20),
                    ]),
                    FalkorValue::FArray(vec![
                        FalkorValue::Int64(1),
                        FalkorValue::Int64(4),
                        FalkorValue::FBool(false),
                    ]),
                ]),
            ]),
            &schema,
            &mut conn,
        );
        assert!(res.is_ok());

        let falkor_edge = res.unwrap();

        let FalkorValue::FEdge(edge) = falkor_edge else {
            panic!("Was not of type edge")
        };
        assert_eq!(edge.entity_id, 100);
        assert_eq!(edge.relationship_type, "very".to_string());
        assert_eq!(edge.src_node_id, 51);
        assert_eq!(edge.dst_node_id, 52);

        assert_eq!(edge.properties.len(), 2);
        assert_eq!(edge.properties.get("age"), Some(&FalkorValue::Int64(20)));
        assert_eq!(
            edge.properties.get("is_boring"),
            Some(&FalkorValue::FBool(false))
        );
    }

    #[test]
    fn test_parse_node() {
        let (schema, mut conn) = open_readonly_graph();

        let res = parse_type(
            8,
            FalkorValue::FArray(vec![
                FalkorValue::Int64(51), // node id
                FalkorValue::FArray(vec![FalkorValue::Int64(0), FalkorValue::Int64(1)]), // node type
                FalkorValue::FArray(vec![
                    FalkorValue::FArray(vec![
                        FalkorValue::Int64(0),
                        FalkorValue::Int64(3),
                        FalkorValue::Int64(15),
                    ]),
                    FalkorValue::FArray(vec![
                        FalkorValue::Int64(2),
                        FalkorValue::Int64(2),
                        FalkorValue::FString("the something".to_string()),
                    ]),
                    FalkorValue::FArray(vec![
                        FalkorValue::Int64(3),
                        FalkorValue::Int64(5),
                        FalkorValue::F64(105.5),
                    ]),
                ]),
            ]),
            &schema,
            &mut conn,
        );
        assert!(res.is_ok());

        let falkor_node = res.unwrap();
        let FalkorValue::FNode(node) = falkor_node else {
            panic!("Was not of type node")
        };

        assert_eq!(node.entity_id, 51);
        assert_eq!(node.labels, vec!["much".to_string(), "actor".to_string()]);
        assert_eq!(node.properties.len(), 3);
        assert_eq!(node.properties.get("age"), Some(&FalkorValue::Int64(15)));
        assert_eq!(
            node.properties.get("something_else"),
            Some(&FalkorValue::FString("the something".to_string()))
        );
        assert_eq!(
            node.properties.get(&"secs_since_login".to_string()),
            Some(&FalkorValue::F64(105.5))
        );
    }

    #[test]
    fn test_parse_path() {
        let (schema, mut conn) = open_readonly_graph();

        let res = parse_type(
            9,
            FalkorValue::FArray(vec![
                FalkorValue::FArray(vec![
                    FalkorValue::FArray(vec![
                        FalkorValue::Int64(51),
                        FalkorValue::FArray(vec![FalkorValue::Int64(0)]),
                        FalkorValue::FArray(vec![]),
                    ]),
                    FalkorValue::FArray(vec![
                        FalkorValue::Int64(52),
                        FalkorValue::FArray(vec![FalkorValue::Int64(0)]),
                        FalkorValue::FArray(vec![]),
                    ]),
                    FalkorValue::FArray(vec![
                        FalkorValue::Int64(53),
                        FalkorValue::FArray(vec![FalkorValue::Int64(0)]),
                        FalkorValue::FArray(vec![]),
                    ]),
                ]),
                FalkorValue::FArray(vec![
                    FalkorValue::FArray(vec![
                        FalkorValue::Int64(100),
                        FalkorValue::Int64(0),
                        FalkorValue::Int64(51),
                        FalkorValue::Int64(52),
                        FalkorValue::FArray(vec![]),
                    ]),
                    FalkorValue::FArray(vec![
                        FalkorValue::Int64(101),
                        FalkorValue::Int64(1),
                        FalkorValue::Int64(52),
                        FalkorValue::Int64(53),
                        FalkorValue::FArray(vec![]),
                    ]),
                ]),
            ]),
            &schema,
            &mut conn,
        );
        assert!(res.is_ok());

        let falkor_path = res.unwrap();
        let FalkorValue::FPath(path) = falkor_path else {
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
        let (schema, mut conn) = open_readonly_graph();

        let res = parse_type(
            10,
            FalkorValue::FArray(vec![
                FalkorValue::FString("key0".to_string()),
                FalkorValue::FArray(vec![
                    FalkorValue::Int64(2),
                    FalkorValue::FString("val0".to_string()),
                ]),
                FalkorValue::FString("key1".to_string()),
                FalkorValue::FArray(vec![FalkorValue::Int64(3), FalkorValue::Int64(1)]),
                FalkorValue::FString("key2".to_string()),
                FalkorValue::FArray(vec![FalkorValue::Int64(4), FalkorValue::FBool(true)]),
            ]),
            &schema,
            &mut conn,
        );
        assert!(res.is_ok());

        let falkor_map = res.unwrap();
        let FalkorValue::FMap(map) = falkor_map else {
            panic!("Is not of type map")
        };

        assert_eq!(map.len(), 3);
        assert_eq!(
            map.get("key0"),
            Some(&FalkorValue::FString("val0".to_string()))
        );
        assert_eq!(map.get("key1"), Some(&FalkorValue::Int64(1)));
        assert_eq!(map.get("key2"), Some(&FalkorValue::FBool(true)));
    }

    #[test]
    fn test_parse_point() {
        let (schema, mut conn) = open_readonly_graph();

        let res = parse_type(
            11,
            FalkorValue::FArray(vec![FalkorValue::F64(102.0), FalkorValue::F64(15.2)]),
            &schema,
            &mut conn,
        );
        assert!(res.is_ok());

        let falkor_point = res.unwrap();
        let FalkorValue::FPoint(point) = falkor_point else {
            panic!("Is not of type point")
        };
        assert_eq!(point.latitude, 102.0);
        assert_eq!(point.longitude, 15.2);
    }
}
