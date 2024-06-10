/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::ProvidesSyncConnections, FalkorDBError, FalkorParsable, FalkorValue, GraphSchema, Point,
};

pub(crate) fn type_val_from_value(value: FalkorValue) -> Result<(i64, FalkorValue), FalkorDBError> {
    let [type_marker, val]: [FalkorValue; 2] = value.into_vec()?.try_into().map_err(|_| {
        FalkorDBError::ParsingArrayToStructElementCount(
            "Expected exactly 2 elements: type marker, and value".to_string(),
        )
    })?;
    let type_marker = type_marker.to_i64().ok_or(FalkorDBError::ParsingI64)?;

    Ok((type_marker, val))
}

pub(crate) fn parse_type<C: ProvidesSyncConnections>(
    type_marker: i64,
    val: FalkorValue,
    graph_schema: &mut GraphSchema<C>,
) -> Result<FalkorValue, FalkorDBError> {
    let res = match type_marker {
        1 => FalkorValue::None,
        2 => FalkorValue::String(val.into_string()?),
        3 => FalkorValue::I64(val.to_i64().ok_or(FalkorDBError::ParsingI64)?),
        4 => FalkorValue::Bool(val.to_bool().ok_or(FalkorDBError::ParsingBool)?),
        5 => FalkorValue::F64(val.try_into()?),
        6 => FalkorValue::Array(
            val.into_vec()?
                .into_iter()
                .flat_map(|item| {
                    type_val_from_value(item)
                        .and_then(|(type_marker, val)| parse_type(type_marker, val, graph_schema))
                })
                .collect(),
        ),
        // The following types are sent as an array and require specific parsing functions
        7 => FalkorValue::Edge(FalkorParsable::from_falkor_value(val, graph_schema)?),
        8 => FalkorValue::Node(FalkorParsable::from_falkor_value(val, graph_schema)?),
        9 => FalkorValue::Path(FalkorParsable::from_falkor_value(val, graph_schema)?),
        10 => FalkorValue::Map(FalkorParsable::from_falkor_value(val, graph_schema)?),
        11 => FalkorValue::Point(Point::parse(val)?),
        _ => Err(FalkorDBError::ParsingUnknownType)?,
    };

    Ok(res)
}

pub(crate) fn parse_vec<T: TryFrom<FalkorValue, Error = FalkorDBError>>(
    value: FalkorValue
) -> Result<Vec<T>, FalkorDBError> {
    Ok(value
        .into_vec()?
        .into_iter()
        .flat_map(TryFrom::try_from)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_schema::tests::open_readonly_graph_with_modified_schema;

    #[test]
    fn test_parse_edge() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            7,
            FalkorValue::Array(vec![
                FalkorValue::I64(100), // edge id
                FalkorValue::I64(0),   // edge type
                FalkorValue::I64(51),  // src node
                FalkorValue::I64(52),  // dst node
                FalkorValue::Array(vec![
                    FalkorValue::Array(vec![
                        FalkorValue::I64(0),
                        FalkorValue::I64(3),
                        FalkorValue::I64(20),
                    ]),
                    FalkorValue::Array(vec![
                        FalkorValue::I64(1),
                        FalkorValue::I64(4),
                        FalkorValue::Bool(false),
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
            Some(&FalkorValue::Bool(false))
        );
    }

    #[test]
    fn test_parse_node() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            8,
            FalkorValue::Array(vec![
                FalkorValue::I64(51),                                               // node id
                FalkorValue::Array(vec![FalkorValue::I64(0), FalkorValue::I64(1)]), // node type
                FalkorValue::Array(vec![
                    FalkorValue::Array(vec![
                        FalkorValue::I64(0),
                        FalkorValue::I64(3),
                        FalkorValue::I64(15),
                    ]),
                    FalkorValue::Array(vec![
                        FalkorValue::I64(2),
                        FalkorValue::I64(2),
                        FalkorValue::String("the something".to_string()),
                    ]),
                    FalkorValue::Array(vec![
                        FalkorValue::I64(3),
                        FalkorValue::I64(5),
                        FalkorValue::F64(105.5),
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
            Some(&FalkorValue::F64(105.5))
        );
    }

    #[test]
    fn test_parse_path() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            9,
            FalkorValue::Array(vec![
                FalkorValue::Array(vec![
                    FalkorValue::Array(vec![
                        FalkorValue::I64(51),
                        FalkorValue::Array(vec![FalkorValue::I64(0)]),
                        FalkorValue::Array(vec![]),
                    ]),
                    FalkorValue::Array(vec![
                        FalkorValue::I64(52),
                        FalkorValue::Array(vec![FalkorValue::I64(0)]),
                        FalkorValue::Array(vec![]),
                    ]),
                    FalkorValue::Array(vec![
                        FalkorValue::I64(53),
                        FalkorValue::Array(vec![FalkorValue::I64(0)]),
                        FalkorValue::Array(vec![]),
                    ]),
                ]),
                FalkorValue::Array(vec![
                    FalkorValue::Array(vec![
                        FalkorValue::I64(100),
                        FalkorValue::I64(0),
                        FalkorValue::I64(51),
                        FalkorValue::I64(52),
                        FalkorValue::Array(vec![]),
                    ]),
                    FalkorValue::Array(vec![
                        FalkorValue::I64(101),
                        FalkorValue::I64(1),
                        FalkorValue::I64(52),
                        FalkorValue::I64(53),
                        FalkorValue::Array(vec![]),
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
            FalkorValue::Array(vec![
                FalkorValue::String("key0".to_string()),
                FalkorValue::Array(vec![
                    FalkorValue::I64(2),
                    FalkorValue::String("val0".to_string()),
                ]),
                FalkorValue::String("key1".to_string()),
                FalkorValue::Array(vec![FalkorValue::I64(3), FalkorValue::I64(1)]),
                FalkorValue::String("key2".to_string()),
                FalkorValue::Array(vec![FalkorValue::I64(4), FalkorValue::Bool(true)]),
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
        assert_eq!(map.get("key2"), Some(&FalkorValue::Bool(true)));
    }

    #[test]
    fn test_parse_point() {
        let mut graph = open_readonly_graph_with_modified_schema();

        let res = parse_type(
            11,
            FalkorValue::Array(vec![FalkorValue::F64(102.0), FalkorValue::F64(15.2)]),
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
}
