/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::connection::blocking::BorrowedSyncConnection;
use crate::graph::schema::GraphSchema;
use crate::value::graph_entities::{Edge, Node};
use crate::value::map::parse_map;
use crate::value::path::Path;
use crate::value::point::Point;
use crate::value::FalkorValue;
use crate::FalkorDBError;
use anyhow::Result;

pub(crate) fn type_val_from_value(value: FalkorValue) -> Result<(i64, FalkorValue)> {
    let [type_marker, val]: [FalkorValue; 2] = value
        .into_vec()?
        .try_into()
        .map_err(|_| FalkorDBError::ParsingError)?;
    let type_marker = type_marker.to_i64().ok_or(FalkorDBError::ParsingI64)?;

    Ok((type_marker, val))
}

pub(crate) fn parse_type(
    type_marker: i64,
    val: FalkorValue,
    graph_schema: &GraphSchema,
    conn: &mut BorrowedSyncConnection,
) -> Result<FalkorValue> {
    let res = match type_marker {
        1 => FalkorValue::None,
        2 => FalkorValue::FString(val.into_string()?),
        3 => FalkorValue::Int64(val.to_i64().ok_or(FalkorDBError::ParsingI64)?),
        4 => FalkorValue::FBool(val.to_bool().ok_or(FalkorDBError::ParsingBool)?),
        5 => FalkorValue::F64(val.to_f64().ok_or(FalkorDBError::ParsingF64)?),
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
        7 => FalkorValue::FEdge(Edge::parse(val, graph_schema, conn)?),
        8 => FalkorValue::FNode(Node::parse(val, graph_schema, conn)?),
        9 => FalkorValue::FPath(Path::parse(val)?),
        10 => FalkorValue::FMap(parse_map(val, graph_schema, conn)?),
        11 => FalkorValue::FPoint(Point::parse(val)?),
        _ => Err(FalkorDBError::ParsingUnknownType)?,
    };

    Ok(res)
}
