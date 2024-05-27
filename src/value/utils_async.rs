/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use super::utils::type_val_from_value;
use crate::{
    AsyncGraphSchema, Edge, FalkorAsyncConnection, FalkorAsyncParseable, FalkorDBError,
    FalkorValue, Node, Path, Point, SchemaType,
};
use anyhow::Result;
use async_recursion::async_recursion;
use std::collections::{HashMap, HashSet};

pub(crate) async fn parse_labels_async(
    raw_ids: Vec<FalkorValue>,
    graph_schema: &AsyncGraphSchema,
    conn: FalkorAsyncConnection,
    schema_type: SchemaType,
) -> Result<Vec<String>> {
    let mut ids_hashset = HashSet::with_capacity(raw_ids.len());
    for label in raw_ids.iter() {
        ids_hashset.insert(label.to_i64().ok_or(FalkorDBError::ParsingI64)?);
    }

    match match graph_schema.verify_id_set(&ids_hashset, schema_type).await {
        None => {
            graph_schema
                .refresh(schema_type, conn, Some(&ids_hashset))
                .await?
        }
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

#[async_recursion]
pub(crate) async fn parse_type_async(
    type_marker: i64,
    val: FalkorValue,
    graph_schema: &AsyncGraphSchema,
    conn: FalkorAsyncConnection,
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
                parsed_vec
                    .push(parse_type_async(type_marker, val, graph_schema, conn.clone()).await?);
            }
            parsed_vec
        }),
        // The following types are sent as an array and require specific parsing functions
        7 => FalkorValue::FEdge(Edge::from_falkor_value_async(val, graph_schema, conn).await?),
        8 => FalkorValue::FNode(Node::from_falkor_value_async(val, graph_schema, conn).await?),
        9 => FalkorValue::FPath(Path::from_falkor_value_async(val, graph_schema, conn).await?),
        10 => FalkorValue::FMap(HashMap::from_falkor_value_async(val, graph_schema, conn).await?),
        11 => FalkorValue::FPoint(Point::parse(val)?),
        _ => Err(FalkorDBError::ParsingUnknownType)?,
    };

    Ok(res)
}
