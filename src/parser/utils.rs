/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::connection::blocking::BorrowedSyncConnection;
use crate::{value::utils::parse_type, FalkorDBError, FalkorValue, GraphSchema};
use anyhow::Result;
use std::collections::HashMap;

pub fn string_vec_from_val(value: FalkorValue) -> Result<Vec<String>, FalkorDBError> {
    value.into_vec().map(|value_as_vec| {
        value_as_vec
            .into_iter()
            .flat_map(FalkorValue::into_string)
            .collect()
    })
}

pub fn parse_header(header: FalkorValue) -> Result<Vec<String>, FalkorDBError> {
    header.into_vec().map(|header_as_vec| {
        header_as_vec
            .into_iter()
            .flat_map(|item| {
                let item_vec = item.into_vec()?;
                if item_vec.len() == 2 {
                    let [_, key]: [FalkorValue; 2] = item_vec
                        .try_into()
                        .map_err(|_| FalkorDBError::ParsingHeader)?;
                    key
                } else {
                    item_vec
                        .into_iter()
                        .next()
                        .ok_or(FalkorDBError::ParsingHeader)?
                }
                .into_string()
            })
            .collect()
    })
}

pub(crate) fn parse_result_set(
    data: FalkorValue,
    graph_schema: &mut GraphSchema,
    conn: &mut BorrowedSyncConnection,
    header_keys: &[String],
) -> Result<Vec<HashMap<String, FalkorValue>>> {
    Ok(data
        .into_vec()?
        .into_iter()
        .flat_map(|column| {
            Result::<_, FalkorDBError>::Ok(
                header_keys
                    .iter()
                    .cloned()
                    .zip(parse_type(6, column, graph_schema, conn))
                    .collect(),
            )
        })
        .collect())
}
