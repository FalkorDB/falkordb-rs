/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection,
    value::utils::{parse_type, type_val_from_value},
    FalkorDBError, FalkorValue, SyncGraphSchema,
};
use anyhow::Result;
use std::collections::HashMap;

#[cfg(feature = "tokio")]
use crate::{
    connection::asynchronous::BorrowedAsyncConnection, value::utils_async::parse_type_async,
    AsyncGraphSchema,
};

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
    graph_schema: &mut SyncGraphSchema,
    conn: &mut BorrowedSyncConnection,
    header_keys: &[String],
) -> Result<Vec<HashMap<String, FalkorValue>>> {
    Ok(data
        .into_vec()?
        .into_iter()
        .flat_map(|column| {
            anyhow::Result::<_, FalkorDBError>::Ok(
                header_keys
                    .iter()
                    .cloned()
                    .zip(column.into_vec()?.into_iter().flat_map(|column_subitem| {
                        type_val_from_value(column_subitem).and_then(|(type_marker, val)| {
                            parse_type(type_marker, val, graph_schema, conn)
                        })
                    }))
                    .collect(),
            )
        })
        .collect())
}

#[cfg(feature = "tokio")]
pub(crate) async fn parse_result_set_async(
    data: FalkorValue,
    graph_schema: &AsyncGraphSchema,
    conn: &mut BorrowedAsyncConnection,
    header_keys: &[String],
) -> Result<Vec<HashMap<String, FalkorValue>>> {
    let data_vec = data.into_vec()?;
    let mut parsed_result_set = Vec::with_capacity(data_vec.len());
    for column in data_vec {
        let column_vec = column.into_vec()?;

        let mut parsed_column = Vec::with_capacity(column_vec.len());
        for column_item in column_vec {
            let (type_marker, val) = type_val_from_value(column_item)?;
            parsed_column.push(parse_type_async(type_marker, val, graph_schema, conn).await?);
        }

        parsed_result_set.push(header_keys.iter().cloned().zip(parsed_column).collect())
    }

    Ok(parsed_result_set)
}
