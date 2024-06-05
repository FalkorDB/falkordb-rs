/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{value::utils::parse_type, FalkorDBError, FalkorValue, GraphSchema, ResultSet};

pub(crate) fn string_vec_from_val(value: FalkorValue) -> Result<Vec<String>, FalkorDBError> {
    value.into_vec().map(|value_as_vec| {
        value_as_vec
            .into_iter()
            .flat_map(FalkorValue::into_string)
            .collect()
    })
}

pub(crate) fn parse_header(header: FalkorValue) -> Result<Vec<String>, FalkorDBError> {
    header.into_vec().map(|header_as_vec| {
        header_as_vec
            .into_iter()
            .flat_map(|item| {
                let item_vec = item.into_vec()?;
                if item_vec.len() == 2 {
                    let [_, key]: [FalkorValue; 2] = item_vec.try_into().map_err(|_| {
                        FalkorDBError::ParsingHeader(
                            "Header was not in (type: header) form".to_string(),
                        )
                    })?;
                    key
                } else {
                    item_vec
                        .into_iter()
                        .next()
                        .ok_or(FalkorDBError::ParsingHeader(
                            "Expected at least one item in header vector".to_string(),
                        ))?
                }
                .into_string()
            })
            .collect()
    })
}

pub(crate) fn parse_result_set(
    data: FalkorValue,
    graph_schema: &mut GraphSchema,
) -> Result<ResultSet, FalkorDBError> {
    let data_vec = data.into_vec()?;

    let mut out_vec = Vec::with_capacity(data_vec.len());
    for column in data_vec {
        out_vec.push(parse_type(6, column, graph_schema)?.into_vec()?);
    }

    Ok(out_vec)
}
