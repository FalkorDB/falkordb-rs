/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::connection::blocking::BorrowedSyncConnection;
use crate::error::FalkorDBError;
use crate::graph::schema::GraphSchema;
use crate::value::{parse_type, type_val_from_value, FalkorValue};
use anyhow::Result;
use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct QueryResult {
    pub(crate) stats: Vec<String>,
    pub(crate) header: Vec<String>,
    pub(crate) result_set: Vec<HashMap<String, FalkorValue>>,
}

fn parse_result_set(
    data_vec: Vec<FalkorValue>,
    graph_schema: &GraphSchema,
    conn: &mut BorrowedSyncConnection,
    header_keys: &[String],
) -> Result<Vec<HashMap<String, FalkorValue>>> {
    let mut parsed_result_set = Vec::with_capacity(data_vec.len());
    for column in data_vec {
        let column_vec = column.into_vec()?;

        let mut parsed_column = Vec::with_capacity(column_vec.len());
        for column_item in column_vec {
            let (type_marker, val) = type_val_from_value(column_item)?;
            parsed_column.push(parse_type(type_marker, val, graph_schema, conn)?);
        }

        parsed_result_set.push(header_keys.iter().cloned().zip(parsed_column).collect())
    }

    Ok(parsed_result_set)
}

impl QueryResult {
    pub fn stats(&self) -> &[String] {
        self.stats.as_slice()
    }

    pub fn header(&self) -> &[String] {
        self.header.as_slice()
    }

    pub fn result_set(&self) -> &[HashMap<String, FalkorValue>] {
        self.result_set.as_slice()
    }

    pub fn take(self) -> (Vec<String>, Vec<String>, Vec<HashMap<String, FalkorValue>>) {
        (self.stats, self.header, self.result_set)
    }

    pub(crate) fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &GraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> Result<Self> {
        let value_vec = value.into_vec()?;

        // Empty result
        if value_vec.is_empty() {
            return Ok(QueryResult::default());
        }

        // Result only contains stats
        if value_vec.len() == 1 {
            let first_element = value_vec.into_iter().nth(0).unwrap();
            return Ok(QueryResult {
                stats: query_parse_stats(first_element)?,
                ..Default::default()
            });
        }

        // Invalid response
        if value_vec.len() == 2 {
            Err(FalkorDBError::InvalidDataReceived)?;
        }

        // Full result
        let [header, data, stats]: [FalkorValue; 3] = value_vec
            .try_into()
            .map_err(|_| FalkorDBError::ParsingError)?;

        let header_keys = query_parse_header(header)?;
        let stats_strings = query_parse_stats(stats)?;

        let data_vec = data.into_vec()?;
        let data_len = data_vec.len();

        let result_set = parse_result_set(data_vec, graph_schema, conn, &header_keys)?;

        if result_set.len() != data_len {
            Err(FalkorDBError::ParsingError)?;
        }

        Ok(QueryResult {
            stats: stats_strings,
            header: header_keys,
            result_set,
        })
    }
}

fn query_parse_header(header: FalkorValue) -> Result<Vec<String>> {
    let header_vec = header.into_vec()?;

    let mut keys = Vec::with_capacity(header_vec.len());
    for item in header_vec {
        let item_vec = item.into_vec()?;
        let key = if item_vec.len() == 2 {
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
        .into_string()?;
        keys.push(key);
    }

    Ok(keys)
}

fn query_parse_stats(stats: FalkorValue) -> Result<Vec<String>> {
    let stats_vec = stats.into_vec()?;

    let mut stats_strings = Vec::with_capacity(stats_vec.len());
    for element in stats_vec {
        stats_strings.push(element.into_string()?);
    }

    Ok(stats_strings)
}
