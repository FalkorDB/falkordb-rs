/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection,
    value::utils::{parse_type, type_val_from_value},
    FalkorDBError, FalkorParsable, FalkorValue, SyncGraphSchema,
};
use anyhow::Result;
use std::collections::HashMap;

#[cfg(feature = "tokio")]
use crate::{
    connection::asynchronous::BorrowedAsyncConnection, value::utils_async::parse_type_async,
    AsyncGraphSchema, FalkorAsyncParseable,
};

/// A struct returned by the various queries, containing the result set, header, and stats
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryResult {
    /// The statistics for this query, such as how long it took
    pub stats: Vec<String>,
    pub header: Vec<String>,
    pub result_set: Vec<HashMap<String, FalkorValue>>,
}

impl QueryResult {
    /// Returns a slice of the statistics for this query, such as how long it took
    pub fn stats(&self) -> &[String] {
        self.stats.as_slice()
    }

    /// Returns a slice of header for this query result, usually contains the verbose names of each column of the result set
    pub fn header(&self) -> &[String] {
        self.header.as_slice()
    }

    /// Returns the result set as a slice which can be iterated without taking ownership
    pub fn result_set(&self) -> &[HashMap<String, FalkorValue>] {
        self.result_set.as_slice()
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

pub(crate) fn parse_result_set(
    data_vec: Vec<FalkorValue>,
    graph_schema: &mut SyncGraphSchema,
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

impl FalkorParsable for QueryResult {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &mut SyncGraphSchema,
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
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        let header_keys = query_parse_header(header)?;
        let stats_strings = query_parse_stats(stats)?;

        let data_vec = data.into_vec()?;
        let data_len = data_vec.len();

        let result_set = parse_result_set(data_vec, graph_schema, conn, &header_keys)?;

        if result_set.len() != data_len {
            Err(FalkorDBError::ParsingError)?;
        }

        Ok(Self {
            stats: stats_strings,
            header: header_keys,
            result_set,
        })
    }
}

#[cfg(feature = "tokio")]
async fn parse_result_set_async(
    data_vec: Vec<FalkorValue>,
    graph_schema: &AsyncGraphSchema,
    conn: &mut BorrowedAsyncConnection,
    header_keys: &[String],
) -> Result<Vec<HashMap<String, FalkorValue>>> {
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

#[cfg(feature = "tokio")]
impl FalkorAsyncParseable for QueryResult {
    async fn from_falkor_value_async(
        value: FalkorValue,
        graph_schema: &AsyncGraphSchema,
        conn: &mut BorrowedAsyncConnection,
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
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        let header_keys = query_parse_header(header)?;
        let stats_strings = query_parse_stats(stats)?;

        let data_vec = data.into_vec()?;
        let data_len = data_vec.len();

        let result_set = parse_result_set_async(data_vec, graph_schema, conn, &header_keys).await?;

        if result_set.len() != data_len {
            Err(FalkorDBError::ParsingError)?;
        }

        Ok(Self {
            stats: stats_strings,
            header: header_keys,
            result_set,
        })
    }
}
