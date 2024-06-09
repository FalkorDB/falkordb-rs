/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    parser::utils::{parse_header, string_vec_from_val},
    value::utils::parse_type,
    FalkorDBError, FalkorParsable, FalkorResult, FalkorValue, GraphSchema, SyncGraph,
};
use std::collections::VecDeque;

pub(crate) mod constraint;
pub(crate) mod execution_plan;
pub(crate) mod index;
pub(crate) mod slowlog_entry;

/// A wrapper around the returned raw data, allowing parsing on demand of each result
/// This implements Iterator, so can simply be collect()'ed into any desired container
pub struct LazyResultSet<'a> {
    data: VecDeque<FalkorValue>,
    graph: &'a mut SyncGraph,
}

impl<'a> LazyResultSet<'a> {
    pub(crate) fn new(
        data: Vec<FalkorValue>,
        graph: &'a mut SyncGraph,
    ) -> Self {
        Self {
            data: data.into(),
            graph,
        }
    }

    /// Returns the remaining rows in the result set.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns whether this result set is empty or depleted
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<'a> Iterator for LazyResultSet<'a> {
    type Item = Vec<FalkorValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.pop_front().map(|current_result| {
            parse_type(6, current_result, &mut self.graph.graph_schema)
                .and_then(|parsed_result| parsed_result.into_vec())
                .unwrap_or(vec![FalkorValue::Unparseable])
        })
    }
}

/// A response struct which also contains the returned header and stats data
#[derive(Clone, Debug, Default)]
pub struct FalkorResponse<T> {
    /// Header for the result data, usually contains the scalar aliases for the columns
    pub header: Vec<String>,
    /// The actual data returned from the database
    pub data: T,
    /// Various statistics regarding the request, such as execution time and number of successful operations
    pub stats: Vec<String>,
}

impl<T> FalkorResponse<T> {
    /// Creates a [`FalkorResponse`] from the specified data, and raw stats, where raw headers are optional
    ///
    /// # Arguments
    /// * `headers`: a [`FalkorValue`] that is expected to be of variant [`FalkorValue::Array`], where each element is expected to be of variant [`FalkorValue::String`]
    /// * `data`: The actual data
    /// * `stats`: a [`FalkorValue`] that is expected to be of variant [`FalkorValue::Array`], where each element is expected to be of variant [`FalkorValue::String`]
    pub fn from_response(
        headers: Option<FalkorValue>,
        data: T,
        stats: FalkorValue,
    ) -> FalkorResult<Self> {
        Ok(Self {
            header: match headers {
                Some(headers) => parse_header(headers)?,
                None => vec![],
            },
            data,
            stats: string_vec_from_val(stats)?,
        })
    }
}

impl<T: FalkorParsable> FalkorParsable for FalkorResponse<Vec<T>> {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &mut GraphSchema,
    ) -> FalkorResult<Self> {
        let [header, indices, stats]: [FalkorValue; 3] =
            value.into_vec()?.try_into().map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 3 elements in query response".to_string(),
                )
            })?;

        FalkorResponse::from_response(
            Some(header),
            indices
                .into_vec()?
                .into_iter()
                .flat_map(|index| FalkorParsable::from_falkor_value(index, graph_schema))
                .collect(),
            stats,
        )
    }
}
