/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::blocking::create_empty_inner_client,
    parser::utils::{parse_header, string_vec_from_val},
    value::utils::parse_type,
    FalkorDBError, FalkorParsable, FalkorResult, FalkorValue, GraphSchema,
};
use parking_lot::Mutex;
use std::{collections::VecDeque, ops::DerefMut, sync::Arc};

pub(crate) mod constraint;
pub(crate) mod execution_plan;
pub(crate) mod index;
pub(crate) mod slowlog_entry;

/// A wrapper around the returned raw data, allowing parsing on demand of each result
/// This implements Iterator, so can simply be collect()'ed into any desired container
#[derive(Debug)]
pub struct LazyResultSet {
    data: VecDeque<FalkorValue>,
    graph_schema: Arc<Mutex<GraphSchema>>,
}

impl LazyResultSet {
    pub(crate) fn new(
        data: Vec<FalkorValue>,
        graph_schema: Arc<Mutex<GraphSchema>>,
    ) -> Self {
        Self {
            data: data.into(),
            graph_schema,
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            data: Default::default(),
            graph_schema: Arc::new(Mutex::new(GraphSchema::new(
                "",
                create_empty_inner_client(),
            ))),
        }
    }

    /// Consumes the entire iterator and returns a specified collection of type T.
    /// Requires that type T implements [`FromIterator<FalkorValue>`] and [`Default`] (which most containers do).
    /// This has a performance boost, but requires parsing the entire iterator, so should be avoided unless that is the specific purpose.
    pub fn collect<T: FromIterator<FalkorValue> + Default>(self) -> T {
        if self.data.is_empty() {
            return T::default();
        }
        let mut graph_schema = self.graph_schema.lock();
        self.data
            .into_iter()
            .filter_map(|current_result| {
                parse_type(6, current_result, graph_schema.deref_mut()).ok()
            })
            .collect::<T>()
    }
}

impl Iterator for LazyResultSet {
    type Item = Vec<FalkorValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.pop_front().and_then(|current_result| {
            parse_type(6, current_result, self.graph_schema.lock().deref_mut())
                .and_then(|parsed_result| parsed_result.into_vec())
                .ok()
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
