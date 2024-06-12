/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    parser::{parse_header, string_vec_from_val},
    FalkorResult,
};

pub(crate) mod constraint;
pub(crate) mod execution_plan;
pub(crate) mod index;
pub(crate) mod lazy_result_set;
pub(crate) mod slowlog_entry;

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
    /// * `headers`: a [`redis::Value`] that is expected to be of variant [`redis::Value::Bulk`], where each element is expected to be of variant [`redis::Value::Data`] or [`redis::Value::Status`]
    /// * `data`: The actual data
    /// * `stats`: a [`redis::Value`] that is expected to be of variant [`redis::Value::Bulk`], where each element is expected to be of variant [`redis::Value::Data`] or [`redis::Value::Status`]
    pub fn from_response(
        headers: Option<redis::Value>,
        data: T,
        stats: redis::Value,
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
