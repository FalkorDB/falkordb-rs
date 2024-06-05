/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    parser::utils::{parse_header, string_vec_from_val},
    FalkorResult, FalkorValue,
};

pub(crate) mod constraint;
pub(crate) mod execution_plan;
pub(crate) mod index;
pub(crate) mod slowlog_entry;

/// A [`Vec`], representing a table of other [`Vec`]s, representing columns, containing [`FalkorValue`]s
pub type ResultSet = Vec<Vec<FalkorValue>>;

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

    /// Creates a [`FalkorResponse`] from the specified data, and raw stats, where parsed headers are given and parsing them is not needed
    ///
    /// # Arguments
    /// * `data`: The actual data
    /// * `headers`: The already parsed headers, this is usually used to pass an empty header vec, or if we already parsed the headers for use in some other context and don't want to repeat
    /// * `stats`: a [`FalkorValue`] that is expected to be of variant [`FalkorValue::Array`], where each element is expected to be of variant [`FalkorValue::String`]
    pub fn from_response_with_headers(
        data: T,
        header: Vec<String>,
        stats: FalkorValue,
    ) -> FalkorResult<Self> {
        Ok(Self {
            header,
            data,
            stats: string_vec_from_val(stats)?,
        })
    }
}
