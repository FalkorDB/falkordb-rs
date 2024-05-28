/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    parser::utils::{parse_header, string_vec_from_val},
    FalkorDBError, FalkorValue,
};
use constraint::Constraint;
use execution_plan::ExecutionPlan;
use index::FalkorIndex;
use slowlog_entry::SlowlogEntry;
use std::collections::HashMap;

pub(crate) mod constraint;
pub(crate) mod execution_plan;
pub(crate) mod index;
pub(crate) mod slowlog_entry;

pub type ResultSet = Vec<HashMap<String, FalkorValue>>;

/// A naive wrapper for the various possible responses from queries
/// Its main usecase is for creating things like JoinSets, which require all the tasks to return the same type
#[derive(Debug, Clone, PartialEq)]
pub enum ResponseEnum {
    ExecutionPlan(ExecutionPlan),
    QueryResult(ResultSet),
    SlowlogEntry(SlowlogEntry),
    Constraints(Vec<Constraint>),
    Indices(Vec<FalkorIndex>),
}

#[derive(Clone, Debug, Default)]
pub struct FalkorResponse<T> {
    pub header: Vec<String>,
    pub data: T,
    pub stats: Vec<String>,
}

impl<T> FalkorResponse<T> {
    pub fn from_response(
        headers: Option<FalkorValue>,
        data: T,
        stats: FalkorValue,
    ) -> Result<Self, FalkorDBError> {
        Ok(Self {
            header: match headers {
                Some(headers) => parse_header(headers)?,
                None => vec![],
            },
            data,
            stats: string_vec_from_val(stats)?,
        })
    }

    pub fn from_response_with_headers(
        data: T,
        header: Vec<String>,
        stats: FalkorValue,
    ) -> Result<Self, FalkorDBError> {
        Ok(Self {
            header,
            data,
            stats: string_vec_from_val(stats)?,
        })
    }
}
