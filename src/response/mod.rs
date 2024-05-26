/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use constraint::Constraint;
use execution_plan::ExecutionPlan;
use index::FalkorIndex;
use query_result::QueryResult;
use slowlog_entry::SlowlogEntry;

pub(crate) mod constraint;
pub(crate) mod execution_plan;
pub(crate) mod index;
pub(crate) mod query_result;
pub(crate) mod slowlog_entry;

/// A naive wrapper for the various possible responses from queries
/// Its main usecase is for creating things like [`JoinSet`](tokio::task::JoinSet)s, which require all the tasks to return the same type
#[derive(Debug, Clone, PartialEq)]
pub enum ResponseVariant {
    ExecutionPlan(ExecutionPlan),
    QueryResult(QueryResult),
    SlowlogEntry(SlowlogEntry),
    Constraints(Vec<Constraint>),
    Indices(Vec<FalkorIndex>),
}
