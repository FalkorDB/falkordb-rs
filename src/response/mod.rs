/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use execution_plan::ExecutionPlan;
use query_result::QueryResult;
use slowlog_entry::SlowlogEntry;

pub(crate) mod execution_plan;
pub(crate) mod query_result;
pub(crate) mod slowlog_entry;

/// A naive wrapper for the various possible responses from queries
/// Its main usecase is for creating things like [tokio::task::JoinSet]s, which require all the tasks to return the same type
#[derive(Debug, Clone)]
pub enum ResponseVariant {
    ExecutionPlan(ExecutionPlan),
    QueryResult(QueryResult),
    SlowlogEntry(SlowlogEntry),
}
