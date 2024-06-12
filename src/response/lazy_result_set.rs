/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{parser::parse_raw_redis_value, FalkorValue, GraphSchema};
use std::collections::VecDeque;

/// A wrapper around the returned raw data, allowing parsing on demand of each result
/// This implements Iterator, so can simply be collect()'ed into any desired container
pub struct LazyResultSet<'a> {
    data: VecDeque<redis::Value>,
    graph_schema: &'a mut GraphSchema,
}

impl<'a> LazyResultSet<'a> {
    pub(crate) fn new(
        data: Vec<redis::Value>,
        graph_schema: &'a mut GraphSchema,
    ) -> Self {
        Self {
            data: data.into(),
            graph_schema,
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

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Next Result", skip_all)
    )]
    fn next(&mut self) -> Option<Self::Item> {
        self.data.pop_front().map(|current_result| {
            parse_raw_redis_value(current_result, self.graph_schema)
                .and_then(FalkorValue::into_vec)
                .unwrap_or(vec![FalkorValue::Unparseable])
        })
    }
}
