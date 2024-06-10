/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::client::ProvidesSyncConnections;
use crate::{value::utils::parse_type, FalkorValue, GraphSchema};
use std::collections::VecDeque;

/// A wrapper around the returned raw data, allowing parsing on demand of each result
/// This implements Iterator, so can simply be collect()'ed into any desired container
pub struct LazyResultSet<'a, C: ProvidesSyncConnections> {
    data: VecDeque<FalkorValue>,
    graph_schema: &'a mut GraphSchema<C>,
}

impl<'a, C: ProvidesSyncConnections> LazyResultSet<'a, C> {
    pub(crate) fn new(
        data: Vec<FalkorValue>,
        graph_schema: &'a mut GraphSchema<C>,
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

impl<'a, C: ProvidesSyncConnections> Iterator for LazyResultSet<'a, C> {
    type Item = Vec<FalkorValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.pop_front().map(|current_result| {
            parse_type(6, current_result, self.graph_schema)
                .and_then(|parsed_result| parsed_result.into_vec())
                .unwrap_or(vec![FalkorValue::Unparseable])
        })
    }
}
