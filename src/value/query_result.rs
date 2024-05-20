/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::value::FalkorValue;
use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct QueryResult {
    pub(crate) stats: Vec<String>,
    pub(crate) header: Vec<String>,
    pub(crate) result_set: Vec<HashMap<String, FalkorValue>>,
}

impl QueryResult {
    pub fn stats(&self) -> &[String] {
        self.stats.as_slice()
    }

    pub fn header(&self) -> &[String] {
        self.header.as_slice()
    }

    pub fn result_set(&self) -> &[HashMap<String, FalkorValue>] {
        self.result_set.as_slice()
    }

    pub fn take(self) -> (Vec<String>, Vec<String>, Vec<HashMap<String, FalkorValue>>) {
        (self.stats, self.header, self.result_set)
    }
}
