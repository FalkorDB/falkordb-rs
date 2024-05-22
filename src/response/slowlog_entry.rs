/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorValue};
use anyhow::Result;

/// A slowlog entry, representing one of the N slowest queries in the current log
#[derive(Clone, Debug)]
pub struct SlowlogEntry {
    /// At which time was this query received
    pub timestamp: i64,
    /// Which command was used to perform this query
    pub command: String,
    /// The query itself
    pub arguments: String,
    /// How long did performing this query take.
    pub time_taken: i64,
}

impl SlowlogEntry {
    pub fn from_value_array(values: [FalkorValue; 4]) -> Result<Self> {
        let [timestamp, command, arguments, time_taken] = values;
        Ok(Self {
            timestamp: timestamp.to_i64().ok_or(FalkorDBError::ParsingI64)?,
            command: command.into_string()?,
            arguments: arguments.into_string()?,
            time_taken: time_taken.to_i64().ok_or(FalkorDBError::ParsingI64)?,
        })
    }
}
