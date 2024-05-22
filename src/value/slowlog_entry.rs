/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::error::FalkorDBError;
use crate::value::FalkorValue;
use anyhow::Result;

#[derive(Clone, Debug)]
pub struct SlowlogEntry {
    pub timestamp: i64,
    pub command: String,
    pub arguments: String,
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
