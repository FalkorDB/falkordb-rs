/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::value::FalkorValue;
use anyhow::Result;

#[derive(Clone, Debug)]
pub struct SlowlogEntry {
    pub timestamp: u64,
    pub command: String,
    pub arguments: String,
    pub time_taken: u64,
}

impl SlowlogEntry {
    pub fn from_value_array(values: [FalkorValue; 4]) -> Result<Self> {
        let [timestamp, command, arguments, time_taken] = values;
        Ok(Self {
            timestamp: timestamp.try_into()?,
            command: command.into_string()?,
            arguments: arguments.into_string()?,
            time_taken: time_taken.try_into()?,
        })
    }
}
