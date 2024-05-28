/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorValue};

/// A slowlog entry, representing one of the N slowest queries in the current log
#[derive(Clone, Debug, PartialEq)]
pub struct SlowlogEntry {
    /// At which time was this query received
    pub timestamp: i64,
    /// Which command was used to perform this query
    pub command: String,
    /// The query itself
    pub arguments: String,
    /// How long did performing this query take.
    pub time_taken: f64,
}

impl TryFrom<FalkorValue> for SlowlogEntry {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        let [timestamp, command, arguments, time_taken] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        Ok(Self {
            timestamp: timestamp
                .into_string()?
                .parse()
                .map_err(|_| FalkorDBError::ParsingI64)?,
            command: command.into_string()?,
            arguments: arguments.into_string()?,
            time_taken: time_taken
                .into_string()?
                .parse()
                .map_err(|_| FalkorDBError::ParsingF64)?,
        })
    }
}
