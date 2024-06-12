/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{redis_ext::redis_value_as_string, FalkorDBError, FalkorResult};

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

impl SlowlogEntry {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Slowlog Entry", skip_all, level = "info")
    )]
    pub(crate) fn parse(value: redis::Value) -> FalkorResult<Self> {
        let [timestamp, command, arguments, time_taken]: [redis::Value; 4] = value
            .into_sequence()
            .map_err(|_| FalkorDBError::ParsingArray)
            .and_then(|as_vec| {
                as_vec.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "Expected exactly 4 elements of slowlog entry",
                    )
                })
            })?;

        Ok(Self {
            timestamp: redis_value_as_string(timestamp)
                .ok()
                .and_then(|timestamp| timestamp.parse().ok())
                .ok_or(FalkorDBError::ParsingI64)?,
            command: redis_value_as_string(command)?,
            arguments: redis_value_as_string(arguments)?,
            time_taken: redis_value_as_string(time_taken)
                .ok()
                .and_then(|time_taken| time_taken.parse().ok())
                .ok_or(FalkorDBError::ParsingF64)?,
        })
    }
}
