/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    parser::{redis_value_as_double, redis_value_as_string, redis_value_as_vec},
    FalkorDBError, FalkorResult,
};

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
        // FalkorDB sends a fifth element (nil) after these four; later ones are ignored.
        let mut fields = redis_value_as_vec(value)?.into_iter();
        let mut next = || {
            fields
                .next()
                .ok_or(FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected at least 4 elements of slowlog entry",
                ))
        };
        let (timestamp, command, arguments, time_taken) = (next()?, next()?, next()?, next()?);

        Ok(Self {
            timestamp: parse_timestamp(timestamp)?,
            command: redis_value_as_string(command)?,
            arguments: redis_value_as_string(arguments)?,
            time_taken: redis_value_as_double(time_taken)?,
        })
    }
}

/// A slowlog timestamp in seconds: a numeric string over RESP2, a whole double over RESP3.
fn parse_timestamp(value: redis::Value) -> FalkorResult<i64> {
    match value {
        redis::Value::Int(seconds) => Ok(seconds),
        redis::Value::Double(seconds) if seconds.fract() == 0.0 => Ok(seconds as i64),
        value => redis_value_as_string(value)
            .ok()
            .and_then(|seconds| seconds.parse().ok())
            .ok_or(FalkorDBError::ParsingI64),
    }
}

/// Parses a `GRAPH.SLOWLOG` reply (a list of entries) into [`SlowlogEntry`]s, propagating a parse
/// error for any malformed entry instead of silently dropping it. Shared by the sync and async
/// `slowlog` methods.
pub(crate) fn parse_slowlog(value: redis::Value) -> FalkorResult<Vec<SlowlogEntry>> {
    redis_value_as_vec(value)?
        .into_iter()
        .map(SlowlogEntry::parse)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slowlog_entry_clone() {
        let entry = SlowlogEntry {
            timestamp: 1234567890,
            command: "GRAPH.QUERY".to_string(),
            arguments: "MATCH (n) RETURN n".to_string(),
            time_taken: 123.456,
        };

        let entry_clone = entry.clone();
        assert_eq!(entry, entry_clone);
        assert_eq!(entry.timestamp, entry_clone.timestamp);
        assert_eq!(entry.command, entry_clone.command);
        assert_eq!(entry.arguments, entry_clone.arguments);
        assert_eq!(entry.time_taken, entry_clone.time_taken);
    }

    #[test]
    fn test_slowlog_entry_debug() {
        let entry = SlowlogEntry {
            timestamp: 1234567890,
            command: "GRAPH.QUERY".to_string(),
            arguments: "MATCH (n) RETURN n".to_string(),
            time_taken: 123.456,
        };

        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("1234567890"));
        assert!(debug_str.contains("GRAPH.QUERY"));
        assert!(debug_str.contains("MATCH"));
        assert!(debug_str.contains("123.456"));
    }

    #[test]
    fn test_slowlog_entry_equality() {
        let entry1 = SlowlogEntry {
            timestamp: 100,
            command: "CMD".to_string(),
            arguments: "args".to_string(),
            time_taken: 1.0,
        };

        let entry2 = SlowlogEntry {
            timestamp: 100,
            command: "CMD".to_string(),
            arguments: "args".to_string(),
            time_taken: 1.0,
        };

        assert_eq!(entry1, entry2);
    }

    #[test]
    fn test_slowlog_entry_fields() {
        let entry = SlowlogEntry {
            timestamp: 999,
            command: "TEST".to_string(),
            arguments: "test args".to_string(),
            time_taken: 0.5,
        };

        assert_eq!(entry.timestamp, 999);
        assert_eq!(entry.command, "TEST");
        assert_eq!(entry.arguments, "test args");
        assert_eq!(entry.time_taken, 0.5);
    }

    fn valid_entry() -> redis::Value {
        redis::Value::Array(vec![
            redis::Value::BulkString(b"1700000000".to_vec()),
            redis::Value::BulkString(b"GRAPH.QUERY".to_vec()),
            redis::Value::BulkString(b"MATCH (n) RETURN n".to_vec()),
            redis::Value::BulkString(b"12.5".to_vec()),
        ])
    }

    #[test]
    fn parse_slowlog_parses_valid_entries() {
        let parsed = parse_slowlog(redis::Value::Array(vec![valid_entry()])).expect("valid");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].timestamp, 1_700_000_000);
        assert_eq!(parsed[0].command, "GRAPH.QUERY");
        assert_eq!(parsed[0].arguments, "MATCH (n) RETURN n");
        assert_eq!(parsed[0].time_taken, 12.5);
    }

    /// An entry as FalkorDB 4.22 and 6.2 send it over RESP2: the four fields and a trailing nil.
    fn resp2_entry() -> redis::Value {
        redis::Value::Array(vec![
            redis::Value::BulkString(b"1795673671".to_vec()),
            redis::Value::BulkString(b"GRAPH.QUERY".to_vec()),
            redis::Value::BulkString(b"UNWIND range(1, 2000000) AS x RETURN count(x)".to_vec()),
            redis::Value::BulkString(b"53.241".to_vec()),
            redis::Value::Nil,
        ])
    }

    /// The same entry over RESP3: the timestamp is a double.
    fn resp3_entry() -> redis::Value {
        redis::Value::Array(vec![
            redis::Value::Double(1_795_673_671.0),
            redis::Value::BulkString(b"GRAPH.QUERY".to_vec()),
            redis::Value::BulkString(b"UNWIND range(1, 2000000) AS x RETURN count(x)".to_vec()),
            redis::Value::BulkString(b"53.241".to_vec()),
            redis::Value::Nil,
        ])
    }

    #[test]
    fn parse_slowlog_reads_server_entries_in_both_protocols() {
        for entry in [resp2_entry(), resp3_entry()] {
            let parsed = parse_slowlog(redis::Value::Array(vec![entry])).expect("parses");
            assert_eq!(parsed.len(), 1);
            assert_eq!(parsed[0].timestamp, 1_795_673_671);
            assert_eq!(parsed[0].command, "GRAPH.QUERY");
            assert_eq!(
                parsed[0].arguments,
                "UNWIND range(1, 2000000) AS x RETURN count(x)"
            );
            assert_eq!(parsed[0].time_taken, 53.241);
        }
    }

    #[test]
    fn parse_slowlog_reads_an_integer_timestamp() {
        let mut entry = redis_value_as_vec(valid_entry()).unwrap();
        entry[0] = redis::Value::Int(1_700_000_000);
        let parsed = SlowlogEntry::parse(redis::Value::Array(entry)).expect("parses");
        assert_eq!(parsed.timestamp, 1_700_000_000);
    }

    #[test]
    fn parse_slowlog_rejects_a_timestamp_that_is_not_a_whole_number() {
        for timestamp in [
            redis::Value::Double(1.5),
            redis::Value::Double(f64::NAN),
            redis::Value::BulkString(b"soon".to_vec()),
            redis::Value::Nil,
        ] {
            let mut entry = redis_value_as_vec(valid_entry()).unwrap();
            entry[0] = timestamp;
            assert!(matches!(
                SlowlogEntry::parse(redis::Value::Array(entry)),
                Err(FalkorDBError::ParsingI64)
            ));
        }
    }

    #[test]
    fn parse_slowlog_empty_reply_is_empty() {
        assert!(parse_slowlog(redis::Value::Array(vec![]))
            .expect("empty")
            .is_empty());
    }

    #[test]
    fn parse_slowlog_propagates_malformed_entry_error() {
        // A second entry with the wrong element count must fail the whole call rather than being
        // silently dropped.
        let value = redis::Value::Array(vec![
            valid_entry(),
            redis::Value::Array(vec![redis::Value::Int(1)]),
        ]);
        assert!(matches!(
            parse_slowlog(value),
            Err(FalkorDBError::ParsingArrayToStructElementCount(_))
        ));
    }

    #[test]
    fn parse_slowlog_rejects_non_array_reply() {
        assert!(parse_slowlog(redis::Value::Int(5)).is_err());
    }
}
