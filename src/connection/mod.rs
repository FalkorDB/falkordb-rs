/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{FalkorDBError, FalkorResult};

pub(crate) mod blocking;

#[cfg(feature = "tokio")]
pub(crate) mod asynchronous;

/// Turn a reply that is itself a server error into an `Err`. redis-rs returns error replies as
/// `Ok(Value::ServerError)`, so without this a command whose reply is otherwise ignored would
/// succeed silently.
pub(crate) fn reject_server_error(value: redis::Value) -> FalkorResult<redis::Value> {
    match value {
        redis::Value::ServerError(error) => Err(server_error(error)),
        value => Ok(value),
    }
}

/// A server's error reply as a [`FalkorDBError`]. The cluster/master-down family is a connection
/// failure, as for transport errors in [`map_redis_err`]; anything else keeps the server's whole
/// message, minus the generic `ERR` code. redis-rs splits a reply into a code and details, but
/// FalkorDB messages rarely start with a code (`Division by zero`), so both parts are kept.
pub(crate) fn server_error(error: redis::ServerError) -> FalkorDBError {
    match error.kind() {
        Some(redis::ServerErrorKind::ClusterDown | redis::ServerErrorKind::MasterDown) => {
            FalkorDBError::ConnectionDown
        }
        _ => FalkorDBError::RedisError(match (error.code(), error.details()) {
            ("ERR", Some(details)) => details.to_string(),
            (code, Some(details)) => format!("{code} {details}"),
            (code, None) => code.to_string(),
        }),
    }
}

fn map_redis_err(error: redis::RedisError) -> FalkorDBError {
    match error.kind() {
        redis::ErrorKind::Io
        | redis::ErrorKind::ClusterConnectionNotFound
        | redis::ErrorKind::Server(redis::ServerErrorKind::ClusterDown)
        | redis::ErrorKind::Server(redis::ServerErrorKind::MasterDown) => {
            FalkorDBError::ConnectionDown
        }
        _ => FalkorDBError::RedisError(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An error reply exactly as the server sends it.
    fn error_reply(reply: &str) -> redis::Value {
        redis::parse_redis_value(format!("-{reply}\r\n").as_bytes()).expect("an error reply")
    }

    #[test]
    fn reject_server_error_keeps_the_whole_server_message() {
        for (reply, message) in [
            // FalkorDB's own messages have no code: nothing may be dropped from them.
            ("Division by zero", "Division by zero"),
            (
                "Invalid input 'X': expected end of input",
                "Invalid input 'X': expected end of input",
            ),
            (
                "graph.RO_QUERY is to be executed only on read-only queries",
                "graph.RO_QUERY is to be executed only on read-only queries",
            ),
            // Only the generic `ERR` code is dropped.
            (
                "ERR Invalid graph operation on empty key",
                "Invalid graph operation on empty key",
            ),
            ("ERR", "ERR"),
        ] {
            assert_eq!(
                reject_server_error(error_reply(reply)),
                Err(FalkorDBError::RedisError(message.to_string())),
                "{reply}"
            );
        }
    }

    #[test]
    fn reject_server_error_reports_a_down_cluster_or_master_as_connection_down() {
        for reply in [
            "CLUSTERDOWN The cluster is down",
            "MASTERDOWN Link with MASTER is down",
        ] {
            assert_eq!(
                reject_server_error(error_reply(reply)),
                Err(FalkorDBError::ConnectionDown),
                "{reply}"
            );
        }
    }

    #[test]
    fn reject_server_error_passes_other_replies_through() {
        assert_eq!(
            reject_server_error(redis::Value::Okay),
            Ok(redis::Value::Okay)
        );
    }

    #[test]
    fn test_map_redis_err_io_error() {
        let error = redis::RedisError::from((redis::ErrorKind::Io, "test error"));
        let result = map_redis_err(error);
        assert!(matches!(result, FalkorDBError::ConnectionDown));
    }

    #[test]
    fn test_map_redis_err_cluster_not_found() {
        let error = redis::RedisError::from((redis::ErrorKind::ClusterConnectionNotFound, "test"));
        let result = map_redis_err(error);
        assert!(matches!(result, FalkorDBError::ConnectionDown));
    }

    #[test]
    fn test_map_redis_err_cluster_down() {
        let error = redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::ClusterDown),
            "test",
        ));
        let result = map_redis_err(error);
        assert!(matches!(result, FalkorDBError::ConnectionDown));
    }

    #[test]
    fn test_map_redis_err_master_down() {
        let error = redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::MasterDown),
            "test",
        ));
        let result = map_redis_err(error);
        assert!(matches!(result, FalkorDBError::ConnectionDown));
    }

    #[test]
    fn test_map_redis_err_other() {
        let error = redis::RedisError::from((redis::ErrorKind::UnexpectedReturnType, "test error"));
        let result = map_redis_err(error);
        assert!(matches!(result, FalkorDBError::RedisError(_)));
    }
}
