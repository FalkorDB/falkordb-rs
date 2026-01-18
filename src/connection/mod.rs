/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::FalkorDBError;

pub(crate) mod blocking;

#[cfg(feature = "tokio")]
pub(crate) mod asynchronous;

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
