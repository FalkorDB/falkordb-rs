/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::FalkorDBError;

#[cfg(feature = "redis")]
pub(super) fn map_redis_error(err: redis::RedisError) -> FalkorDBError {
    match err.kind() {
        redis::ErrorKind::IoError
        | redis::ErrorKind::ClusterConnectionNotFound
        | redis::ErrorKind::ClusterDown
        | redis::ErrorKind::MasterDown => FalkorDBError::ConnectionDown,
        _ => FalkorDBError::RedisError(err.to_string()),
    }
}
