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
        redis::ErrorKind::IoError
        | redis::ErrorKind::ClusterConnectionNotFound
        | redis::ErrorKind::ClusterDown
        | redis::ErrorKind::MasterDown => FalkorDBError::ConnectionDown,
        _ => FalkorDBError::RedisError(error.to_string()),
    }
}
