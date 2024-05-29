/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{connection::blocking::FalkorSyncConnection, FalkorDBError, FalkorResult};
use std::time::Duration;

pub(crate) mod blocking;
pub(crate) mod builder;

pub(crate) enum FalkorClientProvider {
    #[cfg(feature = "redis")]
    Redis(redis::Client),
}

impl FalkorClientProvider {
    pub(crate) fn get_connection(
        &self,
        connection_timeout: Option<Duration>,
    ) -> FalkorResult<FalkorSyncConnection> {
        Ok(match self {
            #[cfg(feature = "redis")]
            FalkorClientProvider::Redis(redis_client) => connection_timeout
                .map(|timeout| redis_client.get_connection_with_timeout(timeout))
                .unwrap_or_else(|| redis_client.get_connection())
                .map_err(|err| FalkorDBError::RedisConnectionError(err.to_string()))?
                .into(),
        })
    }
}
