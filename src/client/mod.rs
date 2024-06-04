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
    Redis {
        client: redis::Client,
        sentinel: Option<redis::Client>,
    },
}

impl FalkorClientProvider {
    pub(crate) fn get_connection(
        &mut self,
        connection_timeout: Option<Duration>,
    ) -> FalkorResult<FalkorSyncConnection> {
        Ok(match self {
            #[cfg(feature = "redis")]
            FalkorClientProvider::Redis {
                client: redis_client,
                sentinel,
            } => FalkorSyncConnection::Redis(
                match (
                    connection_timeout,
                    sentinel.as_ref().unwrap_or(redis_client),
                ) {
                    (None, redis_client) => redis_client.get_connection(),
                    (Some(timeout), redis_client) => {
                        redis_client.get_connection_with_timeout(timeout)
                    }
                }
                .map_err(|err| FalkorDBError::RedisConnectionError(err.to_string()))?,
            ),
        })
    }

    #[cfg(feature = "redis")]
    pub(crate) fn set_sentinel(
        &mut self,
        sentinel_client: redis::Client,
    ) {
        match self {
            FalkorClientProvider::Redis { sentinel, .. } => *sentinel = Some(sentinel_client),
        }
    }
}
