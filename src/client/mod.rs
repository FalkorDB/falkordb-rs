/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{connection::blocking::FalkorSyncConnection, FalkorDBError, FalkorResult};

pub(crate) mod blocking;
pub(crate) mod builder;

pub(crate) enum FalkorClientProvider {
    None,
    #[cfg(feature = "redis")]
    Redis {
        client: redis::Client,
        sentinel: Option<redis::sentinel::SentinelClient>,
    },
}

impl FalkorClientProvider {
    pub(crate) fn get_connection(&mut self) -> FalkorResult<FalkorSyncConnection> {
        Ok(match self {
            #[cfg(feature = "redis")]
            FalkorClientProvider::Redis {
                sentinel: Some(sentinel),
                ..
            } => FalkorSyncConnection::Redis(
                sentinel
                    .get_connection()
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
            ),
            #[cfg(feature = "redis")]
            FalkorClientProvider::Redis { client, .. } => FalkorSyncConnection::Redis(
                client
                    .get_connection()
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
            ),
            FalkorClientProvider::None => Err(FalkorDBError::UnavailableProvider)?,
        })
    }

    #[cfg(feature = "redis")]
    pub(crate) fn set_sentinel(
        &mut self,
        sentinel_client: redis::sentinel::SentinelClient,
    ) {
        match self {
            FalkorClientProvider::Redis { sentinel, .. } => *sentinel = Some(sentinel_client),
            FalkorClientProvider::None => {}
        }
    }
}
