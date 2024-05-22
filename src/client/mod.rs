/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::connection::blocking::FalkorSyncConnection;
use anyhow::Result;
use std::time::Duration;

pub(crate) mod blocking;
pub(crate) mod builder;

#[cfg(feature = "tokio")]
pub(crate) mod asynchronous;

pub(crate) enum FalkorClientProvider {
    #[cfg(feature = "redis")]
    Redis(redis::Client),
}

impl FalkorClientProvider {
    pub(crate) fn get_connection(
        &self,
        connection_timeout: Option<Duration>,
    ) -> Result<FalkorSyncConnection> {
        Ok(match self {
            #[cfg(feature = "redis")]
            FalkorClientProvider::Redis(redis_client) => connection_timeout
                .map(|timeout| redis_client.get_connection_with_timeout(timeout))
                .unwrap_or_else(|| redis_client.get_connection())?
                .into(),
        })
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn get_async_connection(
        &self,
        connection_timeout: Option<Duration>,
    ) -> Result<crate::FalkorAsyncConnection> {
        Ok(match self {
            FalkorClientProvider::Redis(redis_client) => match connection_timeout {
                Some(timeout) => {
                    redis_client
                        .get_multiplexed_tokio_connection_with_response_timeouts(timeout, timeout)
                        .await?
                }
                None => redis_client.get_multiplexed_tokio_connection().await?,
            }
            .into(),
        })
    }
}
