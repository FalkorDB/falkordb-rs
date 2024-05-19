/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::connection::asynchronous::FalkorAsyncConnection;
use crate::connection::blocking::FalkorSyncConnection;
use anyhow::Result;
use std::time::Duration;

#[cfg(feature = "tokio")]
pub mod asynchronous;
pub mod blocking;
pub mod builder;

pub(crate) enum FalkorClientImpl {
    #[cfg(feature = "redis")]
    Redis(redis::Client),
}

impl FalkorClientImpl {
    pub(crate) fn get_connection(
        &self,
        connection_timeout: Option<Duration>,
    ) -> Result<FalkorSyncConnection> {
        Ok(match self {
            #[cfg(feature = "redis")]
            FalkorClientImpl::Redis(redis_client) => connection_timeout
                .map(|timeout| redis_client.get_connection_with_timeout(timeout))
                .unwrap_or_else(|| redis_client.get_connection())?
                .into(),
        })
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn get_async_connection(
        &self,
        connection_timeout: Option<Duration>,
    ) -> Result<FalkorAsyncConnection> {
        Ok(match self {
            FalkorClientImpl::Redis(redis_client) => match connection_timeout {
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
