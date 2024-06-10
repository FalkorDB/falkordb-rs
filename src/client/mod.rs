/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{connection::blocking::FalkorSyncConnection, FalkorDBError, FalkorResult, FalkorValue};
use std::collections::HashMap;
use std::time::Duration;

#[cfg(feature = "tokio")]
use crate::connection::asynchronous::FalkorAsyncConnection;

#[cfg(feature = "tokio")]
pub(crate) mod asynchronous;
pub(crate) mod blocking;
pub(crate) mod builder;

#[allow(clippy::large_enum_variant)]
pub(crate) enum FalkorClientProvider {
    #[allow(unused)]
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

    #[cfg(feature = "tokio")]
    pub(crate) async fn get_async_connection(&mut self) -> FalkorResult<FalkorAsyncConnection> {
        Ok(match self {
            #[cfg(feature = "redis")]
            FalkorClientProvider::Redis {
                sentinel: Some(sentinel),
                ..
            } => FalkorAsyncConnection::Redis(
                sentinel
                    .get_async_connection()
                    .await
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
            ),
            #[cfg(feature = "redis")]
            FalkorClientProvider::Redis { client, .. } => FalkorAsyncConnection::Redis(
                client
                    .get_multiplexed_tokio_connection_with_response_timeouts(
                        Duration::from_secs(2),
                        Duration::from_secs(2),
                    )
                    .await
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

    #[cfg(feature = "redis")]
    pub(crate) fn get_sentinel_client_common(
        &self,
        connection_info: &redis::ConnectionInfo,
        sentinel_masters: Vec<FalkorValue>,
    ) -> FalkorResult<Option<redis::sentinel::SentinelClient>> {
        if sentinel_masters.len() != 1 {
            return Err(FalkorDBError::SentinelMastersCount);
        }

        let sentinel_master: HashMap<_, _> = sentinel_masters
            .into_iter()
            .next()
            .ok_or(FalkorDBError::SentinelMastersCount)?
            .into_vec()?
            .chunks_exact(2)
            .flat_map(|chunk| TryInto::<[FalkorValue; 2]>::try_into(chunk.to_vec()))
            .flat_map(|[key, val]| {
                Result::<_, FalkorDBError>::Ok((key.into_string()?, val.into_string()?))
            })
            .collect();

        let name = sentinel_master
            .get("name")
            .ok_or(FalkorDBError::SentinelMastersCount)?;

        Ok(Some(
            redis::sentinel::SentinelClient::build(
                vec![connection_info.to_owned()],
                name.to_string(),
                Some(redis::sentinel::SentinelNodeConnectionInfo {
                    tls_mode: match connection_info.addr {
                        redis::ConnectionAddr::TcpTls { insecure: true, .. } => {
                            Some(redis::TlsMode::Insecure)
                        }
                        redis::ConnectionAddr::TcpTls {
                            insecure: false, ..
                        } => Some(redis::TlsMode::Secure),
                        _ => None,
                    },
                    redis_connection_info: Some(connection_info.redis.clone()),
                }),
                redis::sentinel::SentinelServerType::Master,
            )
            .map_err(|err| FalkorDBError::SentinelConnection(err.to_string()))?,
        ))
    }

    #[cfg(feature = "redis")]
    pub(crate) fn get_sentinel_client(
        &mut self,
        connection_info: &redis::ConnectionInfo,
    ) -> FalkorResult<Option<redis::sentinel::SentinelClient>> {
        let mut conn = self.get_connection()?;
        if !conn.check_is_redis_sentinel()? {
            return Ok(None);
        }

        self.get_sentinel_client_common(
            connection_info,
            conn.execute_command(None, "SENTINEL", Some("MASTERS"), None)
                .and_then(|res| res.into_vec())?,
        )
    }

    #[cfg(all(feature = "redis", feature = "tokio"))]
    pub(crate) async fn get_sentinel_client_async(
        &mut self,
        connection_info: &redis::ConnectionInfo,
    ) -> FalkorResult<Option<redis::sentinel::SentinelClient>> {
        let mut conn = self.get_async_connection().await?;
        if !conn.check_is_redis_sentinel().await? {
            return Ok(None);
        }

        // This could have been so simple using the Sentinel API, but it requires a service name
        // Perhaps in the future we can use it if we only support the master instance to be called 'master'?
        self.get_sentinel_client_common(
            connection_info,
            conn.execute_command(None, "SENTINEL", Some("MASTERS"), None)
                .await
                .and_then(|res| res.into_vec())?,
        )
    }
}

pub(crate) trait ProvidesSyncConnections {
    fn get_connection(&self) -> FalkorResult<FalkorSyncConnection>;
}
