/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    connection::blocking::FalkorSyncConnection,
    parser::{redis_value_as_string, redis_value_as_vec},
    FalkorDBError, FalkorResult,
};
use std::collections::HashMap;

#[cfg(feature = "tokio")]
use crate::connection::asynchronous::FalkorAsyncConnection;

pub(crate) mod blocking;
pub(crate) mod builder;

#[cfg(feature = "tokio")]
pub(crate) mod asynchronous;

#[allow(clippy::large_enum_variant)]
pub(crate) enum FalkorClientProvider {
    #[cfg(test)]
    None,

    Redis {
        client: redis::Client,
        sentinel: Option<redis::sentinel::SentinelClient>,
        #[cfg(feature = "embedded")]
        embedded_server: Option<std::sync::Arc<crate::embedded::EmbeddedServer>>,
    },
}

impl FalkorClientProvider {
    pub(crate) fn get_connection(&mut self) -> FalkorResult<FalkorSyncConnection> {
        Ok(match self {
            FalkorClientProvider::Redis {
                sentinel: Some(sentinel),
                ..
            } => FalkorSyncConnection::Redis(
                sentinel
                    .get_connection()
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
            ),

            FalkorClientProvider::Redis { client, .. } => FalkorSyncConnection::Redis(
                client
                    .get_connection()
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
            ),
            #[cfg(test)]
            FalkorClientProvider::None => Err(FalkorDBError::UnavailableProvider)?,
        })
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn get_async_connection(&mut self) -> FalkorResult<FalkorAsyncConnection> {
        Ok(match self {
            FalkorClientProvider::Redis {
                sentinel: Some(sentinel),
                ..
            } => FalkorAsyncConnection::Redis(
                sentinel
                    .get_async_connection()
                    .await
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
            ),
            FalkorClientProvider::Redis { client, .. } => FalkorAsyncConnection::Redis(
                client
                    .get_multiplexed_tokio_connection()
                    .await
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
            ),
            #[cfg(test)]
            FalkorClientProvider::None => Err(FalkorDBError::UnavailableProvider)?,
        })
    }

    pub(crate) fn set_sentinel(
        &mut self,
        sentinel_client: redis::sentinel::SentinelClient,
    ) {
        match self {
            FalkorClientProvider::Redis { sentinel, .. } => *sentinel = Some(sentinel_client),
            #[cfg(test)]
            FalkorClientProvider::None => {}
        }
    }

    pub(crate) fn get_sentinel_client_common(
        &self,
        connection_info: &redis::ConnectionInfo,
        sentinel_masters: Vec<redis::Value>,
    ) -> FalkorResult<Option<redis::sentinel::SentinelClient>> {
        if sentinel_masters.len() != 1 {
            return Err(FalkorDBError::SentinelMastersCount);
        }

        let sentinel_master: HashMap<_, _> = sentinel_masters
            .into_iter()
            .next()
            .and_then(|master| master.into_sequence().ok())
            .ok_or(FalkorDBError::SentinelMastersCount)?
            .chunks_exact(2)
            .flat_map(TryInto::<&[redis::Value; 2]>::try_into) // TODO: In the future, check if this can be done with no copying, but this should be a rare function call tbh
            .flat_map(|[key, val]| {
                redis_value_as_string(key.to_owned())
                    .and_then(|key| redis_value_as_string(val.to_owned()).map(|val| (key, val)))
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

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Get Sentinel Client", skip_all, level = "info")
    )]
    pub(crate) fn get_sentinel_client(
        &mut self,
        connection_info: &redis::ConnectionInfo,
    ) -> FalkorResult<Option<redis::sentinel::SentinelClient>> {
        let mut conn = self.get_connection()?;
        if !conn.check_is_redis_sentinel()? {
            return Ok(None);
        }

        conn.execute_command(None, "SENTINEL", Some("MASTERS"), None)
            .and_then(redis_value_as_vec)
            .and_then(|sentinel_masters| {
                self.get_sentinel_client_common(connection_info, sentinel_masters)
            })
    }

    #[cfg(feature = "tokio")]
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Get Sentinel Client", skip_all, level = "info")
    )]
    pub(crate) async fn get_sentinel_client_async(
        &mut self,
        connection_info: &redis::ConnectionInfo,
    ) -> FalkorResult<Option<redis::sentinel::SentinelClient>> {
        let mut conn = self.get_async_connection().await?;
        if !conn.check_is_redis_sentinel().await? {
            return Ok(None);
        }

        conn.execute_command(None, "SENTINEL", Some("MASTERS"), None)
            .await
            .and_then(redis_value_as_vec)
            .and_then(|sentinel_masters| {
                self.get_sentinel_client_common(connection_info, sentinel_masters)
            })
    }
}

pub(crate) trait ProvidesSyncConnections: Sync + Send {
    fn get_connection(&self) -> FalkorResult<FalkorSyncConnection>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_falkor_client_provider_none_connection() {
        let mut provider = FalkorClientProvider::None;
        let result = provider.get_connection();
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, FalkorDBError::UnavailableProvider));
        }
    }

    #[test]
    #[cfg(feature = "tokio")]
    fn test_falkor_client_provider_none_async_connection() {
        use tokio::runtime::Runtime;
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut provider = FalkorClientProvider::None;
            let result = provider.get_async_connection().await;
            assert!(result.is_err());
            if let Err(e) = result {
                assert!(matches!(e, FalkorDBError::UnavailableProvider));
            }
        });
    }

    #[test]
    fn test_falkor_client_provider_set_sentinel() {
        let mut provider = FalkorClientProvider::None;
        // Just test that set_sentinel doesn't panic with None provider
        let connection_info = redis::ConnectionInfo {
            addr: redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 26379),
            redis: redis::RedisConnectionInfo::default(),
        };
        let sentinel = redis::sentinel::SentinelClient::build(
            vec![connection_info],
            "master".to_string(),
            None,
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();
        provider.set_sentinel(sentinel);
    }

    #[test]
    fn test_get_sentinel_client_common_invalid_count() {
        let provider = FalkorClientProvider::None;
        let connection_info = redis::ConnectionInfo {
            addr: redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379),
            redis: redis::RedisConnectionInfo::default(),
        };

        // Test with empty vector
        let result = provider.get_sentinel_client_common(&connection_info, vec![]);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, FalkorDBError::SentinelMastersCount));
        }

        // Test with multiple masters
        let result = provider.get_sentinel_client_common(
            &connection_info,
            vec![redis::Value::Nil, redis::Value::Nil],
        );
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, FalkorDBError::SentinelMastersCount));
        }
    }

    #[test]
    #[cfg(feature = "embedded")]
    fn test_falkor_client_provider_with_embedded_server() {
        // Test that FalkorClientProvider::Redis can hold an embedded server
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let _provider = FalkorClientProvider::Redis {
            client,
            sentinel: None,
            embedded_server: None,
        };
        // Just verify the structure can be created
    }

    #[test]
    fn test_falkor_client_provider_redis_without_sentinel() {
        // Test creating a Redis provider without sentinel
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let _provider = FalkorClientProvider::Redis {
            client,
            sentinel: None,
            #[cfg(feature = "embedded")]
            embedded_server: None,
        };
        // Just verify the structure can be created
    }
}
