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
        /// Sentinel client configured to hand out connections to replica nodes,
        /// used to route read-only queries away from the primary. `None` when the
        /// deployment is not Sentinel-managed or has no readable replicas.
        sentinel_replica: Option<redis::sentinel::SentinelClient>,
        #[cfg(feature = "embedded")]
        #[allow(dead_code)]
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
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
            ),
            #[cfg(test)]
            FalkorClientProvider::None => Err(FalkorDBError::UnavailableProvider)?,
        })
    }

    /// Returns a connection routed to a replica node when one is available
    /// (Sentinel deployments with replicas), otherwise falls back to the regular
    /// (primary) connection. Used to serve read-only queries from replicas.
    ///
    /// If a replica Sentinel client exists but the connection attempt fails (e.g.
    /// the replica is temporarily unreachable), the error is silently discarded and
    /// the call falls back to the primary so that read-only queries keep working.
    pub(crate) fn get_readonly_connection(&mut self) -> FalkorResult<FalkorSyncConnection> {
        // Obtain the replica result inside a scoped block so the borrow on `self`
        // ends before we potentially call `self.get_connection()` as fallback.
        let replica_result = if let FalkorClientProvider::Redis {
            sentinel_replica: Some(replica),
            ..
        } = self
        {
            Some(replica.get_connection())
        } else {
            None
        };

        if let Some(Ok(conn)) = replica_result {
            return Ok(FalkorSyncConnection::Redis(conn));
        }

        self.get_connection()
    }

    /// Async counterpart of [`get_readonly_connection`](Self::get_readonly_connection).
    ///
    /// Falls back to the primary connection when the replica Sentinel is
    /// unreachable, matching the behavior of the sync version.
    #[cfg(feature = "tokio")]
    pub(crate) async fn get_async_readonly_connection(
        &mut self
    ) -> FalkorResult<FalkorAsyncConnection> {
        let replica_result = if let FalkorClientProvider::Redis {
            sentinel_replica: Some(replica),
            ..
        } = self
        {
            Some(replica.get_async_connection().await)
        } else {
            None
        };

        if let Some(Ok(conn)) = replica_result {
            return Ok(FalkorAsyncConnection::Redis(conn));
        }

        self.get_async_connection().await
    }

    /// Whether this provider can route read-only queries to replica nodes.
    pub(crate) fn has_sentinel_replica(&self) -> bool {
        matches!(
            self,
            FalkorClientProvider::Redis {
                sentinel_replica: Some(_),
                ..
            }
        )
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

    pub(crate) fn set_sentinel_replica(
        &mut self,
        sentinel_client: redis::sentinel::SentinelClient,
    ) {
        match self {
            FalkorClientProvider::Redis {
                sentinel_replica, ..
            } => *sentinel_replica = Some(sentinel_client),
            #[cfg(test)]
            FalkorClientProvider::None => {}
        }
    }

    #[cfg(test)]
    pub(crate) fn get_sentinel_client_common(
        &self,
        connection_info: &redis::ConnectionInfo,
        sentinel_masters: Vec<redis::Value>,
    ) -> FalkorResult<Option<redis::sentinel::SentinelClient>> {
        self.build_sentinel_client(
            connection_info,
            sentinel_masters,
            redis::sentinel::SentinelServerType::Master,
        )
    }

    /// Build a [`SentinelClient`](redis::sentinel::SentinelClient) for the given
    /// `server_type` (master or replica) out of the `SENTINEL MASTERS` reply.
    fn build_sentinel_client(
        &self,
        connection_info: &redis::ConnectionInfo,
        sentinel_masters: Vec<redis::Value>,
        server_type: redis::sentinel::SentinelServerType,
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
                name.clone(),
                Some({
                    let node_info = redis::sentinel::SentinelNodeConnectionInfo::default()
                        .set_redis_connection_info(connection_info.redis_settings().clone());
                    match connection_info.addr() {
                        redis::ConnectionAddr::TcpTls { insecure: true, .. } => {
                            node_info.set_tls_mode(redis::TlsMode::Insecure)
                        }
                        redis::ConnectionAddr::TcpTls {
                            insecure: false, ..
                        } => node_info.set_tls_mode(redis::TlsMode::Secure),
                        _ => node_info,
                    }
                }),
                server_type,
            )
            .map_err(|err| FalkorDBError::SentinelConnection(err.to_string()))?,
        ))
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Get Sentinel Clients", skip_all, level = "info")
    )]
    pub(crate) fn get_sentinel_client(
        &mut self,
        connection_info: &redis::ConnectionInfo,
    ) -> FalkorResult<Option<SentinelClients>> {
        let mut conn = self.get_connection()?;
        if !conn.check_is_redis_sentinel()? {
            return Ok(None);
        }

        let sentinel_masters = conn
            .execute_command(None, "SENTINEL", Some("MASTERS"), None)
            .and_then(redis_value_as_vec)?;

        self.build_sentinel_clients(connection_info, sentinel_masters)
    }

    #[cfg(feature = "tokio")]
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Get Sentinel Clients", skip_all, level = "info")
    )]
    pub(crate) async fn get_sentinel_client_async(
        &mut self,
        connection_info: &redis::ConnectionInfo,
    ) -> FalkorResult<Option<SentinelClients>> {
        let mut conn = self.get_async_connection().await?;
        if !conn.check_is_redis_sentinel().await? {
            return Ok(None);
        }

        let sentinel_masters = conn
            .execute_command(None, "SENTINEL", Some("MASTERS"), None)
            .await
            .and_then(redis_value_as_vec)?;

        self.build_sentinel_clients(connection_info, sentinel_masters)
    }

    /// Build both the master and replica [`SentinelClient`](redis::sentinel::SentinelClient)s
    /// from a single `SENTINEL MASTERS` reply, so read-only queries can be routed to replicas.
    fn build_sentinel_clients(
        &self,
        connection_info: &redis::ConnectionInfo,
        sentinel_masters: Vec<redis::Value>,
    ) -> FalkorResult<Option<SentinelClients>> {
        let master = self
            .build_sentinel_client(
                connection_info,
                sentinel_masters.clone(),
                redis::sentinel::SentinelServerType::Master,
            )?
            .ok_or(FalkorDBError::SentinelMastersCount)?;
        let replica = self.build_sentinel_client(
            connection_info,
            sentinel_masters,
            redis::sentinel::SentinelServerType::Replica,
        )?;

        Ok(Some(SentinelClients { master, replica }))
    }
}

/// The pair of Sentinel clients derived from a Sentinel deployment: the `master`
/// client serves writes/primary reads, while the optional `replica` client routes
/// read-only queries to replica nodes.
pub(crate) struct SentinelClients {
    pub(crate) master: redis::sentinel::SentinelClient,
    pub(crate) replica: Option<redis::sentinel::SentinelClient>,
}

pub(crate) trait ProvidesSyncConnections: Sync + Send {
    fn get_connection(&self) -> FalkorResult<FalkorSyncConnection>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

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
    fn test_has_sentinel_replica_default() {
        // A Redis provider with no replica Sentinel must not advertise replica routing.
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let provider = FalkorClientProvider::Redis {
            client,
            sentinel: None,
            sentinel_replica: None,
            #[cfg(feature = "embedded")]
            embedded_server: None,
        };
        assert!(!provider.has_sentinel_replica());
    }

    #[test]
    fn test_get_readonly_connection_falls_back_without_replica() {
        // Without a replica Sentinel, get_readonly_connection defers to get_connection,
        // which for the None provider surfaces UnavailableProvider rather than panicking.
        let mut provider = FalkorClientProvider::None;
        assert!(!provider.has_sentinel_replica());
        let result = provider.get_readonly_connection();
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, FalkorDBError::UnavailableProvider));
        }
    }

    #[test]
    fn test_set_sentinel_replica() {
        let mut provider = FalkorClientProvider::None;
        // Setting a replica Sentinel on the None provider is a no-op and must not panic.
        let connection_info = redis::ConnectionInfo::from_str("redis://127.0.0.1:26379").unwrap();
        let replica = redis::sentinel::SentinelClient::build(
            vec![connection_info],
            "master".to_string(),
            None,
            redis::sentinel::SentinelServerType::Replica,
        )
        .unwrap();
        provider.set_sentinel_replica(replica);
        assert!(!provider.has_sentinel_replica());
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
        let connection_info = redis::ConnectionInfo::from_str("redis://127.0.0.1:26379").unwrap();
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
        let connection_info = redis::ConnectionInfo::from_str("redis://127.0.0.1:6379").unwrap();

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
            sentinel_replica: None,
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
            sentinel_replica: None,
            #[cfg(feature = "embedded")]
            embedded_server: None,
        };
        // Just verify the structure can be created
    }

    #[test]
    fn test_get_readonly_connection_falls_back_when_replica_unreachable() {
        // When a replica SentinelClient exists but the replica is unreachable, the
        // implementation should swallow that error and fall back to the primary client
        // rather than propagating the replica-specific error to the caller.
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        // Port 1 is reliably unroutable in test environments.
        let connection_info = redis::ConnectionInfo::from_str("redis://127.0.0.1:1").unwrap();
        let replica = redis::sentinel::SentinelClient::build(
            vec![connection_info],
            "mymaster".to_string(),
            None,
            redis::sentinel::SentinelServerType::Replica,
        )
        .unwrap();
        let mut provider = FalkorClientProvider::Redis {
            client,
            sentinel: None,
            sentinel_replica: Some(replica),
            #[cfg(feature = "embedded")]
            embedded_server: None,
        };
        // The replica connection will fail; the code must not propagate that error
        // directly but instead attempt the primary path. The key assertion is that
        // the call falls back to the primary without panicking: when the primary is
        // reachable it returns `Ok` (proving the fallback ran), and when it is not it
        // returns a primary-path `RedisError` (never a replica-specific error).
        match provider.get_readonly_connection() {
            Ok(_) => {}
            Err(e) => assert!(
                matches!(e, FalkorDBError::RedisError(_)),
                "error should come from the primary fallback path"
            ),
        }
    }

    #[test]
    #[cfg(feature = "tokio")]
    fn test_get_async_readonly_connection_falls_back_when_replica_unreachable() {
        use tokio::runtime::Runtime;
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
            let connection_info = redis::ConnectionInfo::from_str("redis://127.0.0.1:1").unwrap();
            let replica = redis::sentinel::SentinelClient::build(
                vec![connection_info],
                "mymaster".to_string(),
                None,
                redis::sentinel::SentinelServerType::Replica,
            )
            .unwrap();
            let mut provider = FalkorClientProvider::Redis {
                client,
                sentinel: None,
                sentinel_replica: Some(replica),
                #[cfg(feature = "embedded")]
                embedded_server: None,
            };
            let result = provider.get_async_readonly_connection().await;
            match result {
                Ok(_) => {}
                Err(e) => assert!(
                    matches!(e, FalkorDBError::RedisError(_)),
                    "error should come from the primary fallback path"
                ),
            }
        });
    }
}
