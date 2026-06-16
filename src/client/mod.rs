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
use std::num::NonZeroU8;

#[cfg(feature = "tokio")]
use std::num::NonZeroUsize;

#[cfg(feature = "tokio")]
use crate::connection::asynchronous::FalkorAsyncConnection;

pub(crate) mod blocking;
pub(crate) mod builder;

#[cfg(feature = "tokio")]
pub(crate) mod asynchronous;

/// Selects how a client manages its underlying Redis connection(s).
///
/// The asynchronous client defaults to [`ConnectionStrategy::Multiplexed`], which
/// pipelines many concurrent in-flight commands over a small number of shared,
/// auto-reconnecting sockets. The synchronous client always uses
/// [`ConnectionStrategy::Pooled`] (a blocking connection cannot be multiplexed).
///
/// # Example
/// ```no_run
/// use std::num::NonZeroU8;
/// use falkordb::ConnectionStrategy;
///
/// // One shared multiplexed socket carrying many concurrent commands.
/// let strategy = ConnectionStrategy::Multiplexed {
///     connections: NonZeroU8::new(1).unwrap(),
/// };
/// assert_eq!(strategy.connection_count().get(), 1);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionStrategy {
    /// A fixed pool of independent connections, each used by exactly one command at a
    /// time (borrow/return). Provides strict per-command isolation and a natural cap on
    /// the number of in-flight commands. This is the only strategy for the sync client.
    Pooled {
        /// Number of independent connections held in the pool.
        size: NonZeroU8,
    },

    /// A small number of shared, cloneable, auto-reconnecting connections. Commands are
    /// round-robined across them and pipelined concurrently over each socket, so a
    /// single connection can carry many in-flight commands at once. Default for the
    /// asynchronous client.
    Multiplexed {
        /// Number of underlying multiplexed sockets to spread commands across.
        connections: NonZeroU8,
    },
}

impl ConnectionStrategy {
    /// The number of underlying connections this strategy maintains (pool size or
    /// number of multiplexed sockets).
    pub fn connection_count(&self) -> NonZeroU8 {
        match self {
            ConnectionStrategy::Pooled { size } => *size,
            ConnectionStrategy::Multiplexed { connections } => *connections,
        }
    }

    /// Returns a copy of this strategy with its connection count replaced. Used by the
    /// builder so `with_num_connections` can update whichever strategy is active.
    pub(crate) fn with_connection_count(
        self,
        count: NonZeroU8,
    ) -> Self {
        match self {
            ConnectionStrategy::Pooled { .. } => ConnectionStrategy::Pooled { size: count },
            ConnectionStrategy::Multiplexed { .. } => {
                ConnectionStrategy::Multiplexed { connections: count }
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum FalkorClientProvider {
    #[cfg(test)]
    None,

    Redis {
        client: redis::Client,
        sentinel: Option<redis::sentinel::SentinelClient>,
        /// Replica-typed Sentinel client used to route read-only queries away from
        /// the primary. It is set whenever a Sentinel master is detected during
        /// construction, so its presence indicates a Sentinel deployment — not that
        /// readable replicas actually exist or are currently reachable. `None` when
        /// the deployment is not Sentinel-managed. Because replica reachability is
        /// only determined when a connection is established, read-only pool creation
        /// uses replica-only connection getters (no primary fallback) so the pool is
        /// only built when a replica connection actually succeeds.
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

    /// Returns a replica-routed connection without fallback. This is used for
    /// building and maintaining the dedicated read-only pool so that it never
    /// gets populated with primary connections.
    pub(crate) fn get_replica_connection(&mut self) -> FalkorResult<FalkorSyncConnection> {
        match self {
            FalkorClientProvider::Redis {
                sentinel_replica: Some(replica),
                ..
            } => Ok(FalkorSyncConnection::Redis(
                replica
                    .get_connection()
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
            )),
            _ => Err(FalkorDBError::UnavailableProvider),
        }
    }

    /// Async counterpart of [`get_replica_connection`](Self::get_replica_connection).
    /// Returns a replica-routed connection without falling back to primary.
    #[cfg(feature = "tokio")]
    pub(crate) async fn get_async_replica_connection(
        &mut self
    ) -> FalkorResult<FalkorAsyncConnection> {
        match self {
            FalkorClientProvider::Redis {
                sentinel_replica: Some(replica),
                ..
            } => Ok(FalkorAsyncConnection::Redis(
                replica
                    .get_async_connection()
                    .await
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
            )),
            _ => Err(FalkorDBError::UnavailableProvider),
        }
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

    /// Whether this provider is managed by Redis Sentinel (its primary is resolved via a
    /// Sentinel client). Multiplexed connections built from a Sentinel-resolved client
    /// pin to a single node and do not re-resolve on failover, so the async client falls
    /// back to the pooled strategy for Sentinel deployments.
    #[cfg(feature = "tokio")]
    pub(crate) fn has_sentinel(&self) -> bool {
        matches!(
            self,
            FalkorClientProvider::Redis {
                sentinel: Some(_),
                ..
            }
        )
    }

    /// Build an auto-reconnecting, multiplexed [`ConnectionManager`](redis::aio::ConnectionManager)
    /// for the primary node. Sentinel deployments resolve the current master via the
    /// Sentinel client; direct deployments use the configured client. `max_inflight`,
    /// when set, bounds the number of concurrently in-flight commands per socket.
    #[cfg(feature = "tokio")]
    pub(crate) async fn get_async_connection_manager(
        &mut self,
        max_inflight: Option<NonZeroUsize>,
    ) -> FalkorResult<FalkorAsyncConnection> {
        let client = match self {
            FalkorClientProvider::Redis {
                sentinel: Some(sentinel),
                ..
            } => sentinel
                .async_get_client()
                .await
                .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
            FalkorClientProvider::Redis { client, .. } => client.clone(),
            #[cfg(test)]
            FalkorClientProvider::None => return Err(FalkorDBError::UnavailableProvider),
        };
        Self::manager_from_client(client, max_inflight).await
    }

    /// Replica-routed counterpart of
    /// [`get_async_connection_manager`](Self::get_async_connection_manager). Returns a
    /// multiplexed manager pinned to a replica node, without falling back to the primary.
    #[cfg(feature = "tokio")]
    pub(crate) async fn get_async_replica_connection_manager(
        &mut self,
        max_inflight: Option<NonZeroUsize>,
    ) -> FalkorResult<FalkorAsyncConnection> {
        match self {
            FalkorClientProvider::Redis {
                sentinel_replica: Some(replica),
                ..
            } => {
                let client = replica
                    .async_get_client()
                    .await
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?;
                Self::manager_from_client(client, max_inflight).await
            }
            _ => Err(FalkorDBError::UnavailableProvider),
        }
    }

    #[cfg(feature = "tokio")]
    async fn manager_from_client(
        client: redis::Client,
        max_inflight: Option<NonZeroUsize>,
    ) -> FalkorResult<FalkorAsyncConnection> {
        let manager = match max_inflight {
            Some(limit) => {
                let config =
                    redis::aio::ConnectionManagerConfig::new().set_concurrency_limit(limit.get());
                redis::aio::ConnectionManager::new_with_config(client, config).await
            }
            None => redis::aio::ConnectionManager::new(client).await,
        }
        .map_err(|err| FalkorDBError::RedisError(err.to_string()))?;
        Ok(FalkorAsyncConnection::Managed(manager))
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
    fn test_get_replica_connection_errors_without_replica() {
        // Without a replica Sentinel, get_replica_connection does not fall back to
        // the primary; it surfaces UnavailableProvider so the read-only pool is
        // never populated with primary connections.
        let mut provider = FalkorClientProvider::None;
        assert!(!provider.has_sentinel_replica());
        let result = provider.get_replica_connection();
        assert!(matches!(result, Err(FalkorDBError::UnavailableProvider)));
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
        assert!(matches!(result, Err(FalkorDBError::SentinelMastersCount)));
    }

    /// A single `SENTINEL MASTERS` master entry exposing the given `name`, in the
    /// alternating key/value layout the parser expects.
    fn single_master_reply(name: &str) -> Vec<redis::Value> {
        vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"name".to_vec()),
            redis::Value::BulkString(name.as_bytes().to_vec()),
        ])]
    }

    #[test]
    fn test_build_sentinel_client_happy_path() {
        // A well-formed single-master reply must yield a built master SentinelClient.
        let provider = FalkorClientProvider::None;
        let connection_info = redis::ConnectionInfo::from_str("redis://127.0.0.1:6379").unwrap();
        let client = provider
            .get_sentinel_client_common(&connection_info, single_master_reply("mymaster"))
            .expect("master client should build");
        assert!(client.is_some());
    }

    #[test]
    fn test_build_sentinel_client_missing_name() {
        // A master entry without a `name` field must surface SentinelMastersCount.
        let provider = FalkorClientProvider::None;
        let connection_info = redis::ConnectionInfo::from_str("redis://127.0.0.1:6379").unwrap();
        let reply = vec![redis::Value::Array(vec![
            redis::Value::BulkString(b"ip".to_vec()),
            redis::Value::BulkString(b"127.0.0.1".to_vec()),
        ])];
        let result = provider.get_sentinel_client_common(&connection_info, reply);
        assert!(matches!(result, Err(FalkorDBError::SentinelMastersCount)));
    }

    #[test]
    fn test_build_sentinel_clients_master_and_replica() {
        // A single-master reply must build both the master client and the optional
        // replica client used to route read-only queries.
        let provider = FalkorClientProvider::None;
        let connection_info = redis::ConnectionInfo::from_str("redis://127.0.0.1:6379").unwrap();
        let clients = provider
            .build_sentinel_clients(&connection_info, single_master_reply("mymaster"))
            .expect("clients should build")
            .expect("a Sentinel reply yields clients");
        assert!(clients.replica.is_some());
    }

    #[test]
    fn test_build_sentinel_clients_invalid_count() {
        let provider = FalkorClientProvider::None;
        let connection_info = redis::ConnectionInfo::from_str("redis://127.0.0.1:6379").unwrap();
        let result = provider.build_sentinel_clients(&connection_info, vec![]);
        assert!(matches!(result, Err(FalkorDBError::SentinelMastersCount)));
    }

    #[test]
    fn test_set_sentinel_replica_on_redis_provider() {
        // Setting a replica Sentinel on a real Redis provider stores it, so
        // has_sentinel_replica then reports true.
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let mut provider = FalkorClientProvider::Redis {
            client,
            sentinel: None,
            sentinel_replica: None,
            #[cfg(feature = "embedded")]
            embedded_server: None,
        };
        assert!(!provider.has_sentinel_replica());
        let connection_info = redis::ConnectionInfo::from_str("redis://127.0.0.1:26379").unwrap();
        let replica = redis::sentinel::SentinelClient::build(
            vec![connection_info],
            "mymaster".to_string(),
            None,
            redis::sentinel::SentinelServerType::Replica,
        )
        .unwrap();
        provider.set_sentinel_replica(replica);
        assert!(provider.has_sentinel_replica());
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
    fn test_get_replica_connection_errors_when_replica_unreachable() {
        // When a replica SentinelClient exists but the replica is unreachable, the
        // implementation must propagate the error rather than fall back to the
        // primary, so the read-only pool never receives primary connections.
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
        // The replica connection fails and the error must surface as a replica-path
        // RedisError; the call must not fall back to the primary.
        let result = provider.get_replica_connection();
        assert!(
            matches!(result, Err(FalkorDBError::RedisError(_))),
            "error should come from the replica path"
        );
    }

    #[test]
    #[cfg(feature = "tokio")]
    fn test_get_async_replica_connection_errors_when_replica_unreachable() {
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
            let result = provider.get_async_replica_connection().await;
            assert!(
                matches!(result, Err(FalkorDBError::RedisError(_))),
                "error should come from the replica path"
            );
        });
    }

    #[test]
    fn test_get_replica_connection_on_redis_provider_without_replica_does_not_use_primary() {
        // A real Redis provider with a reachable primary but no replica Sentinel must
        // still return `UnavailableProvider` — never a primary connection. This catches
        // a reintroduced fallback even when the primary URL happens to be reachable.
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let mut provider = FalkorClientProvider::Redis {
            client,
            sentinel: None,
            sentinel_replica: None,
            #[cfg(feature = "embedded")]
            embedded_server: None,
        };
        let result = provider.get_replica_connection();
        assert!(matches!(result, Err(FalkorDBError::UnavailableProvider)));
    }

    #[test]
    #[cfg(feature = "tokio")]
    fn test_get_async_replica_connection_on_redis_provider_without_replica_does_not_use_primary() {
        use tokio::runtime::Runtime;
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
            let mut provider = FalkorClientProvider::Redis {
                client,
                sentinel: None,
                sentinel_replica: None,
                #[cfg(feature = "embedded")]
                embedded_server: None,
            };
            let result = provider.get_async_replica_connection().await;
            assert!(matches!(result, Err(FalkorDBError::UnavailableProvider)));
        });
    }

    #[test]
    #[cfg(feature = "tokio")]
    fn test_get_async_connection_manager_none_provider_is_unavailable() {
        use tokio::runtime::Runtime;
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut provider = FalkorClientProvider::None;
            let result = provider.get_async_connection_manager(None).await;
            assert!(matches!(result, Err(FalkorDBError::UnavailableProvider)));
        });
    }

    #[test]
    #[cfg(feature = "tokio")]
    fn test_get_async_connection_manager_routes_through_sentinel() {
        use tokio::runtime::Runtime;
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
            let connection_info = redis::ConnectionInfo::from_str("redis://127.0.0.1:1").unwrap();
            let sentinel = redis::sentinel::SentinelClient::build(
                vec![connection_info],
                "mymaster".to_string(),
                None,
                redis::sentinel::SentinelServerType::Master,
            )
            .unwrap();
            let mut provider = FalkorClientProvider::Redis {
                client,
                sentinel: Some(sentinel),
                sentinel_replica: None,
                #[cfg(feature = "embedded")]
                embedded_server: None,
            };
            // The sentinel arm resolves the master through an unreachable sentinel, which
            // fails fast with a RedisError rather than bypassing it.
            let result = provider.get_async_connection_manager(None).await;
            assert!(
                matches!(result, Err(FalkorDBError::RedisError(_))),
                "error should come from the sentinel resolution path"
            );
        });
    }

    #[test]
    #[cfg(feature = "tokio")]
    fn test_get_async_replica_connection_manager_errors_when_replica_unreachable() {
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
            let result = provider.get_async_replica_connection_manager(None).await;
            assert!(
                matches!(result, Err(FalkorDBError::RedisError(_))),
                "error should come from the replica manager path"
            );
        });
    }

    #[test]
    #[cfg(feature = "tokio")]
    fn test_get_async_replica_connection_manager_without_replica_does_not_use_primary() {
        use tokio::runtime::Runtime;
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
            let mut provider = FalkorClientProvider::Redis {
                client,
                sentinel: None,
                sentinel_replica: None,
                #[cfg(feature = "embedded")]
                embedded_server: None,
            };
            let result = provider.get_async_replica_connection_manager(None).await;
            assert!(matches!(result, Err(FalkorDBError::UnavailableProvider)));
        });
    }
}
