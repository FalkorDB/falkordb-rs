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
    /// distributed round-robin across them and pipelined concurrently over each socket, so a
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

/// Controls which node a read-only query may be served from.
///
/// A FalkorDB replica applies writes only **after** the primary has committed them, so a read
/// served from a replica can return slightly **stale** data. Routing reads to replicas is therefore
/// opt-in: the default ([`ReadPreference::Primary`]) keeps every read on the primary, so you never
/// observe replication lag unless you ask for it.
///
/// This is orthogonal to the read-only *command* (`GRAPH.RO_QUERY`, issued by
/// [`ro_query`](crate::SyncGraph::ro_query) / [`call_procedure_ro`](crate::SyncGraph::call_procedure_ro)):
/// the command decides whether the server allows writes, while `ReadPreference` decides which node
/// answers. A read preference only takes effect for read-only commands.
///
/// Set a client-wide default with
/// [`FalkorClientBuilder::with_read_preference`](crate::FalkorClientBuilder::with_read_preference),
/// and override it per query with
/// [`QueryBuilder::with_read_preference`](crate::QueryBuilder::with_read_preference) (or the
/// [`prefer_replica`](crate::QueryBuilder::prefer_replica) /
/// [`primary_only`](crate::QueryBuilder::primary_only) shortcuts).
///
/// # Example
/// ```no_run
/// use falkordb::ReadPreference;
///
/// assert_eq!(ReadPreference::default(), ReadPreference::Primary);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadPreference {
    /// Serve reads from the primary only — never subject to replica lag. **Default.**
    #[default]
    Primary,
    /// Prefer a replica when one is available, falling back to the primary otherwise. Accepts that
    /// reads may be slightly stale in exchange for offloading the primary.
    PreferReplica,
}

/// Where the next Sentinel replica lookup starts in the list of eligible replicas.
static REPLICA_ROTATION: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// redis-rs 1.7.1's timed native-TLS connect runs the handshake on a socket with no deadline and
/// `unwrap`s its result, so a failing peer panics and a silent one blocks forever. Rustls
/// handshakes lazily under the read timeout redis-rs sets during setup, so only native-TLS-only
/// builds need this.
#[cfg(all(
    any(feature = "native-tls", feature = "tokio-native-tls"),
    not(any(feature = "rustls", feature = "tokio-rustls"))
))]
mod native_tls_connect {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc, Mutex, OnceLock};
    use std::time::Duration;

    /// Most in-flight or abandoned attempts per `(host, port)` target. Each stalled attempt
    /// holds a helper thread and a socket until its peer answers or closes, so past this per-peer
    /// limit, new attempts to that peer fail fast without affecting healthy peers.
    pub(super) const MAX_IN_FLIGHT_PER_TARGET: usize = 16;

    static TARGET_COUNTS: OnceLock<Mutex<HashMap<(String, u16), Arc<AtomicUsize>>>> =
        OnceLock::new();

    pub(super) fn counter_for(client: &redis::Client) -> Arc<AtomicUsize> {
        let key = match client.get_connection_info().addr() {
            redis::ConnectionAddr::Tcp(host, port)
            | redis::ConnectionAddr::TcpTls { host, port, .. } => (host.clone(), *port),
            other => (other.to_string(), 0),
        };
        let mut map = TARGET_COUNTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            map.entry(key)
                .or_insert_with(|| Arc::new(AtomicUsize::new(0))),
        )
    }

    struct SlotGuard(Arc<AtomicUsize>);

    impl Drop for SlotGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn try_reserve(
        counter: &Arc<AtomicUsize>,
        max_in_flight: usize,
    ) -> Option<SlotGuard> {
        let mut cur = counter.load(Ordering::Acquire);
        loop {
            if cur >= max_in_flight {
                return None;
            }
            match counter.compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Some(SlotGuard(Arc::clone(counter))),
                Err(actual) => cur = actual,
            }
        }
    }

    fn timed_out(msg: &str) -> redis::RedisError {
        std::io::Error::new(std::io::ErrorKind::TimedOut, msg.to_string()).into()
    }

    /// Connect `client` on a helper thread and return within `timeout`, turning the handshake
    /// panic into an error. Capacity is reserved atomically in `in_flight` before spawning and
    /// released when the worker exits; while `max_in_flight` workers for the target are running
    /// or stalled, no new thread is spawned.
    pub(super) fn connect(
        client: &redis::Client,
        timeout: Duration,
        in_flight: &Arc<AtomicUsize>,
        max_in_flight: usize,
    ) -> redis::RedisResult<redis::Connection> {
        let Some(slot) = try_reserve(in_flight, max_in_flight) else {
            return Err(timed_out(
                "too many earlier TLS connection attempts are still stalled",
            ));
        };

        let client = client.clone();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _slot = slot;
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                client.get_connection_with_timeout(timeout)
            }))
            .unwrap_or_else(|payload| {
                let detail = payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("TLS handshake failed")
                    .to_string();
                Err(redis::RedisError::from((
                    redis::ErrorKind::Io,
                    "SSL Handshake error",
                    detail,
                )))
            });
            // If the caller already gave up, the send fails and the connection is dropped here.
            let _ = tx.send(res);
        });

        rx.recv_timeout(timeout)
            .unwrap_or_else(|_| Err(timed_out("TLS connection setup timed out")))
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
        #[cfg(feature = "embedded-core")]
        #[allow(dead_code)]
        embedded_server: Option<std::sync::Arc<crate::embedded::EmbeddedServer>>,
        /// Client-side response timeout applied to sync and async connections. `None`
        /// (the default) disables the client-side deadline entirely, letting the server's
        /// own `TIMEOUT`/`TIMEOUT_DEFAULT` configuration govern query duration. This
        /// overrides redis-rs 1.x's default 500ms async response timeout, which is far too
        /// short for typical graph queries (e.g. `LOAD CSV`, deep traversals) and
        /// would otherwise surface as spurious connection errors while the server
        /// keeps executing the query.
        response_timeout: Option<std::time::Duration>,
        /// Deadline for everything that must finish before a connection is usable:
        /// establishing it, and the one-off topology probe
        /// [`get_sentinel_client`](FalkorClientProvider::get_sentinel_client) /
        /// [`get_sentinel_client_async`](FalkorClientProvider::get_sentinel_client_async)
        /// runs while the client is being built. It deliberately does not bound query
        /// responses — that is `response_timeout`'s job — so a caller can demand quick
        /// answers without making their client impossible to construct.
        connect_timeout: Option<std::time::Duration>,
    },
}

impl FalkorClientProvider {
    /// Open a synchronous connection from `client`, honoring `connect_timeout` during
    /// establishment and applying `response_timeout` as the socket read/write deadline
    /// once setup finishes.
    fn open_sync_connection(
        client: &redis::Client,
        response_timeout: Option<std::time::Duration>,
        connect_timeout: Option<std::time::Duration>,
    ) -> FalkorResult<FalkorSyncConnection> {
        Self::connect_redis_client(client, connect_timeout)
            .and_then(|conn| {
                conn.set_read_timeout(response_timeout)
                    .and_then(|()| conn.set_write_timeout(response_timeout))
                    .map(|()| FalkorSyncConnection::Redis(conn))
            })
            .map_err(|err| FalkorDBError::RedisError(err.to_string()))
    }

    /// Connect `client` with an optional setup deadline while avoiding redis-rs 1.7.1's
    /// `tls_connector.connect(...).unwrap()` panic and unbounded TLS handshake on the
    /// timed `native-tls` (without `rustls`) code path.
    fn connect_redis_client(
        client: &redis::Client,
        connect_timeout: Option<std::time::Duration>,
    ) -> redis::RedisResult<redis::Connection> {
        match connect_timeout {
            #[cfg(all(
                any(feature = "native-tls", feature = "tokio-native-tls"),
                not(any(feature = "rustls", feature = "tokio-rustls"))
            ))]
            Some(timeout)
                if matches!(
                    client.get_connection_info().addr(),
                    redis::ConnectionAddr::TcpTls { .. }
                ) =>
            {
                let counter = native_tls_connect::counter_for(client);
                native_tls_connect::connect(
                    client,
                    timeout,
                    &counter,
                    native_tls_connect::MAX_IN_FLIGHT_PER_TARGET,
                )
            }
            Some(timeout) => client.get_connection_with_timeout(timeout),
            None => client.get_connection(),
        }
    }

    /// Open a synchronous connection and bound any follow-up probe commands on it by
    /// `connect_timeout`.
    fn open_probe_connection(
        connection_info: &redis::ConnectionInfo,
        connect_timeout: Option<std::time::Duration>,
    ) -> FalkorResult<redis::Connection> {
        redis::Client::open(connection_info.clone())
            .and_then(|client| Self::connect_redis_client(&client, connect_timeout))
            .and_then(|conn| {
                conn.set_read_timeout(connect_timeout)
                    .and_then(|()| conn.set_write_timeout(connect_timeout))
                    .map(|()| conn)
            })
            .map_err(|err| FalkorDBError::RedisError(err.to_string()))
    }

    /// Build a node `redis::ConnectionInfo` for a Sentinel-discovered `(ip, port)` using
    /// the Sentinel's Redis/TCP/TLS settings.
    fn sentinel_node_connection_info(
        sentinel_info: &redis::ConnectionInfo,
        ip: String,
        port: u16,
    ) -> redis::ConnectionInfo {
        let mut addr = sentinel_info.addr().clone();
        match &mut addr {
            redis::ConnectionAddr::Tcp(host, p)
            | redis::ConnectionAddr::TcpTls { host, port: p, .. } => {
                *host = ip;
                *p = port;
            }
            _ => addr = redis::ConnectionAddr::Tcp(ip, port),
        }
        sentinel_info.clone().set_addr(addr)
    }

    /// Parse a `SENTINEL MASTERS` or `SENTINEL SLAVES` array reply into key/value maps.
    fn parse_sentinel_maps(entries: Vec<redis::Value>) -> Vec<HashMap<String, String>> {
        entries
            .into_iter()
            .filter_map(|entry| entry.into_sequence().ok())
            .map(|seq| {
                seq.chunks_exact(2)
                    .filter_map(|chunk| {
                        redis_value_as_string(chunk[0].clone())
                            .and_then(|k| redis_value_as_string(chunk[1].clone()).map(|v| (k, v)))
                            .ok()
                    })
                    .collect()
            })
            .collect()
    }

    /// Query a Redis command on `conn`, mapping any error into [`FalkorDBError::RedisError`].
    fn query_cmd<T: redis::FromRedisValue>(
        cmd: &redis::Cmd,
        conn: &mut redis::Connection,
    ) -> FalkorResult<T> {
        cmd.query(conn)
            .map_err(|err| FalkorDBError::RedisError(err.to_string()))
    }

    /// Check whether a candidate node reports the expected `expected_role` (`"master"` or
    /// `"slave"`) via `ROLE`, with both connection establishment and the role query
    /// bounded by `connect_timeout`.
    ///
    /// Like redis-rs's own resolver, a node whose server rejects `ROLE` (renamed, or denied by
    /// an ACL) is asked for the `role:` field of `INFO REPLICATION` instead. Transport failures
    /// do not fall back, since the connection may be left mid-reply.
    fn node_matches_role(
        node_info: &redis::ConnectionInfo,
        expected_role: &str,
        connect_timeout: Option<std::time::Duration>,
    ) -> FalkorResult<bool> {
        let mut conn = Self::open_probe_connection(node_info, connect_timeout)?;
        match redis::cmd("ROLE").query::<Vec<redis::Value>>(&mut conn) {
            Ok(items) => Ok(items
                .into_iter()
                .next()
                .and_then(|v| redis_value_as_string(v).ok())
                .as_deref()
                == Some(expected_role)),
            Err(role_err)
                if matches!(
                    role_err.kind(),
                    redis::ErrorKind::Server(_) | redis::ErrorKind::Extension
                ) =>
            {
                let info: String = redis::cmd("INFO")
                    .arg("REPLICATION")
                    .query(&mut conn)
                    .map_err(|_| FalkorDBError::RedisError(role_err.to_string()))?;
                Ok(info
                    .lines()
                    .find_map(|line| line.trim_end().strip_prefix("role:"))
                    == Some(expected_role))
            }
            Err(err) => Err(FalkorDBError::RedisError(err.to_string())),
        }
    }

    /// Resolve a `redis::Client` for the current Sentinel master (`is_replica == false`)
    /// or replica (`is_replica == true`) while bounding every Sentinel query and
    /// candidate-node `ROLE` validation by `connect_timeout`.
    fn resolve_sentinel_client_with_timeout(
        sentinel_info: &redis::ConnectionInfo,
        is_replica: bool,
        connect_timeout: Option<std::time::Duration>,
    ) -> FalkorResult<redis::Client> {
        let mut sentinel_conn = Self::open_probe_connection(sentinel_info, connect_timeout)?;
        let raw_masters: Vec<redis::Value> =
            Self::query_cmd(redis::cmd("SENTINEL").arg("MASTERS"), &mut sentinel_conn)?;
        if raw_masters.len() > 1 {
            return Err(FalkorDBError::SentinelMastersCount);
        }
        let masters = Self::parse_sentinel_maps(raw_masters);

        let (candidates, expected_role, not_found_msg) = if is_replica {
            let service_name = masters
                .first()
                .and_then(|m| m.get("name"))
                .cloned()
                .ok_or_else(|| {
                    FalkorDBError::RedisError(
                        "No valid master found in sentinel for replica lookup".to_string(),
                    )
                })?;
            let raw_replicas: Vec<redis::Value> = Self::query_cmd(
                redis::cmd("SENTINEL").arg("SLAVES").arg(&service_name),
                &mut sentinel_conn,
            )?;
            (
                Self::parse_sentinel_maps(raw_replicas),
                "slave",
                "No valid replica found in sentinel for given name",
            )
        } else {
            (
                masters,
                "master",
                "Master with given name not found in sentinel",
            )
        };

        // Same eligibility as redis-rs: flags must be present and not down, and an advertised
        // master must also carry the `master` flag.
        let mut addresses: Vec<(String, u16)> = candidates
            .into_iter()
            .filter(|candidate| {
                candidate.get("flags").is_some_and(|flags| {
                    (is_replica || flags.contains("master"))
                        && !flags.contains("s_down")
                        && !flags.contains("o_down")
                })
            })
            .filter_map(|candidate| {
                candidate
                    .get("ip")
                    .cloned()
                    .zip(candidate.get("port").and_then(|p| p.parse::<u16>().ok()))
            })
            .collect();
        // Start each replica lookup one further along the list, so a read-only pool that
        // resolves once per connection spreads across replicas instead of piling onto the first.
        if is_replica && !addresses.is_empty() {
            let start = REPLICA_ROTATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let len = addresses.len();
            addresses.rotate_left(start % len);
        }

        let mut last_probe_err = None;
        for (ip, port) in addresses {
            let node_info = Self::sentinel_node_connection_info(sentinel_info, ip, port);
            match Self::node_matches_role(&node_info, expected_role, connect_timeout) {
                Ok(true) => {
                    return Ok(redis::Client::open(node_info).expect("valid Redis ConnectionInfo"));
                }
                Ok(false) => continue,
                Err(err) => last_probe_err = Some(err),
            }
        }
        Err(last_probe_err.unwrap_or_else(|| FalkorDBError::RedisError(not_found_msg.to_string())))
    }

    pub(crate) fn get_connection(&mut self) -> FalkorResult<FalkorSyncConnection> {
        let response_timeout = self.response_timeout();
        let connect_timeout = self.connect_timeout();
        self.get_connection_with_timeouts(response_timeout, connect_timeout)
    }

    /// [`get_connection`](Self::get_connection) with an explicit response timeout, so
    /// callers that are not executing user queries (such as the one-off Sentinel probe
    /// during client construction) can opt out of the query deadline.
    fn get_connection_with_timeouts(
        &mut self,
        response_timeout: Option<std::time::Duration>,
        connect_timeout: Option<std::time::Duration>,
    ) -> FalkorResult<FalkorSyncConnection> {
        let client = match self {
            FalkorClientProvider::Redis {
                client: sentinel_client,
                sentinel: Some(_),
                ..
            } => Self::resolve_sentinel_client_with_timeout(
                sentinel_client.get_connection_info(),
                false,
                connect_timeout,
            )?,
            FalkorClientProvider::Redis { client, .. } => client.clone(),
            #[cfg(test)]
            FalkorClientProvider::None => return Err(FalkorDBError::UnavailableProvider),
        };
        Self::open_sync_connection(&client, response_timeout, connect_timeout)
    }

    /// Async connection config carrying the provider's timeouts. Both are always set
    /// explicitly (even when `None`) so redis-rs 1.x's default 500ms response timeout
    /// never applies.
    #[cfg(feature = "tokio")]
    fn async_connection_config(
        response_timeout: Option<std::time::Duration>,
        connect_timeout: Option<std::time::Duration>,
    ) -> redis::AsyncConnectionConfig {
        redis::AsyncConnectionConfig::new()
            .set_response_timeout(response_timeout)
            .set_connection_timeout(connect_timeout)
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn get_async_connection(&mut self) -> FalkorResult<FalkorAsyncConnection> {
        let response_timeout = self.response_timeout();
        let connect_timeout = self.connect_timeout();
        self.get_async_connection_with_timeouts(response_timeout, connect_timeout)
            .await
    }

    /// [`get_async_connection`](Self::get_async_connection) with an explicit response
    /// timeout, so callers that are not executing user queries can opt out of the
    /// configured deadline. See [`get_sentinel_client_async`](Self::get_sentinel_client_async).
    #[cfg(feature = "tokio")]
    async fn get_async_connection_with_timeouts(
        &mut self,
        response_timeout: Option<std::time::Duration>,
        connect_timeout: Option<std::time::Duration>,
    ) -> FalkorResult<FalkorAsyncConnection> {
        Ok(match self {
            FalkorClientProvider::Redis {
                sentinel: Some(sentinel),
                ..
            } => FalkorAsyncConnection::Redis(
                sentinel
                    .get_async_connection_with_config(&Self::async_connection_config(
                        response_timeout,
                        connect_timeout,
                    ))
                    .await
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
            ),
            FalkorClientProvider::Redis { client, .. } => FalkorAsyncConnection::Redis(
                client
                    .get_multiplexed_async_connection_with_config(&Self::async_connection_config(
                        response_timeout,
                        connect_timeout,
                    ))
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
        let response_timeout = self.response_timeout();
        let connect_timeout = self.connect_timeout();
        match self {
            FalkorClientProvider::Redis {
                client: sentinel_client,
                sentinel_replica: Some(_),
                ..
            } => {
                let client = Self::resolve_sentinel_client_with_timeout(
                    sentinel_client.get_connection_info(),
                    true,
                    connect_timeout,
                )?;
                Self::open_sync_connection(&client, response_timeout, connect_timeout)
            }
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
                response_timeout,
                connect_timeout,
                ..
            } => Ok(FalkorAsyncConnection::Redis(
                replica
                    .get_async_connection_with_config(&Self::async_connection_config(
                        *response_timeout,
                        *connect_timeout,
                    ))
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
        let response_timeout = self.response_timeout();
        let connect_timeout = self.connect_timeout();
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
        Self::manager_from_client(client, max_inflight, response_timeout, connect_timeout).await
    }

    /// Replica-routed counterpart of
    /// [`get_async_connection_manager`](Self::get_async_connection_manager). Returns a
    /// multiplexed manager pinned to a replica node, without falling back to the primary.
    #[cfg(feature = "tokio")]
    pub(crate) async fn get_async_replica_connection_manager(
        &mut self,
        max_inflight: Option<NonZeroUsize>,
    ) -> FalkorResult<FalkorAsyncConnection> {
        let response_timeout = self.response_timeout();
        let connect_timeout = self.connect_timeout();
        match self {
            FalkorClientProvider::Redis {
                sentinel_replica: Some(replica),
                ..
            } => {
                let client = replica
                    .async_get_client()
                    .await
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?;
                Self::manager_from_client(client, max_inflight, response_timeout, connect_timeout)
                    .await
            }
            _ => Err(FalkorDBError::UnavailableProvider),
        }
    }

    /// The configured client-side response timeout (`None` disables it).
    fn response_timeout(&self) -> Option<std::time::Duration> {
        match self {
            FalkorClientProvider::Redis {
                response_timeout, ..
            } => *response_timeout,
            #[cfg(test)]
            FalkorClientProvider::None => None,
        }
    }

    /// The configured deadline for connection setup (`None` disables it).
    fn connect_timeout(&self) -> Option<std::time::Duration> {
        match self {
            FalkorClientProvider::Redis {
                connect_timeout, ..
            } => *connect_timeout,
            #[cfg(test)]
            FalkorClientProvider::None => None,
        }
    }

    #[cfg(feature = "tokio")]
    async fn manager_from_client(
        client: redis::Client,
        max_inflight: Option<NonZeroUsize>,
        response_timeout: Option<std::time::Duration>,
        connect_timeout: Option<std::time::Duration>,
    ) -> FalkorResult<FalkorAsyncConnection> {
        // Always set the response timeout explicitly (even when `None`) so redis-rs
        // 1.x's default 500ms response timeout never applies. The connect timeout bounds
        // each (re)connection attempt the manager makes, never a query's reply.
        let config = redis::aio::ConnectionManagerConfig::new()
            .set_response_timeout(response_timeout)
            .set_connection_timeout(connect_timeout);
        let config = match max_inflight {
            Some(limit) => config.set_concurrency_limit(limit.get()),
            None => config,
        };
        let manager = redis::aio::ConnectionManager::new_with_config(client, config)
            .await
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
                        .set_redis_connection_info(connection_info.redis_settings().clone())
                        .set_tcp_settings(connection_info.tcp_settings().clone());
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
        // Topology detection runs while the client is still being built, so bound it by
        // `connect_timeout` rather than `response_timeout` (matching the async probe).
        let connect_timeout = self.connect_timeout();
        let mut conn = self.get_connection_with_timeouts(connect_timeout, connect_timeout)?;
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
        // Topology detection runs while the client is still being built, so it is bounded by
        // the connect timeout rather than the response timeout: the latter expresses "a query
        // must answer within X", not "the client must finish connecting within X". Applying
        // the response timeout here made `FalkorClientBuilder::build` fail with a connection
        // error whenever the handshake or this `INFO` happened to be slower than a tight
        // timeout — the client could not even be constructed.
        let connect_timeout = self.connect_timeout();
        let mut conn = self
            .get_async_connection_with_timeouts(connect_timeout, connect_timeout)
            .await?;
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

    /// Start a minimal RESP server on a background thread and return the URL to reach it.
    /// It answers every command immediately except `INFO`, which it holds back for
    /// `info_delay`, so a test can pin down which timeout a code path is subject to without
    /// needing a real server. One connection — all the Sentinel probe opens — is served.
    fn spawn_slow_info_server(info_delay: std::time::Duration) -> String {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
        let port = listener
            .local_addr()
            .expect("read the bound address")
            .port();

        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept the probe's connection");
            let mut pending = String::new();
            let mut chunk = [0u8; 1024];

            while let Ok(read) = stream.read(&mut chunk) {
                if read == 0 {
                    break;
                }

                // Commands are ASCII here, so decoding each read is safe even though a read
                // may land mid-command; `take_commands` is what re-establishes the framing.
                pending.push_str(&String::from_utf8_lossy(&chunk[..read]));

                for command in take_commands(&mut pending) {
                    // Match `INFO` as a whole argument: `SETINFO`, sent by the handshake,
                    // must neither match nor be delayed.
                    let reply = if command.contains("\r\nINFO\r\n") {
                        std::thread::sleep(info_delay);
                        let body = "# Server\r\nredis_mode:standalone\r\n";
                        format!("${}\r\n{body}\r\n", body.len())
                    } else {
                        "+OK\r\n".to_string()
                    };

                    // A deliberately slow reply may arrive after the client has given up and
                    // gone away, which is exactly what the connect-timeout test provokes.
                    let _ = stream.write_all(reply.as_bytes());
                }
            }
        });

        format!("redis://127.0.0.1:{port}")
    }

    /// Split every complete RESP command off the front of `pending`, leaving a partial one
    /// behind. TCP reads are not message boundaries: a read can carry several pipelined
    /// commands or half of one, and answering per read rather than per command would
    /// desynchronize the stream and stall whatever the server is standing in for.
    fn take_commands(pending: &mut String) -> Vec<String> {
        let mut commands = Vec::new();

        // A request is an array header (`*<argc>`) followed by `argc` bulk strings, each a
        // `$<len>` header and a payload line, so a whole command spans `1 + 2 * argc` lines.
        while let Some((header, _)) = pending.split_once("\r\n") {
            let argc: usize = header
                .strip_prefix('*')
                .expect("a RESP request starts with an array header")
                .parse()
                .expect("a RESP array header carries its argument count");

            let Some(end) = crlf_lines_end(pending, 1 + 2 * argc) else {
                break;
            };

            commands.push(pending.drain(..end).collect());
        }

        commands
    }

    /// The offset just past `lines` CRLF-terminated lines of `buf`, or `None` when it holds
    /// fewer than that.
    fn crlf_lines_end(
        buf: &str,
        lines: usize,
    ) -> Option<usize> {
        let mut end = 0;
        for _ in 0..lines {
            end += buf[end..].find("\r\n")? + 2;
        }

        Some(end)
    }

    /// Whatever the fake server stands in for, it must recover the command framing from a
    /// byte stream: reads carry pipelined commands, split commands, or both.
    #[test]
    fn test_take_commands_reframes_the_byte_stream() {
        let ping = "*1\r\n$4\r\nPING\r\n";
        let info = "*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n";

        let mut pending = String::new();
        assert!(
            take_commands(&mut pending).is_empty(),
            "an empty buffer holds no command"
        );

        pending.push_str(ping);
        pending.push_str(info);
        assert_eq!(
            take_commands(&mut pending),
            vec![ping.to_string(), info.to_string()],
            "pipelined commands are answered one by one"
        );
        assert!(pending.is_empty(), "nothing is left behind");

        let (head, tail) = info.split_at(12);
        pending.push_str(head);
        assert!(
            take_commands(&mut pending).is_empty(),
            "a partial command is held back rather than answered early"
        );

        pending.push_str(tail);
        assert_eq!(
            take_commands(&mut pending),
            vec![info.to_string()],
            "the command is answered once the rest of it arrives"
        );
    }

    /// The connect timeout is what bounds setup, so a server that accepts the connection and
    /// then goes silent cannot hang client construction, however generous — or absent — the
    /// query response timeout is.
    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sentinel_detection_is_bounded_by_the_connect_timeout() {
        let url = spawn_slow_info_server(std::time::Duration::from_secs(30));

        let mut provider = FalkorClientProvider::Redis {
            client: redis::Client::open(url.as_str()).expect("open a client for the fake server"),
            sentinel: None,
            sentinel_replica: None,
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            // No query deadline at all: setup is bounded on its own terms.
            response_timeout: None,
            connect_timeout: Some(std::time::Duration::from_millis(50)),
        };

        let connection_info =
            redis::ConnectionInfo::from_str(url.as_str()).expect("parse the fake server url");
        let probe = provider.get_sentinel_client_async(&connection_info).await;

        assert!(
            probe.is_err(),
            "a server that goes silent after the handshake must not hang client construction"
        );
    }

    /// Sentinel detection runs while the client is still being built, so it must not be cut
    /// off by the configured response timeout — that deadline bounds query responses, not
    /// client construction. Against a server whose `INFO` is far slower than the timeout,
    /// detection must still complete; before the fix this returned a connection error and
    /// `FalkorClientBuilder::build` failed outright.
    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sentinel_detection_is_not_bounded_by_the_response_timeout() {
        let url = spawn_slow_info_server(std::time::Duration::from_millis(250));

        let mut provider = FalkorClientProvider::Redis {
            client: redis::Client::open(url.as_str()).expect("open a client for the fake server"),
            sentinel: None,
            sentinel_replica: None,
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: Some(std::time::Duration::from_millis(5)),
            connect_timeout: Some(crate::DEFAULT_CONNECT_TIMEOUT),
        };

        let connection_info =
            redis::ConnectionInfo::from_str(url.as_str()).expect("parse the fake server url");
        let sentinels = provider
            .get_sentinel_client_async(&connection_info)
            .await
            .expect("detection must outlive the 5ms query response timeout");

        assert!(
            sentinels.is_none(),
            "the fake server reports redis_mode:standalone, so no Sentinel clients are built"
        );
    }

    #[test]
    fn test_sync_sentinel_detection_is_bounded_by_the_connect_timeout() {
        let url = spawn_slow_info_server(std::time::Duration::from_secs(30));

        let mut provider = FalkorClientProvider::Redis {
            client: redis::Client::open(url.as_str()).expect("open a client for the fake server"),
            sentinel: None,
            sentinel_replica: None,
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: None,
            connect_timeout: Some(std::time::Duration::from_millis(50)),
        };

        let connection_info =
            redis::ConnectionInfo::from_str(url.as_str()).expect("parse the fake server url");
        let probe = provider.get_sentinel_client(&connection_info);

        assert!(
            probe.is_err(),
            "a server that goes silent after the handshake must not hang sync client construction"
        );
    }

    #[test]
    fn test_sync_sentinel_detection_is_not_bounded_by_the_response_timeout() {
        let url = spawn_slow_info_server(std::time::Duration::from_millis(250));

        let mut provider = FalkorClientProvider::Redis {
            client: redis::Client::open(url.as_str()).expect("open a client for the fake server"),
            sentinel: None,
            sentinel_replica: None,
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: Some(std::time::Duration::from_millis(5)),
            connect_timeout: Some(crate::DEFAULT_CONNECT_TIMEOUT),
        };

        let connection_info =
            redis::ConnectionInfo::from_str(url.as_str()).expect("parse the fake server url");
        let sentinels = provider
            .get_sentinel_client(&connection_info)
            .expect("sync detection must outlive the 5ms query response timeout");

        assert!(
            sentinels.is_none(),
            "the fake server reports redis_mode:standalone, so no Sentinel clients are built"
        );
    }

    #[test]
    fn test_sync_get_connection_applies_response_timeout() {
        let url = spawn_slow_info_server(std::time::Duration::from_secs(30));

        let mut provider = FalkorClientProvider::Redis {
            client: redis::Client::open(url.as_str()).expect("open a client for the fake server"),
            sentinel: None,
            sentinel_replica: None,
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: Some(std::time::Duration::from_millis(50)),
            connect_timeout: Some(std::time::Duration::from_secs(2)),
        };

        let mut conn = provider
            .get_connection()
            .expect("connection establishment should succeed within connect_timeout");
        let start = std::time::Instant::now();
        let res = conn.execute_command(None, "INFO", None, None);
        assert!(
            res.is_err(),
            "slow command on sync connection must fail once response_timeout expires"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "command must time out promptly rather than waiting for the 30s server delay"
        );
    }

    /// Spawn a minimal fake Sentinel + master server pair so `SentinelClient::get_client()`
    /// resolves a node `redis::Client` whose `ConnectionInfo` can be inspected.
    fn spawn_fake_sentinel_master_resolver() -> String {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let master_listener = TcpListener::bind("127.0.0.1:0").expect("bind master port");
        let master_port = master_listener
            .local_addr()
            .expect("read master address")
            .port();

        std::thread::spawn(move || {
            for stream in master_listener.incoming().take(2).flatten() {
                let mut stream = stream;
                let mut pending = String::new();
                let mut chunk = [0u8; 1024];
                while let Ok(read) = stream.read(&mut chunk) {
                    if read == 0 {
                        break;
                    }
                    pending.push_str(&String::from_utf8_lossy(&chunk[..read]));
                    for command in take_commands(&mut pending) {
                        let reply = if command.to_ascii_uppercase().contains("\r\nROLE\r\n") {
                            "*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n"
                        } else {
                            "+OK\r\n"
                        };
                        let _ = stream.write_all(reply.as_bytes());
                    }
                }
            }
        });

        let sentinel_listener = TcpListener::bind("127.0.0.1:0").expect("bind sentinel port");
        let sentinel_port = sentinel_listener
            .local_addr()
            .expect("read sentinel address")
            .port();

        std::thread::spawn(move || {
            let (mut stream, _) = sentinel_listener
                .accept()
                .expect("accept sentinel connection");
            let mut pending = String::new();
            let mut chunk = [0u8; 1024];
            while let Ok(read) = stream.read(&mut chunk) {
                if read == 0 {
                    break;
                }
                pending.push_str(&String::from_utf8_lossy(&chunk[..read]));
                for command in take_commands(&mut pending) {
                    let upper = command.to_ascii_uppercase();
                    let reply = if upper.contains("\r\nSENTINEL\r\n")
                        && upper.contains("\r\nMASTERS\r\n")
                    {
                        let port_str = master_port.to_string();
                        format!(
                            "*1\r\n*8\r\n$4\r\nname\r\n$8\r\nmymaster\r\n$2\r\nip\r\n$9\r\n127.0.0.1\r\n$4\r\nport\r\n${}\r\n{port_str}\r\n$5\r\nflags\r\n$6\r\nmaster\r\n",
                            port_str.len()
                        )
                    } else if upper.contains("\r\nROLE\r\n") {
                        "*2\r\n$8\r\nsentinel\r\n*1\r\n$8\r\nmymaster\r\n".to_string()
                    } else {
                        "+OK\r\n".to_string()
                    };
                    let _ = stream.write_all(reply.as_bytes());
                }
            }
        });

        format!("redis://127.0.0.1:{sentinel_port}")
    }

    #[test]
    fn test_build_sentinel_client_forwards_tcp_settings_to_node_connections() {
        let sentinel_url = spawn_fake_sentinel_master_resolver();
        let settings = redis::io::tcp::TcpSettings::default()
            .set_nodelay(true)
            .set_keepalive(
                redis::io::tcp::socket2::TcpKeepalive::new()
                    .with_time(std::time::Duration::from_secs(42)),
            );
        let connection_info = redis::ConnectionInfo::from_str(&sentinel_url)
            .expect("parse sentinel url")
            .set_tcp_settings(settings);

        let provider = FalkorClientProvider::None;
        let mut sentinel_client = provider
            .get_sentinel_client_common(&connection_info, single_master_reply("mymaster"))
            .expect("sentinel client should build")
            .expect("single master yields a client");

        let resolved_node_client = sentinel_client
            .get_client()
            .expect("resolve master client from fake sentinel");
        let node_tcp = resolved_node_client.get_connection_info().tcp_settings();
        assert!(
            node_tcp.nodelay(),
            "Sentinel-resolved node client must inherit tcp_settings.nodelay"
        );
        assert!(
            node_tcp.keepalive().is_some(),
            "Sentinel-resolved node client must inherit tcp_settings.keepalive"
        );
    }

    #[test]
    fn test_sync_get_connection_routes_through_sentinel() {
        let sentinel_url = spawn_fake_sentinel_master_resolver();
        let connection_info =
            redis::ConnectionInfo::from_str(&sentinel_url).expect("parse sentinel url");
        let sentinel = FalkorClientProvider::None
            .get_sentinel_client_common(&connection_info, single_master_reply("mymaster"))
            .expect("sentinel client should build")
            .expect("single master yields a client");

        let mut provider = FalkorClientProvider::Redis {
            client: redis::Client::open(connection_info).unwrap(),
            sentinel: Some(sentinel),
            sentinel_replica: None,
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: Some(std::time::Duration::from_millis(200)),
            connect_timeout: None,
        };

        // Note: the fake master accepts two connections (one for Sentinel ROLE check and one
        // for open_sync_connection), so get_connection() succeeds via Sentinel.
        assert!(
            provider.get_connection().is_ok(),
            "sync get_connection should resolve the master via Sentinel and open a connection"
        );
    }

    #[test]
    fn test_sync_sentinel_resolution_stalled_sentinel_is_bounded_by_connect_timeout() {
        use std::net::TcpListener;

        // A listener that accepts the TCP handshake and never sends a byte.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind silent sentinel");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (_stream, _) = listener.accept().expect("accept silent sentinel");
            std::thread::sleep(std::time::Duration::from_secs(30));
        });

        let sentinel_url = format!("redis://127.0.0.1:{port}");
        let connection_info = redis::ConnectionInfo::from_str(&sentinel_url).unwrap();
        let sentinel = FalkorClientProvider::None
            .get_sentinel_client_common(&connection_info, single_master_reply("mymaster"))
            .unwrap()
            .unwrap();
        let replica = redis::sentinel::SentinelClient::build(
            vec![connection_info.clone()],
            "mymaster".to_string(),
            None,
            redis::sentinel::SentinelServerType::Replica,
        )
        .unwrap();

        let mut provider = FalkorClientProvider::Redis {
            client: redis::Client::open(connection_info).unwrap(),
            sentinel: Some(sentinel),
            sentinel_replica: Some(replica),
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: None,
            connect_timeout: Some(std::time::Duration::from_millis(60)),
        };

        let start = std::time::Instant::now();
        assert!(
            provider.get_connection().is_err(),
            "stalled Sentinel must fail within connect_timeout"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "Sentinel resolution must not hang when Sentinel stalls after TCP accept"
        );
    }

    #[test]
    fn test_sync_sentinel_resolution_stalled_node_role_is_bounded_by_connect_timeout() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        // Candidate master accepts TCP but stalls on ROLE.
        let master_listener = TcpListener::bind("127.0.0.1:0").expect("bind silent master");
        let master_port = master_listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (_stream, _) = master_listener.accept().expect("accept silent master");
            std::thread::sleep(std::time::Duration::from_secs(30));
        });

        // Sentinel answers ROLE and SENTINEL MASTERS pointing at the stalled master.
        let sentinel_listener = TcpListener::bind("127.0.0.1:0").expect("bind sentinel");
        let sentinel_port = sentinel_listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut stream, _) = sentinel_listener.accept().expect("accept sentinel");
            let mut pending = String::new();
            let mut chunk = [0u8; 1024];
            while let Ok(read) = stream.read(&mut chunk) {
                if read == 0 {
                    break;
                }
                pending.push_str(&String::from_utf8_lossy(&chunk[..read]));
                for command in take_commands(&mut pending) {
                    let upper = command.to_ascii_uppercase();
                    let reply = if upper.contains("\r\nSENTINEL\r\n")
                        && upper.contains("\r\nMASTERS\r\n")
                    {
                        let port_str = master_port.to_string();
                        format!(
                            "*1\r\n*8\r\n$4\r\nname\r\n$8\r\nmymaster\r\n$2\r\nip\r\n$9\r\n127.0.0.1\r\n$4\r\nport\r\n${}\r\n{port_str}\r\n$5\r\nflags\r\n$6\r\nmaster\r\n",
                            port_str.len()
                        )
                    } else if upper.contains("\r\nROLE\r\n") {
                        "*2\r\n$8\r\nsentinel\r\n*1\r\n$8\r\nmymaster\r\n".to_string()
                    } else {
                        "+OK\r\n".to_string()
                    };
                    let _ = stream.write_all(reply.as_bytes());
                }
            }
        });

        let sentinel_url = format!("redis://127.0.0.1:{sentinel_port}");
        let connection_info = redis::ConnectionInfo::from_str(&sentinel_url).unwrap();
        let sentinel = FalkorClientProvider::None
            .get_sentinel_client_common(&connection_info, single_master_reply("mymaster"))
            .unwrap()
            .unwrap();

        let mut provider = FalkorClientProvider::Redis {
            client: redis::Client::open(connection_info).unwrap(),
            sentinel: Some(sentinel),
            sentinel_replica: None,
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: None,
            connect_timeout: Some(std::time::Duration::from_millis(60)),
        };

        let start = std::time::Instant::now();
        assert!(
            provider.get_connection().is_err(),
            "stalled node ROLE check must fail within connect_timeout"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "node ROLE validation must not hang when candidate node stalls"
        );
    }

    #[test]
    fn test_sync_get_replica_connection_routes_through_sentinel() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let replica_listener = TcpListener::bind("127.0.0.1:0").expect("bind replica");
        let replica_port = replica_listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for stream in replica_listener.incoming().take(2).flatten() {
                let mut stream = stream;
                let mut pending = String::new();
                let mut chunk = [0u8; 1024];
                while let Ok(read) = stream.read(&mut chunk) {
                    if read == 0 {
                        break;
                    }
                    pending.push_str(&String::from_utf8_lossy(&chunk[..read]));
                    for command in take_commands(&mut pending) {
                        let reply = if command.to_ascii_uppercase().contains("\r\nROLE\r\n") {
                            "*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:6379\r\n$9\r\nconnected\r\n:0\r\n"
                        } else {
                            "+OK\r\n"
                        };
                        let _ = stream.write_all(reply.as_bytes());
                    }
                }
            }
        });

        let sentinel_listener = TcpListener::bind("127.0.0.1:0").expect("bind sentinel");
        let sentinel_port = sentinel_listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut stream, _) = sentinel_listener.accept().expect("accept sentinel");
            let mut pending = String::new();
            let mut chunk = [0u8; 1024];
            while let Ok(read) = stream.read(&mut chunk) {
                if read == 0 {
                    break;
                }
                pending.push_str(&String::from_utf8_lossy(&chunk[..read]));
                for command in take_commands(&mut pending) {
                    let upper = command.to_ascii_uppercase();
                    let reply = if upper.contains("\r\nSENTINEL\r\n")
                        && upper.contains("\r\nMASTERS\r\n")
                    {
                        "*1\r\n*2\r\n$4\r\nname\r\n$8\r\nmymaster\r\n".to_string()
                    } else if upper.contains("\r\nSENTINEL\r\n") && upper.contains("\r\nSLAVES\r\n")
                    {
                        let port_str = replica_port.to_string();
                        format!(
                            "*2\r\n*6\r\n$2\r\nip\r\n$9\r\n127.0.0.1\r\n$4\r\nport\r\n$1\r\n0\r\n$5\r\nflags\r\n$5\r\nslave\r\n*6\r\n$2\r\nip\r\n$9\r\n127.0.0.1\r\n$4\r\nport\r\n${}\r\n{port_str}\r\n$5\r\nflags\r\n$5\r\nslave\r\n",
                            port_str.len()
                        )
                    } else if upper.contains("\r\nROLE\r\n") {
                        "*2\r\n$8\r\nsentinel\r\n*1\r\n$8\r\nmymaster\r\n".to_string()
                    } else {
                        "+OK\r\n".to_string()
                    };
                    let _ = stream.write_all(reply.as_bytes());
                }
            }
        });

        let sentinel_url = format!("redis://127.0.0.1:{sentinel_port}");
        let connection_info = redis::ConnectionInfo::from_str(&sentinel_url).unwrap();
        let replica = redis::sentinel::SentinelClient::build(
            vec![connection_info.clone()],
            "mymaster".to_string(),
            None,
            redis::sentinel::SentinelServerType::Replica,
        )
        .unwrap();

        let mut provider = FalkorClientProvider::Redis {
            client: redis::Client::open(connection_info).unwrap(),
            sentinel: None,
            sentinel_replica: Some(replica),
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: Some(std::time::Duration::from_millis(200)),
            connect_timeout: Some(std::time::Duration::from_secs(2)),
        };

        assert!(
            provider.get_replica_connection().is_ok(),
            "sync get_replica_connection should resolve a replica via Sentinel with timeouts"
        );
    }

    #[test]
    fn test_sync_sentinel_resolution_edge_cases() {
        use redis::IntoConnectionInfo;
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let closed_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let closed_port = closed_listener.local_addr().unwrap().port();
        drop(closed_listener);
        let tcp_info =
            redis::ConnectionInfo::from_str(&format!("redis://127.0.0.1:{closed_port}")).unwrap();
        assert!(FalkorClientProvider::connect_redis_client(
            &redis::Client::open(tcp_info).unwrap(),
            None
        )
        .is_err());

        let unix_info = redis::ConnectionInfo::from_str("redis+unix:///tmp/sentinel.sock").unwrap();
        let mapped_unix = FalkorClientProvider::sentinel_node_connection_info(
            &unix_info,
            "127.0.0.1".to_string(),
            6380,
        );
        assert!(matches!(
            mapped_unix.addr(),
            redis::ConnectionAddr::Tcp(host, 6380) if host == "127.0.0.1"
        ));

        let tls_sentinel_info = redis::ConnectionAddr::TcpTls {
            host: "sentinel.local".to_string(),
            port: 26379,
            insecure: true,
            tls_params: None,
        }
        .into_connection_info()
        .unwrap();
        let mapped_tls = FalkorClientProvider::sentinel_node_connection_info(
            &tls_sentinel_info,
            "10.0.0.2".to_string(),
            6381,
        );
        assert!(matches!(
            mapped_tls.addr(),
            redis::ConnectionAddr::TcpTls {
                host,
                port: 6381,
                insecure: true,
                ..
            } if host == "10.0.0.2"
        ));

        let multi_service = TcpListener::bind("127.0.0.1:0").unwrap();
        let multi_port = multi_service.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut s, _)) = multi_service.accept() {
                let mut pending = String::new();
                let mut chunk = [0u8; 512];
                while let Ok(read) = s.read(&mut chunk) {
                    if read == 0 {
                        break;
                    }
                    pending.push_str(&String::from_utf8_lossy(&chunk[..read]));
                    for command in take_commands(&mut pending) {
                        let reply = if command.to_ascii_uppercase().contains("\r\nROLE\r\n") {
                            "-NOPERM this user has no permissions to run the 'role' command\r\n"
                        } else {
                            "*2\r\n*2\r\n$4\r\nname\r\n$2\r\nm1\r\n*2\r\n$4\r\nname\r\n$2\r\nm2\r\n"
                        };
                        let _ = s.write_all(reply.as_bytes());
                    }
                }
            }
        });
        let multi_info =
            redis::ConnectionInfo::from_str(&format!("redis://127.0.0.1:{multi_port}")).unwrap();
        assert!(matches!(
            FalkorClientProvider::resolve_sentinel_client_with_timeout(
                &multi_info,
                false,
                Some(std::time::Duration::from_millis(500)),
            ),
            Err(FalkorDBError::SentinelMastersCount)
        ));

        let non_sentinel = TcpListener::bind("127.0.0.1:0").unwrap();
        let non_sentinel_port = non_sentinel.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut s, _)) = non_sentinel.accept() {
                let mut pending = String::new();
                let mut chunk = [0u8; 512];
                while let Ok(read) = s.read(&mut chunk) {
                    if read == 0 {
                        break;
                    }
                    pending.push_str(&String::from_utf8_lossy(&chunk[..read]));
                    for command in take_commands(&mut pending) {
                        let reply = if command.to_ascii_uppercase().contains("\r\nSENTINEL\r\n") {
                            "-ERR unknown command 'SENTINEL'\r\n"
                        } else {
                            "+OK\r\n"
                        };
                        let _ = s.write_all(reply.as_bytes());
                    }
                }
            }
        });
        let info =
            redis::ConnectionInfo::from_str(&format!("redis://127.0.0.1:{non_sentinel_port}"))
                .unwrap();
        assert!(
            FalkorClientProvider::resolve_sentinel_client_with_timeout(
                &info,
                false,
                Some(std::time::Duration::from_millis(500)),
            )
            .is_err(),
            "non-sentinel ROLE should return an error"
        );

        let no_master_sentinel = TcpListener::bind("127.0.0.1:0").unwrap();
        let no_master_port = no_master_sentinel.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = no_master_sentinel.accept() {
                let mut pending = String::new();
                let mut chunk = [0u8; 512];
                while let Ok(read) = stream.read(&mut chunk) {
                    if read == 0 {
                        break;
                    }
                    pending.push_str(&String::from_utf8_lossy(&chunk[..read]));
                    for _ in take_commands(&mut pending) {
                        let _ = stream.write_all(b"*0\r\n");
                    }
                }
            }
        });
        let no_master_info =
            redis::ConnectionInfo::from_str(&format!("redis://127.0.0.1:{no_master_port}"))
                .unwrap();
        assert!(
            FalkorClientProvider::resolve_sentinel_client_with_timeout(
                &no_master_info,
                true,
                Some(std::time::Duration::from_millis(500)),
            )
            .is_err(),
            "replica resolution without any master in SENTINEL MASTERS should return an error"
        );

        let empty_sentinel = TcpListener::bind("127.0.0.1:0").unwrap();
        let empty_port = empty_sentinel.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for stream in empty_sentinel.incoming().take(3).flatten() {
                let mut stream = stream;
                let mut pending = String::new();
                let mut chunk = [0u8; 512];
                while let Ok(read) = stream.read(&mut chunk) {
                    if read == 0 {
                        break;
                    }
                    pending.push_str(&String::from_utf8_lossy(&chunk[..read]));
                    for command in take_commands(&mut pending) {
                        let upper = command.to_ascii_uppercase();
                        let reply = if upper.contains("\r\nROLE\r\n") {
                            "*2\r\n$8\r\nsentinel\r\n*0\r\n".to_string()
                        } else if upper.contains("\r\nSLAVES\r\n") {
                            "*0\r\n".to_string()
                        } else {
                            // Single master entry whose candidate port is the sentinel itself (so ROLE returns "sentinel", not "master")
                            let self_port = empty_port.to_string();
                            format!(
                                "*1\r\n*8\r\n$2\r\nip\r\n$9\r\n127.0.0.1\r\n$4\r\nport\r\n${}\r\n{self_port}\r\n$4\r\nname\r\n$8\r\nmymaster\r\n$5\r\nflags\r\n$6\r\nmaster\r\n",
                                self_port.len()
                            )
                        };
                        let _ = stream.write_all(reply.as_bytes());
                    }
                }
            }
        });
        let empty_info =
            redis::ConnectionInfo::from_str(&format!("redis://127.0.0.1:{empty_port}")).unwrap();
        assert!(FalkorClientProvider::resolve_sentinel_client_with_timeout(
            &empty_info,
            false,
            Some(std::time::Duration::from_millis(500)),
        )
        .is_err());
        assert!(FalkorClientProvider::resolve_sentinel_client_with_timeout(
            &empty_info,
            true,
            Some(std::time::Duration::from_millis(500)),
        )
        .is_err());
    }

    #[test]
    fn test_tls_handshake_failure_and_stall_are_bounded_and_do_not_panic() {
        use redis::IntoConnectionInfo;
        use std::io::Write;
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind plain tcp server");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _ = stream.write_all(b"-ERR plain tcp\r\n");
            }
        });

        let tls_info = redis::ConnectionAddr::TcpTls {
            host: "127.0.0.1".to_string(),
            port,
            insecure: false,
            tls_params: None,
        }
        .into_connection_info()
        .unwrap();
        let client = redis::Client::open(tls_info).unwrap();
        let res = FalkorClientProvider::open_sync_connection(
            &client,
            Some(std::time::Duration::from_millis(200)),
            Some(std::time::Duration::from_millis(500)),
        );
        assert!(
            res.is_err(),
            "failed TLS handshake with finite connect_timeout must return Err, never panic"
        );

        let silent_listener = TcpListener::bind("127.0.0.1:0").expect("bind silent tcp server");
        let silent_port = silent_listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((_stream, _)) = silent_listener.accept() {
                std::thread::sleep(std::time::Duration::from_millis(300));
            }
        });
        let silent_tls_info = redis::ConnectionAddr::TcpTls {
            host: "127.0.0.1".to_string(),
            port: silent_port,
            insecure: false,
            tls_params: None,
        }
        .into_connection_info()
        .unwrap();
        let silent_client = redis::Client::open(silent_tls_info).unwrap();
        let start = std::time::Instant::now();
        assert!(
            FalkorClientProvider::open_sync_connection(
                &silent_client,
                None,
                Some(std::time::Duration::from_millis(60)),
            )
            .is_err(),
            "stalled TLS peer must fail within connect_timeout"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(1),
            "stalled TLS peer must not hang indefinitely"
        );
    }

    /// Serve every connection to an ephemeral port on its own thread, answering each command
    /// with `reply(command)`, and return the port.
    fn spawn_fake_resp_server(reply: impl Fn(&str) -> String + Send + Sync + 'static) -> u16 {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake server");
        let port = listener.local_addr().unwrap().port();
        let reply = std::sync::Arc::new(reply);
        std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let reply = std::sync::Arc::clone(&reply);
                std::thread::spawn(move || {
                    let mut pending = String::new();
                    let mut chunk = [0u8; 1024];
                    while let Ok(read) = stream.read(&mut chunk) {
                        if read == 0 {
                            break;
                        }
                        pending.push_str(&String::from_utf8_lossy(&chunk[..read]));
                        for command in take_commands(&mut pending) {
                            let _ =
                                stream.write_all(reply(&command.to_ascii_uppercase()).as_bytes());
                        }
                    }
                });
            }
        });
        port
    }

    /// A fake Sentinel monitoring `mymaster`, answering `SENTINEL MASTERS` / `SENTINEL SLAVES`
    /// with the given pre-encoded RESP replies and rejecting any other command (such as `ROLE`)
    /// with `-NOPERM`.
    fn spawn_fake_sentinel(
        masters: String,
        replicas: String,
    ) -> redis::ConnectionInfo {
        let port = spawn_fake_resp_server(move |command| {
            if command.contains("\r\nMASTERS\r\n") {
                masters.clone()
            } else if command.contains("\r\nSLAVES\r\n") {
                replicas.clone()
            } else {
                "-NOPERM this user has no permissions to run this command\r\n".to_string()
            }
        });
        redis::ConnectionInfo::from_str(&format!("redis://127.0.0.1:{port}")).unwrap()
    }

    /// RESP encoding of one `SENTINEL MASTERS`/`SLAVES` entry.
    fn sentinel_entry(fields: &[(&str, &str)]) -> String {
        let mut out = format!("*{}\r\n", fields.len() * 2);
        for (key, val) in fields {
            out.push_str(&format!(
                "${}\r\n{key}\r\n${}\r\n{val}\r\n",
                key.len(),
                val.len()
            ));
        }
        out
    }

    fn resolved_port(client: &redis::Client) -> u16 {
        let redis::ConnectionAddr::Tcp(_, port) = client.get_connection_info().addr() else {
            unreachable!()
        };
        *port
    }

    #[test]
    fn test_sync_replica_resolution_spreads_across_healthy_replicas() {
        let replica_role = |command: &str| {
            if command.contains("\r\nROLE\r\n") {
                "*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:6379\r\n$9\r\nconnected\r\n:0\r\n"
                    .to_string()
            } else {
                "+OK\r\n".to_string()
            }
        };
        let first = spawn_fake_resp_server(replica_role);
        let second = spawn_fake_resp_server(replica_role);
        let replicas = [first, second]
            .iter()
            .map(|port| {
                sentinel_entry(&[
                    ("ip", "127.0.0.1"),
                    ("port", &port.to_string()),
                    ("flags", "slave"),
                ])
            })
            .collect::<String>();
        let sentinel = spawn_fake_sentinel(
            format!("*1\r\n{}", sentinel_entry(&[("name", "mymaster")])),
            format!("*2\r\n{replicas}"),
        );

        let chosen: std::collections::HashSet<u16> = (0..6)
            .map(|_| {
                let client = FalkorClientProvider::resolve_sentinel_client_with_timeout(
                    &sentinel,
                    true,
                    Some(std::time::Duration::from_secs(2)),
                )
                .expect("a healthy replica resolves");
                resolved_port(&client)
            })
            .collect();
        assert_eq!(
            chosen,
            [first, second].into_iter().collect(),
            "repeated replica lookups must not all land on the first healthy replica"
        );
    }

    #[test]
    fn test_sync_master_resolution_checks_flags_and_falls_back_to_info_replication() {
        // Rejects `ROLE` but reports its role through `INFO REPLICATION`.
        let info_only = |role: &'static str| {
            spawn_fake_resp_server(move |command| {
                if command.contains("\r\nROLE\r\n") {
                    "-NOPERM this user has no permissions to run the 'role' command\r\n".to_string()
                } else if command.contains("\r\nINFO\r\n") {
                    let body = format!("# Replication\r\nrole:{role}\r\nconnected_slaves:0\r\n");
                    format!("${}\r\n{body}\r\n", body.len())
                } else {
                    "+OK\r\n".to_string()
                }
            })
        };
        let master = info_only("master");
        let masters = |port: u16| {
            format!(
                "*1\r\n{}",
                sentinel_entry(&[
                    ("name", "mymaster"),
                    ("ip", "127.0.0.1"),
                    ("port", &port.to_string()),
                    ("flags", "master"),
                ]),
            )
        };

        let unflagged_sentinel = spawn_fake_sentinel(
            format!(
                "*1\r\n{}",
                sentinel_entry(&[
                    ("name", "mymaster"),
                    ("ip", "127.0.0.1"),
                    ("port", &master.to_string()),
                ]),
            ),
            "*0\r\n".to_string(),
        );
        assert!(
            FalkorClientProvider::resolve_sentinel_client_with_timeout(
                &unflagged_sentinel,
                false,
                Some(std::time::Duration::from_secs(2)),
            )
            .is_err(),
            "a master entry without the master flag is not eligible"
        );

        let sentinel = spawn_fake_sentinel(masters(master), "*0\r\n".to_string());
        let client = FalkorClientProvider::resolve_sentinel_client_with_timeout(
            &sentinel,
            false,
            Some(std::time::Duration::from_secs(2)),
        )
        .expect("a master that rejects ROLE resolves through INFO REPLICATION");
        assert_eq!(resolved_port(&client), master);

        let demoted = spawn_fake_sentinel(masters(info_only("slave")), "*0\r\n".to_string());
        assert!(
            FalkorClientProvider::resolve_sentinel_client_with_timeout(
                &demoted,
                false,
                Some(std::time::Duration::from_secs(2)),
            )
            .is_err(),
            "a node whose INFO REPLICATION says slave is not accepted as the master"
        );
    }

    #[cfg(all(
        any(feature = "native-tls", feature = "tokio-native-tls"),
        not(any(feature = "rustls", feature = "tokio-rustls"))
    ))]
    fn silent_tls_client(port: u16) -> redis::Client {
        use redis::IntoConnectionInfo;

        redis::Client::open(
            redis::ConnectionAddr::TcpTls {
                host: "127.0.0.1".to_string(),
                port,
                insecure: false,
                tls_params: None,
            }
            .into_connection_info()
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    #[cfg(all(
        any(feature = "native-tls", feature = "tokio-native-tls"),
        not(any(feature = "rustls", feature = "tokio-rustls"))
    ))]
    fn test_native_tls_abandoned_handshake_is_released_when_the_peer_closes() {
        use std::net::TcpListener;
        use std::sync::atomic::Ordering;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind silent tls peer");
        let port = listener.local_addr().unwrap().port();
        let (close_tx, close_rx) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            let (_stream, _) = listener.accept().expect("accept the handshake");
            let _ = close_rx.recv();
        });

        let client = silent_tls_client(port);
        let in_flight = native_tls_connect::counter_for(&client);
        assert!(native_tls_connect::connect(
            &client,
            std::time::Duration::from_millis(60),
            &in_flight,
            16,
        )
        .is_err());
        assert_eq!(
            in_flight.load(Ordering::Acquire),
            1,
            "the stalled attempt is tracked"
        );

        close_tx.send(()).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while in_flight.load(Ordering::Acquire) != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the helper thread must finish once the peer closes"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[test]
    #[cfg(all(
        any(feature = "native-tls", feature = "tokio-native-tls"),
        not(any(feature = "rustls", feature = "tokio-rustls"))
    ))]
    fn test_native_tls_connect_fails_fast_once_too_many_attempts_are_stalled() {
        use std::net::TcpListener;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier};

        // 1. Concurrent admission: 4 callers race against a cap of 2; at most 2 are admitted.
        let stalled_listener = TcpListener::bind("127.0.0.1:0").expect("bind stalled tls peer");
        let stalled_port = stalled_listener.local_addr().unwrap().port();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            let mut accepted = Vec::new();
            for stream in stalled_listener.incoming().take(2).flatten() {
                accepted.push(stream);
            }
            let _ = release_rx.recv();
        });

        let stalled_client = silent_tls_client(stalled_port);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(4));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let c = stalled_client.clone();
                let counter = Arc::clone(&in_flight);
                let b = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    b.wait();
                    native_tls_connect::connect(
                        &c,
                        std::time::Duration::from_millis(60),
                        &counter,
                        2,
                    )
                })
            })
            .collect();
        for h in handles {
            assert!(h.join().unwrap().is_err());
        }
        assert!(
            in_flight.load(Ordering::Acquire) <= 2,
            "concurrent callers must never exceed max_in_flight"
        );

        // 2. While the stalled target is at capacity, a new attempt to that target does not even
        //    open a TCP socket, whereas a different target gets its own independent counter.
        let unaccepted = TcpListener::bind("127.0.0.1:0").expect("bind tls peer");
        unaccepted.set_nonblocking(true).unwrap();
        let unaccepted_port = unaccepted.local_addr().unwrap().port();
        let saturated = Arc::new(AtomicUsize::new(2));
        assert!(native_tls_connect::connect(
            &silent_tls_client(unaccepted_port),
            std::time::Duration::from_secs(5),
            &saturated,
            2,
        )
        .is_err());
        assert_eq!(
            unaccepted.accept().map_err(|err| err.kind()).err(),
            Some(std::io::ErrorKind::WouldBlock),
            "no connection is attempted while the target cap is reached"
        );
        assert_eq!(
            native_tls_connect::counter_for(&silent_tls_client(unaccepted_port))
                .load(Ordering::Acquire),
            0,
            "an unrelated target has its own counter and is not blocked by another peer"
        );

        // 3. Releasing the stalled peers returns the counter to 0 without underflow.
        let _ = release_tx.send(());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while in_flight.load(Ordering::Acquire) != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "stalled workers must release their reservations without wrapping"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

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
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: None,
            connect_timeout: None,
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
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: None,
            connect_timeout: None,
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
    #[cfg(feature = "embedded-core")]
    fn test_falkor_client_provider_with_embedded_server() {
        // Test that FalkorClientProvider::Redis can hold an embedded server
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let _provider = FalkorClientProvider::Redis {
            client,
            sentinel: None,
            sentinel_replica: None,
            embedded_server: None,
            response_timeout: None,
            connect_timeout: None,
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
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: None,
            connect_timeout: None,
        };
        // Just verify the structure can be created
    }

    #[test]
    fn test_get_replica_connection_errors_when_replica_unreachable() {
        // When a replica SentinelClient exists but the replica is unreachable, the
        // implementation must propagate the error rather than fall back to the
        // primary, so the read-only pool never receives primary connections.
        // Port 1 is reliably unroutable in test environments.
        let client = redis::Client::open("redis://127.0.0.1:1").unwrap();
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
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: None,
            connect_timeout: None,
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
                #[cfg(feature = "embedded-core")]
                embedded_server: None,
                response_timeout: None,
                connect_timeout: None,
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
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: None,
            connect_timeout: None,
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
                #[cfg(feature = "embedded-core")]
                embedded_server: None,
                response_timeout: None,
                connect_timeout: None,
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
                #[cfg(feature = "embedded-core")]
                embedded_server: None,
                response_timeout: None,
                connect_timeout: None,
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
                #[cfg(feature = "embedded-core")]
                embedded_server: None,
                response_timeout: None,
                connect_timeout: None,
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
                #[cfg(feature = "embedded-core")]
                embedded_server: None,
                response_timeout: None,
                connect_timeout: None,
            };
            let result = provider.get_async_replica_connection_manager(None).await;
            assert!(matches!(result, Err(FalkorDBError::UnavailableProvider)));
        });
    }
}
