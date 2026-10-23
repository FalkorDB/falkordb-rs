/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    client::{ConnectionStrategy, FalkorClientProvider, ReadPreference},
    connection::asynchronous::{BorrowedAsyncConnection, FalkorAsyncConnection},
    parser::{parse_config_hashmap, redis_value_as_untyped_string_vec},
    AsyncGraph, ConfigValue, FalkorConnectionInfo, FalkorDBError, FalkorResult, RetryPolicy,
};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, Mutex};

/// A connection pool holding a fixed number of slots that callers borrow from and
/// return to. A slot holds a live async connection, or `None` once its connection died
/// and no replacement could be opened, in which case the next borrower opens one. Used
/// by the [`ConnectionStrategy::Pooled`] strategy for both the primary pool and the
/// optional replica-routed read-only pool.
pub(crate) struct AsyncConnectionPool {
    tx: mpsc::Sender<Option<FalkorAsyncConnection>>,
    rx: Mutex<mpsc::Receiver<Option<FalkorAsyncConnection>>>,
}

/// Holds a fixed number of independent, shared multiplexed connections and hands out
/// cheap clones round-robin. Used by the [`ConnectionStrategy::Multiplexed`] strategy.
pub(crate) struct MultiplexedExecutor {
    conns: Vec<FalkorAsyncConnection>,
    next: AtomicUsize,
}

impl MultiplexedExecutor {
    /// Returns the next connection (a cheap clone of a shared multiplexed socket),
    /// selected round-robin across the underlying connections.
    fn pick(&self) -> FalkorAsyncConnection {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.conns.len();
        self.conns[idx].clone_handle()
    }
}

/// The concrete connection-management backend for a primary or read-only route.
pub(crate) enum AsyncExecutor {
    /// A bounded pool of independent connections, borrowed one at a time.
    Pooled(AsyncConnectionPool),
    /// A set of shared multiplexed connections, cloned per command.
    Multiplexed(MultiplexedExecutor),
}

/// A user-opaque inner struct, containing the actual implementation of the asynchronous client
/// The idea is that each member here is either Copy, or locked in some form, and the public struct only has an Arc to this struct
/// allowing thread safe operations and cloning
pub struct FalkorAsyncClientInner {
    _inner: Mutex<FalkorClientProvider>,

    /// The effective strategy this client runs (may differ from the requested one, e.g.
    /// a Sentinel deployment that falls back to pooling).
    strategy: ConnectionStrategy,
    /// Backend serving primary (read-write) commands.
    primary: AsyncExecutor,
    /// Backend serving read-only queries from replica nodes. `None` when the deployment
    /// has no readable replicas, in which case read-only queries reuse the primary
    /// backend (preserving the previous behavior).
    readonly: Option<AsyncExecutor>,
    /// Opt-in retry policy applied to eligible operations; [`disabled`](RetryPolicy::disabled) by
    /// default, in which case every operation is attempted exactly once.
    retry_policy: RetryPolicy,
    /// When set, the raw query text is recorded as a span field (opt-in via `with_query_logging`).
    #[cfg_attr(not(feature = "tracing"), allow(dead_code))]
    query_logging: bool,
    /// Client-wide default read preference for read-only queries (overridable per query).
    read_preference: ReadPreference,
}

impl FalkorAsyncClientInner {
    /// The retry policy configured for this client (defaults to
    /// [`disabled`](RetryPolicy::disabled)).
    pub(crate) fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy
    }

    /// The client-wide default [`ReadPreference`] for read-only queries.
    pub(crate) fn read_preference(&self) -> ReadPreference {
        self.read_preference
    }

    /// The active connection strategy (used for observability span fields and metric labels).
    #[cfg(any(feature = "tracing", feature = "metrics"))]
    pub(crate) fn strategy(&self) -> ConnectionStrategy {
        self.strategy
    }

    /// Whether raw query text may be recorded on spans (opt-in; `false` by default).
    #[cfg(feature = "tracing")]
    pub(crate) fn query_logging(&self) -> bool {
        self.query_logging
    }

    /// Borrow a connection from the given executor. For the pooled strategy this waits
    /// for an available connection; for the multiplexed strategy it hands out a cheap
    /// clone immediately.
    async fn borrow_from(
        executor: &AsyncExecutor,
        pool_owner: Arc<Self>,
        readonly: bool,
    ) -> FalkorResult<BorrowedAsyncConnection> {
        match executor {
            AsyncExecutor::Pooled(pool) => {
                #[cfg(feature = "metrics")]
                let wait_start = std::time::Instant::now();
                let conn = pool
                    .rx
                    .lock()
                    .await
                    .recv()
                    .await
                    .ok_or(FalkorDBError::EmptyConnection)?;
                #[cfg(feature = "metrics")]
                crate::observability::record_pool_wait(readonly, wait_start.elapsed());
                Ok(BorrowedAsyncConnection::new(
                    conn,
                    pool.tx.clone(),
                    pool_owner,
                    readonly,
                ))
            }
            AsyncExecutor::Multiplexed(executor) => Ok(BorrowedAsyncConnection::new_multiplexed(
                executor.pick(),
                pool_owner,
                readonly,
            )),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Borrow Connection From Connection Pool",
            skip_all,
            level = "debug"
        )
    )]
    pub(crate) async fn borrow_connection(
        &self,
        pool_owner: Arc<Self>,
    ) -> FalkorResult<BorrowedAsyncConnection> {
        Self::borrow_from(&self.primary, pool_owner, false).await
    }

    /// Borrow a connection for a read-only query. When a replica-routed read-only
    /// backend exists the connection is taken from it (serving the query from a
    /// replica), otherwise it falls back to the primary backend.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Borrow Readonly Connection From Connection Pool",
            skip_all,
            level = "debug"
        )
    )]
    pub(crate) async fn borrow_readonly_connection(
        &self,
        pool_owner: Arc<Self>,
    ) -> FalkorResult<BorrowedAsyncConnection> {
        match &self.readonly {
            Some(executor) => Self::borrow_from(executor, pool_owner, true).await,
            None => self.borrow_connection(pool_owner).await,
        }
    }

    /// Whether read-only queries are routed to replica nodes for this client.
    pub(crate) fn has_readonly_pool(&self) -> bool {
        self.readonly.is_some()
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Get New Async Connection From Client",
            skip_all,
            level = "info"
        )
    )]
    pub(crate) async fn get_async_connection(&self) -> FalkorResult<FalkorAsyncConnection> {
        self._inner.lock().await.get_async_connection().await
    }

    /// Obtain a fresh async connection routed to a replica node without fallback.
    /// Used for read-only pool creation and reconnection so the read-only pool
    /// never receives primary connections.
    pub(crate) async fn get_async_replica_connection(&self) -> FalkorResult<FalkorAsyncConnection> {
        self._inner
            .lock()
            .await
            .get_async_replica_connection()
            .await
    }
}

/// This is the publicly exposed API of the asynchronous Falkor Client
/// It makes no assumptions in regard to which database the Falkor module is running on,
/// and will select it based on enabled features and url connection
///
/// # Thread Safety
/// This struct is fully thread safe, it can be cloned and passed between threads without constraints,
/// Its API uses only immutable references
#[derive(Clone)]
pub struct FalkorAsyncClient {
    inner: Arc<FalkorAsyncClientInner>,
    _connection_info: FalkorConnectionInfo,
}

impl FalkorAsyncClient {
    pub(crate) async fn create(
        mut client: FalkorClientProvider,
        connection_info: FalkorConnectionInfo,
        requested_strategy: ConnectionStrategy,
        max_inflight: Option<NonZeroUsize>,
        retry_policy: RetryPolicy,
        query_logging: bool,
        read_preference: ReadPreference,
    ) -> FalkorResult<Self> {
        // A multiplexed ConnectionManager built from a Sentinel-resolved client pins to a
        // single node and reconnects to the same address rather than re-resolving the
        // current master/replica via Sentinel on failover. The pooled strategy opens a
        // fresh, Sentinel-resolved connection on every (re)connect, so for Sentinel
        // deployments we downgrade to an equivalently-sized pool. `connection_strategy()`
        // reports this effective value.
        let strategy = match requested_strategy {
            ConnectionStrategy::Multiplexed { connections } if client.has_sentinel() => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    "Sentinel deployment detected: downgrading ConnectionStrategy from \
                     Multiplexed to Pooled. A multiplexed ConnectionManager pins to a single \
                     resolved node and will not re-resolve on failover; the pooled strategy \
                     opens a fresh Sentinel-resolved connection on every reconnect."
                );
                ConnectionStrategy::Pooled { size: connections }
            }
            other => other,
        };

        let primary = Self::build_executor(&mut client, strategy, max_inflight, false).await?;

        // Best-effort replica-routed read-only backend. Absent (transparent fallback to
        // the primary) when the deployment exposes no readable replicas or they cannot
        // currently be reached.
        let readonly = if client.has_sentinel_replica() {
            Self::build_executor(&mut client, strategy, max_inflight, true)
                .await
                .ok()
        } else {
            None
        };

        Ok(Self {
            inner: Arc::new(FalkorAsyncClientInner {
                _inner: client.into(),
                strategy,
                primary,
                readonly,
                retry_policy,
                query_logging,
                read_preference,
            }),
            _connection_info: connection_info,
        })
    }

    /// Build an [`AsyncExecutor`] for the requested strategy. `readonly` selects the
    /// replica-routed provider getters so a read-only backend never receives primary
    /// connections.
    async fn build_executor(
        client: &mut FalkorClientProvider,
        strategy: ConnectionStrategy,
        max_inflight: Option<NonZeroUsize>,
        readonly: bool,
    ) -> FalkorResult<AsyncExecutor> {
        let count = strategy.connection_count().get() as usize;
        match strategy {
            ConnectionStrategy::Pooled { .. } => {
                let mut connections = Vec::with_capacity(count);
                for _ in 0..count {
                    let conn = if readonly {
                        client.get_async_replica_connection().await?
                    } else {
                        client.get_async_connection().await?
                    };
                    connections.push(conn);
                }
                Self::pool_from_connections(connections)
                    .map(AsyncExecutor::Pooled)
                    .ok_or(FalkorDBError::EmptyConnection)
            }
            ConnectionStrategy::Multiplexed { .. } => {
                let mut conns = Vec::with_capacity(count);
                for _ in 0..count {
                    // Each iteration opens an independent multiplexed socket; clones for
                    // concurrent commands are taken later at execution time.
                    let conn = if readonly {
                        client
                            .get_async_replica_connection_manager(max_inflight)
                            .await?
                    } else {
                        client.get_async_connection_manager(max_inflight).await?
                    };
                    conns.push(conn);
                }
                Ok(AsyncExecutor::Multiplexed(MultiplexedExecutor {
                    conns,
                    next: AtomicUsize::new(0),
                }))
            }
        }
    }

    /// Build an [`AsyncConnectionPool`] pre-filled with the given connections. Returns
    /// `None` if the connections cannot be enqueued.
    fn pool_from_connections(
        connections: Vec<FalkorAsyncConnection>
    ) -> Option<AsyncConnectionPool> {
        let (tx, rx) = mpsc::channel(connections.len().max(1));
        for conn in connections {
            if tx.try_send(Some(conn)).is_err() {
                return None;
            }
        }

        Some(AsyncConnectionPool {
            tx,
            rx: Mutex::new(rx),
        })
    }

    /// Get the number of underlying connections this client maintains (pool size for the
    /// pooled strategy, or the number of multiplexed sockets).
    pub fn connection_pool_size(&self) -> u8 {
        self.inner.strategy.connection_count().get()
    }

    /// The effective [`ConnectionStrategy`] this client is running.
    pub fn connection_strategy(&self) -> ConnectionStrategy {
        self.inner.strategy
    }

    /// Whether replica connections are available for routing read-only queries — `true` only for
    /// Redis Sentinel deployments that expose readable replicas.
    ///
    /// This reports **capability**, not policy: a query is served from a replica only when this is
    /// `true` **and** the effective [`ReadPreference`] is
    /// [`PreferReplica`](ReadPreference::PreferReplica). Use [`read_preference`](Self::read_preference)
    /// to inspect the client's default policy.
    pub fn replica_reads_available(&self) -> bool {
        self.inner.has_readonly_pool()
    }

    /// The client-wide default [`ReadPreference`] applied to read-only queries (set via
    /// [`FalkorClientBuilder::with_read_preference`](crate::FalkorClientBuilder::with_read_preference);
    /// defaults to [`ReadPreference::Primary`]). Individual queries can override it.
    pub fn read_preference(&self) -> ReadPreference {
        self.inner.read_preference()
    }

    /// Whether read-only queries (`ro_query` / `call_procedure_ro`) are routed to
    /// replica nodes. This is `true` only for Redis Sentinel deployments that expose
    /// readable replicas; otherwise read-only queries are served by the primary.
    #[deprecated(
        since = "0.10.0",
        note = "renamed to `replica_reads_available` (reports replica capability). Since replica \
                routing is now opt-in, use `read_preference` to inspect the routing policy."
    )]
    pub fn reads_from_replicas(&self) -> bool {
        self.inner.has_readonly_pool()
    }

    pub(crate) async fn borrow_connection(&self) -> FalkorResult<BorrowedAsyncConnection> {
        self.inner.borrow_connection(self.inner.clone()).await
    }

    /// Return a list of graphs currently residing in the database
    ///
    /// # Returns
    /// A [`Vec`] of [`String`]s, containing the names of available graphs
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "List Graphs", skip_all, level = "info")
    )]
    pub async fn list_graphs(&self) -> FalkorResult<Vec<String>> {
        self.borrow_connection()
            .await?
            .execute_command(None, "GRAPH.LIST", None, None)
            .await
            .and_then(redis_value_as_untyped_string_vec)
    }

    /// Return the current value of a configuration option in the database.
    ///
    /// # Arguments
    /// * `config_Key`: A [`String`] representation of a configuration's key.
    ///   The config key can also be "*", which will return ALL the configuration options.
    ///
    /// # Returns
    /// A [`HashMap`] comprised of [`String`] keys, and [`ConfigValue`] values.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Get Config Value", skip_all, level = "info")
    )]
    pub async fn config_get(
        &self,
        config_key: &str,
    ) -> FalkorResult<HashMap<String, ConfigValue>> {
        self.borrow_connection()
            .await?
            .execute_command(None, "GRAPH.CONFIG", Some("GET"), Some(&[config_key]))
            .await
            .and_then(parse_config_hashmap)
    }

    /// Return the current value of a configuration option in the database.
    ///
    /// # Arguments
    /// * `config_Key`: A [`String`] representation of a configuration's key.
    ///   The config key can also be "*", which will return ALL the configuration options.
    /// * `value`: The new value to set, which is anything that can be converted into a [`ConfigValue`], namely string types and i64.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Set Config Value", skip_all, level = "info")
    )]
    pub async fn config_set<C: Into<ConfigValue>>(
        &self,
        config_key: &str,
        value: C,
    ) -> FalkorResult<redis::Value> {
        self.borrow_connection()
            .await?
            .execute_command(
                None,
                "GRAPH.CONFIG",
                Some("SET"),
                Some(&[config_key, value.into().to_string().as_str()]),
            )
            .await
    }

    /// Opens a graph context for queries and operations
    ///
    /// # Arguments
    /// * `graph_name`: A string identifier of the graph to open.
    ///
    /// # Returns
    /// a [`AsyncGraph`] object, allowing various graph operations.
    pub fn select_graph<T: ToString>(
        &self,
        graph_name: T,
    ) -> AsyncGraph {
        AsyncGraph::new(self.inner.clone(), graph_name)
    }

    /// Copies an entire graph and returns the [`AsyncGraph`] for the new copied graph.
    ///
    /// # Arguments
    /// * `graph_to_clone`: A string identifier of the graph to copy.
    /// * `new_graph_name`: The name to give the new graph.
    ///
    /// # Returns
    /// If successful, will return the new [`AsyncGraph`] object.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Copy Graph", skip_all, level = "info")
    )]
    pub async fn copy_graph(
        &self,
        graph_to_clone: &str,
        new_graph_name: &str,
    ) -> FalkorResult<AsyncGraph> {
        self.borrow_connection()
            .await?
            .execute_command(
                Some(graph_to_clone),
                "GRAPH.COPY",
                None,
                Some(&[new_graph_name]),
            )
            .await?;
        Ok(self.select_graph(new_graph_name))
    }

    /// Retrieves redis information
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Client Get Redis Info", skip_all, level = "info")
    )]
    pub async fn redis_info(
        &self,
        section: Option<&str>,
    ) -> FalkorResult<HashMap<String, String>> {
        self.borrow_connection()
            .await?
            .get_redis_info(section)
            .await
    }

    /// Load a User Defined Function (UDF) library.
    ///
    /// # Arguments
    /// * `name`: The name of the library to load.
    /// * `script`: The UDF script contents.
    /// * `replace`: If true, replace an existing library with the same name.
    ///
    /// # Returns
    /// A [`redis::Value`] indicating the result of the operation.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Load UDF Library", skip_all, level = "info")
    )]
    pub async fn udf_load(
        &self,
        name: &str,
        script: &str,
        replace: bool,
    ) -> FalkorResult<redis::Value> {
        let params = if replace {
            vec!["REPLACE", name, script]
        } else {
            vec![name, script]
        };
        self.borrow_connection()
            .await?
            .execute_command(None, "GRAPH.UDF", Some("LOAD"), Some(&params))
            .await
    }

    /// List User Defined Function (UDF) libraries.
    ///
    /// # Arguments
    /// * `lib`: If provided, filter the list to this specific library.
    /// * `with_code`: If true, include the library source code in the result.
    ///
    /// # Returns
    /// A [`redis::Value`] containing the list of UDF libraries and their metadata.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "List UDF Libraries", skip_all, level = "info")
    )]
    pub async fn udf_list(
        &self,
        lib: Option<&str>,
        with_code: bool,
    ) -> FalkorResult<redis::Value> {
        let mut params = Vec::new();
        if let Some(library) = lib {
            params.push(library);
        }
        if with_code {
            params.push("WITHCODE");
        }

        let params_slice = if params.is_empty() {
            None
        } else {
            Some(params.as_slice())
        };

        self.borrow_connection()
            .await?
            .execute_command(None, "GRAPH.UDF", Some("LIST"), params_slice)
            .await
    }

    /// Flush (remove) all User Defined Function (UDF) libraries.
    ///
    /// # Returns
    /// A [`redis::Value`] indicating the result of the operation.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Flush UDF Libraries", skip_all, level = "info")
    )]
    pub async fn udf_flush(&self) -> FalkorResult<redis::Value> {
        self.borrow_connection()
            .await?
            .execute_command(None, "GRAPH.UDF", Some("FLUSH"), None)
            .await
    }

    /// Delete a User Defined Function (UDF) library.
    ///
    /// # Arguments
    /// * `lib`: The name of the library to delete.
    ///
    /// # Returns
    /// A [`redis::Value`] indicating the result of the operation.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Delete UDF Library", skip_all, level = "info")
    )]
    pub async fn udf_delete(
        &self,
        lib: &str,
    ) -> FalkorResult<redis::Value> {
        self.borrow_connection()
            .await?
            .execute_command(None, "GRAPH.UDF", Some("DELETE"), Some(&[lib]))
            .await
    }
}

/// An inner client whose provider cannot open connections, for tests that need any reconnect
/// attempt to fail.
#[cfg(test)]
pub(crate) fn create_empty_inner_async_client() -> Arc<FalkorAsyncClientInner> {
    Arc::new(FalkorAsyncClientInner {
        _inner: Mutex::new(FalkorClientProvider::None),
        strategy: ConnectionStrategy::Pooled {
            size: std::num::NonZeroU8::MIN,
        },
        primary: AsyncExecutor::Pooled(
            FalkorAsyncClient::pool_from_connections(Vec::new()).expect("an empty pool builds"),
        ),
        readonly: None,
        retry_policy: RetryPolicy::disabled(),
        query_logging: false,
        read_preference: ReadPreference::Primary,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_utils::{
            create_async_test_client, default_on_connection_down, imdb_async_test_client,
            open_empty_async_test_graph, read_copied_actors, retry_until_async_fn_with_timeout,
            udf_script, TestAsyncGraphHandle, UdfRegistryLock, COPY_RETRY_TIMEOUT,
        },
        FalkorClientBuilder,
    };
    use futures::StreamExt;
    use std::{
        mem,
        num::{NonZeroU8, NonZeroUsize},
        thread,
    };
    use tokio::sync::mpsc::error::TryRecvError;

    #[test]
    fn falkor_async_client_is_clone_send_sync() {
        // The struct's doc comment promises the client can be cloned and shared across threads.
        // Assert those bounds at compile time so the impls (notably `Clone`) cannot silently regress.
        fn assert_clone_send_sync<T: Clone + Send + Sync>() {}
        assert_clone_send_sync::<FalkorAsyncClient>();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiplexed_strategy_with_max_inflight() {
        // An explicit multiplexed strategy combined with a bounded in-flight limit
        // exercises the `ConnectionManager` concurrency-limit configuration path and the
        // multiplexed executor build loop.
        let client = FalkorClientBuilder::new_async()
            .with_connection_strategy(ConnectionStrategy::Multiplexed {
                connections: NonZeroU8::new(2).expect("Could not create a perfectly valid u8"),
            })
            .with_max_inflight(
                NonZeroUsize::new(4).expect("Could not create a perfectly valid usize"),
            )
            .build()
            .await
            .expect("Could not build a bounded multiplexed client");

        assert!(
            matches!(
                client.connection_strategy(),
                ConnectionStrategy::Multiplexed { .. }
            ),
            "An explicit multiplexed strategy must be preserved on a single-node deployment"
        );
        assert_eq!(client.connection_pool_size(), 2);
        assert!(
            matches!(&client.inner.primary, AsyncExecutor::Multiplexed(_)),
            "The primary executor must be multiplexed"
        );

        // A real round-trip proves the bounded multiplexed sockets route responses.
        let mut graph = client.select_graph("test_multiplexed_max_inflight");
        graph
            .query("CREATE (:N {v: 1})")
            .execute()
            .await
            .expect("Could not create node over the multiplexed client");
        let mut result = graph
            .ro_query("MATCH (n:N) RETURN count(n)")
            .execute()
            .await
            .expect("Could not read over the multiplexed client");
        assert!(
            result.data.next().await.is_some(),
            "Expected the multiplexed read-only query to return a row"
        );
        graph.delete().await.ok();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_with_num_connections_preserves_multiplexed() {
        // `with_num_connections` updates the count of the builder's active strategy; on
        // the async builder the default strategy is multiplexed, so the multiplexed arm
        // of `with_connection_count` is exercised.
        let client = FalkorClientBuilder::new_async()
            .with_num_connections(NonZeroU8::new(3).expect("Could not create a perfectly valid u8"))
            .build()
            .await
            .expect("Could not build a multiplexed client with a custom socket count");

        assert!(matches!(
            client.connection_strategy(),
            ConnectionStrategy::Multiplexed { .. }
        ));
        assert_eq!(client.connection_pool_size(), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_borrow_connection() {
        let client = FalkorClientBuilder::new_async()
            .with_connection_strategy(ConnectionStrategy::Pooled {
                size: NonZeroU8::new(6).expect("Could not create a perfectly valid u8"),
            })
            .build()
            .await
            .expect("Could not create client for this test");

        // Client was created with 6 connections
        let mut conn_vec = Vec::with_capacity(6);
        for _ in 0..6 {
            let conn = client.borrow_connection().await;
            assert!(conn.is_ok());
            conn_vec.push(conn);
        }

        let AsyncExecutor::Pooled(pool) = &client.inner.primary else {
            panic!("Expected a pooled primary executor");
        };
        let non_existing_conn = pool.rx.lock().await.try_recv();
        assert!(non_existing_conn.is_err());

        let Err(TryRecvError::Empty) = non_existing_conn else {
            panic!("Got error, but not a TryRecvError::Empty, as expected");
        };
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_borrowed_connection_returns_to_pool_on_drop() {
        // A borrowed pooled connection must be returned to the pool when it is dropped
        // without calling `execute_command`. Otherwise the pool would permanently leak
        // connections and eventually deadlock future borrows.
        let client = FalkorClientBuilder::new_async()
            .with_connection_strategy(ConnectionStrategy::Pooled {
                size: NonZeroU8::new(2).expect("Could not create a perfectly valid u8"),
            })
            .build()
            .await
            .expect("Could not build a pooled client");

        // Drain the entire pool by borrowing every connection.
        let mut borrowed = Vec::with_capacity(2);
        for _ in 0..2 {
            borrowed.push(client.borrow_connection().await.expect("Could not borrow"));
        }

        let AsyncExecutor::Pooled(pool) = &client.inner.primary else {
            panic!("Expected a pooled primary executor");
        };
        assert!(
            matches!(pool.rx.lock().await.try_recv(), Err(TryRecvError::Empty)),
            "The pool must be empty while every connection is borrowed"
        );

        // Drop the borrows without ever calling `execute_command`.
        drop(borrowed);

        let mut returned = 0;
        let mut rx = pool.rx.lock().await;
        while rx.try_recv().is_ok() {
            returned += 1;
        }
        assert_eq!(
            returned, 2,
            "Every dropped borrow must be returned to the pool"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replica_reads_available_single_node() {
        // On a single-node (non-Sentinel) deployment there are no readable
        // replicas, so read-only queries transparently fall back to the primary
        // pool and `replica_reads_available` reports false.
        let client = create_async_test_client().await;
        assert!(
            !client.replica_reads_available(),
            "A single-node deployment must not have replica connections available"
        );

        let mut graph = client.select_graph("test_replica_reads_available_single_node_async");
        graph
            .query("CREATE (n:Person {name: 'Jane Doe', age: 25})")
            .execute()
            .await
            .expect("Could not create Jane");

        // Read-only query still succeeds via the primary fallback.
        let mut result = graph
            .ro_query("MATCH (n:Person {name: 'Jane Doe'}) RETURN n.age")
            .execute()
            .await
            .expect("Could not read Jane via ro_query");
        assert!(
            result.data.next().await.is_some(),
            "Expected the read-only query to return Jane"
        );

        graph.delete().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_readonly_pool_none_without_replica() {
        // Without a Sentinel replica, building a read-only executor fails (no fallback
        // to primary), so `create` leaves the read-only route empty and reads are
        // served by the primary executor.
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
        assert!(FalkorAsyncClient::build_executor(
            &mut provider,
            ConnectionStrategy::Pooled {
                size: NonZeroU8::new(4).unwrap()
            },
            None,
            true,
        )
        .await
        .is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_readonly_pool_none_when_replica_unreachable() {
        // With a replica-typed Sentinel client that cannot be reached, building the
        // read-only executor must fail rather than fall back to primary connections, so
        // the read-only route is never populated with primary connections.
        use std::str::FromStr;
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
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: None,
            connect_timeout: None,
        };
        assert!(provider.has_sentinel_replica());
        assert!(FalkorAsyncClient::build_executor(
            &mut provider,
            ConnectionStrategy::Pooled {
                size: NonZeroU8::new(4).unwrap()
            },
            None,
            true,
        )
        .await
        .is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_pool_from_connections_and_borrow_readonly() {
        // Exercise the read-only routing plumbing (filling the read-only executor,
        // borrowing from it, reporting it exists, and the replica-only inner getter)
        // without needing a live replica deployment. Primary connections are used purely
        // as placeholders for the pool slots.
        let mut provider = FalkorClientProvider::Redis {
            client: redis::Client::open("redis://127.0.0.1:6379").unwrap(),
            sentinel: None,
            sentinel_replica: None,
            #[cfg(feature = "embedded-core")]
            embedded_server: None,
            response_timeout: None,
            connect_timeout: None,
        };
        let readonly_conn = provider
            .get_async_connection()
            .await
            .expect("primary connection");
        let readonly = AsyncExecutor::Pooled(
            FalkorAsyncClient::pool_from_connections(vec![readonly_conn])
                .expect("read-only pool should build"),
        );

        let primary_conn = provider
            .get_async_connection()
            .await
            .expect("primary connection");
        let primary = AsyncExecutor::Pooled(
            FalkorAsyncClient::pool_from_connections(vec![primary_conn])
                .expect("primary pool should build"),
        );

        let inner = Arc::new(FalkorAsyncClientInner {
            _inner: Mutex::new(provider),
            strategy: ConnectionStrategy::Pooled {
                size: NonZeroU8::new(1).unwrap(),
            },
            primary,
            readonly: Some(readonly),
            retry_policy: RetryPolicy::disabled(),
            query_logging: false,
            read_preference: ReadPreference::Primary,
        });

        assert!(inner.has_readonly_pool());

        // The inner replica getter forwards to the provider and errors (no fallback)
        // when no replica Sentinel is configured.
        assert!(matches!(
            inner.get_async_replica_connection().await,
            Err(FalkorDBError::UnavailableProvider)
        ));

        // Borrowing routes to the read-only executor.
        let borrowed = inner
            .borrow_readonly_connection(inner.clone())
            .await
            .expect("should borrow from the read-only executor");
        drop(borrowed);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_redis_info() {
        let info = create_async_test_client()
            .await
            .redis_info(Some("server"))
            .await
            .expect("INFO server");
        assert!(info.contains_key("redis_version"), "{info:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_list_graphs() {
        let client = create_async_test_client().await;
        let res = client.list_graphs().await;
        assert!(res.is_ok());

        let graphs = res.unwrap();
        assert!(graphs.contains(&"imdb".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_select_graph_and_query() {
        let client = imdb_async_test_client().await;

        let mut graph = client.select_graph("imdb");
        assert_eq!(graph.graph_name(), "imdb".to_string());

        let res = graph
            .query("MATCH (a:actor) return a")
            .execute()
            .await
            .expect("Could not get actors from unmodified graph");

        assert_eq!(res.data.collect::<Vec<_>>().await.len(), 1317);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_copy_graph() {
        let client = imdb_async_test_client().await;

        let mut original_graph = client.select_graph("imdb");

        let expected = original_graph
            .query("MATCH (a:actor) RETURN a")
            .execute()
            .await
            .expect("Could not get actors from unmodified graph")
            .data
            .collect::<Vec<_>>()
            .await;

        // Ensure the copied graph is cleaned up even if an assertion panics,
        // so leftover state cannot interfere with other parallel tests.
        let _copy_guard = TestAsyncGraphHandle {
            inner: client.select_graph("imdb_ro_copy_async"),
        };

        // GRAPH.COPY is performed by a background fork on the server; when the
        // server is busy forking for other operations the copy can silently
        // complete empty, and waiting never populates it. A successful copy is
        // visible immediately, so re-issue the copy until the new graph reports
        // the same rows as the source graph.
        let copied = retry_until_async_fn_with_timeout(
            COPY_RETRY_TIMEOUT,
            || async {
                client
                    .select_graph("imdb_ro_copy_async")
                    .delete()
                    .await
                    .ok();
                // A transient `ConnectionDown` retries with a fresh connection; any other error
                // fails fast.
                default_on_connection_down(
                    "Could not copy graph",
                    read_copied_actors(client.copy_graph("imdb", "imdb_ro_copy_async")).await,
                )
            },
            |rows| rows == &expected,
        )
        .await;

        assert_eq!(copied, expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_copy_graph_op_wait() {
        let client = imdb_async_test_client().await;

        let mut original_graph = client.select_graph("imdb");
        let expected = original_graph
            .query("MATCH (a:actor) RETURN a")
            .execute()
            .await
            .expect("Could not get actors from unmodified graph")
            .data
            .collect::<Vec<_>>()
            .await;

        let _copy_guard = TestAsyncGraphHandle {
            inner: client.select_graph("imdb_op_copy_async_wait"),
        };

        // `.wait()` retries only transient fork failures; the rare empty-but-OK copy is still
        // possible, so the outer loop re-issues until the destination matches the source.
        let copied = retry_until_async_fn_with_timeout(
            COPY_RETRY_TIMEOUT,
            || async {
                client
                    .select_graph("imdb_op_copy_async_wait")
                    .delete()
                    .await
                    .ok();
                default_on_connection_down(
                    "Could not copy graph",
                    read_copied_actors(
                        client
                            .copy_graph_op("imdb", "imdb_op_copy_async_wait")
                            .wait(),
                    )
                    .await,
                )
            },
            |rows| rows == &expected,
        )
        .await;

        assert_eq!(copied, expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_copy_graph_op_execute() {
        let client = imdb_async_test_client().await;

        let _copy_guard = TestAsyncGraphHandle {
            inner: client.select_graph("imdb_op_copy_async_execute"),
        };

        let copied = retry_until_async_fn_with_timeout(
            COPY_RETRY_TIMEOUT,
            || async {
                client
                    .select_graph("imdb_op_copy_async_execute")
                    .delete()
                    .await
                    .ok();
                let mut graph = client
                    .copy_graph_op("imdb", "imdb_op_copy_async_execute")
                    .execute()
                    .await?;
                Ok::<_, crate::FalkorDBError>(
                    graph
                        .query("MATCH (a:actor) RETURN a")
                        .execute()
                        .await?
                        .data
                        .collect::<Vec<_>>()
                        .await,
                )
            },
            |rows| matches!(rows, Ok(rows) if !rows.is_empty()),
        )
        .await;

        assert!(!copied.expect("Could not copy graph").is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_config() {
        let client = create_async_test_client().await;

        let config = client
            .config_get("QUERY_MEM_CAPACITY")
            .await
            .expect("Could not get configuration");

        assert_eq!(config.len(), 1);
        assert!(config.contains_key("QUERY_MEM_CAPACITY"));
        assert_eq!(
            mem::discriminant(config.get("QUERY_MEM_CAPACITY").unwrap()),
            mem::discriminant(&ConfigValue::Int64(0))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_config_all() {
        let client = create_async_test_client().await;
        let configuration = client
            .config_get("*")
            .await
            .expect("Could not get configuration");
        assert_eq!(
            configuration.get("THREAD_COUNT").cloned().unwrap(),
            ConfigValue::Int64(thread::available_parallelism().unwrap().get() as i64)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_set_config() {
        let client = create_async_test_client().await;

        let config = client
            .config_get("MAX_QUEUED_QUERIES")
            .await
            .expect("Could not get configuration");

        let current_val = config
            .get("MAX_QUEUED_QUERIES")
            .cloned()
            .unwrap()
            .as_i64()
            .unwrap();

        let desired_val = if current_val == 4294967295 {
            4294967295 / 2
        } else {
            4294967295
        };

        client
            .config_set("MAX_QUEUED_QUERIES", desired_val)
            .await
            .expect("Could not set config value");

        let new_config = client
            .config_get("MAX_QUEUED_QUERIES")
            .await
            .expect("Could not get configuration");

        assert_eq!(
            new_config
                .get("MAX_QUEUED_QUERIES")
                .cloned()
                .unwrap()
                .as_i64()
                .unwrap(),
            desired_val
        );

        client
            .config_set("MAX_QUEUED_QUERIES", current_val)
            .await
            .ok();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_server_error_replies_are_errors() {
        // The error a command failed with (empty if it succeeded). Tests check only a stable part
        // of the server's message, whose exact wording varies between server versions.
        let error =
            |result: FalkorResult<()>| result.err().map(|err| err.to_string()).unwrap_or_default();
        for strategy in [
            ConnectionStrategy::Pooled {
                size: NonZeroU8::MIN,
            },
            ConnectionStrategy::Multiplexed {
                connections: NonZeroU8::MIN,
            },
        ] {
            let client = FalkorClientBuilder::new_async()
                .with_connection_strategy(strategy)
                .build()
                .await
                .expect("Could not build the client");

            // These replies used to be ignored, so the commands reported success.
            let copy = error(
                client
                    .copy_graph("rs_missing_graph", "rs_missing_graph_copy")
                    .await
                    .map(|_| ()),
            );
            assert!(
                copy.contains("Invalid graph operation on empty key"),
                "{strategy:?}: {copy:?}"
            );
            let delete = error(client.select_graph("rs_missing_graph").delete().await);
            assert!(
                delete.contains("Invalid graph operation on empty key"),
                "{strategy:?}: {delete:?}"
            );
            let config = error(client.config_set("NO_SUCH_CONFIG_KEY", 1).await.map(|_| ()));
            assert!(
                config.contains("Unknown configuration field"),
                "{strategy:?}: {config:?}"
            );

            // A query error keeps the server's whole message, first word included.
            let mut graph = client.select_graph("test_server_error_replies_async");
            let query = error(graph.query("RETURN 1/0").execute().await.map(|_| ()));
            assert!(
                query.contains("Division by zero"),
                "{strategy:?}: {query:?}"
            );
            graph.delete().await.ok();
        }
    }

    /// The single integer `query` returns.
    async fn scalar(
        graph: &mut AsyncGraph,
        query: &str,
    ) -> FalkorResult<i64> {
        let mut result = graph.query(query).execute().await?;
        result
            .data
            .next()
            .await
            .expect("a row")
            .and_then(|row| row.try_get_at::<i64>(0))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_udf_operations() {
        let _registry = UdfRegistryLock::acquire();
        let client = create_async_test_client().await;
        let mut graph = open_empty_async_test_graph("test_udf_operations_async").await;

        client
            .udf_load("rs_udf_async_ops", &udf_script("add", "a + b"), false)
            .await
            .expect("Failed to load UDF library");
        assert_eq!(
            scalar(&mut graph.inner, "RETURN rs_udf_async_ops.add(1, 2)").await,
            Ok(3)
        );
        assert!(
            client.udf_list(None, false).await.is_ok(),
            "Failed to list UDF libraries"
        );
        assert!(
            client
                .udf_list(Some("rs_udf_async_ops"), true)
                .await
                .is_ok(),
            "Failed to list UDF library with code"
        );

        client
            .udf_delete("rs_udf_async_ops")
            .await
            .expect("Failed to delete UDF library");
        // Once deleted, the library's function is gone and the library cannot be deleted again.
        assert!(
            scalar(&mut graph.inner, "RETURN rs_udf_async_ops.add(3, 4)")
                .await
                .is_err()
        );
        assert!(client.udf_delete("rs_udf_async_ops").await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_udf_load_replace() {
        let _registry = UdfRegistryLock::acquire();
        let client = create_async_test_client().await;
        let mut graph = open_empty_async_test_graph("test_udf_load_replace_async").await;

        client
            .udf_load("rs_udf_async_replace", &udf_script("calc", "a * 2"), false)
            .await
            .expect("Failed to load UDF library");
        assert_eq!(
            scalar(&mut graph.inner, "RETURN rs_udf_async_replace.calc(5, 0)").await,
            Ok(10)
        );

        // An existing library is only overwritten when `replace` is set.
        let updated = udf_script("calc", "a * 3");
        assert!(client
            .udf_load("rs_udf_async_replace", &updated, false)
            .await
            .is_err());
        client
            .udf_load("rs_udf_async_replace", &updated, true)
            .await
            .expect("Failed to replace UDF library");
        assert_eq!(
            scalar(&mut graph.inner, "RETURN rs_udf_async_replace.calc(5, 1)").await,
            Ok(15)
        );

        client
            .udf_delete("rs_udf_async_replace")
            .await
            .expect("Failed to delete UDF library");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_udf_flush() {
        let _registry = UdfRegistryLock::acquire();
        let client = create_async_test_client().await;

        client
            .udf_load("rs_udf_async_flush", &udf_script("f", "a"), false)
            .await
            .expect("Failed to load UDF library");
        client
            .udf_flush()
            .await
            .expect("Failed to flush UDF libraries");
        assert_eq!(
            client.udf_list(None, false).await,
            Ok(redis::Value::Array(vec![])),
            "flush removes every library"
        );
    }
}
