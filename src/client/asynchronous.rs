/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    client::{ConnectionStrategy, FalkorClientProvider, ProvidesSyncConnections},
    connection::{
        asynchronous::{BorrowedAsyncConnection, FalkorAsyncConnection},
        blocking::FalkorSyncConnection,
    },
    parser::{parse_config_hashmap, redis_value_as_untyped_string_vec},
    AsyncGraph, ConfigValue, FalkorConnectionInfo, FalkorDBError, FalkorResult,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    sync::{mpsc, Mutex},
    task,
};

/// A connection pool holding a fixed number of async connections that callers
/// borrow from and return to. Used by the [`ConnectionStrategy::Pooled`] strategy for
/// both the primary pool and the optional replica-routed read-only pool.
pub(crate) struct AsyncConnectionPool {
    tx: mpsc::Sender<FalkorAsyncConnection>,
    rx: Mutex<mpsc::Receiver<FalkorAsyncConnection>>,
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
}

impl FalkorAsyncClientInner {
    /// Borrow a connection from the given executor. For the pooled strategy this waits
    /// for an available connection; for the multiplexed strategy it hands out a cheap
    /// clone immediately.
    async fn borrow_from(
        executor: &AsyncExecutor,
        pool_owner: Arc<Self>,
        readonly: bool,
    ) -> FalkorResult<BorrowedAsyncConnection> {
        match executor {
            AsyncExecutor::Pooled(pool) => Ok(BorrowedAsyncConnection::new(
                pool.rx
                    .lock()
                    .await
                    .recv()
                    .await
                    .ok_or(FalkorDBError::EmptyConnection)?,
                pool.tx.clone(),
                pool_owner,
                readonly,
            )),
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

    /// Obtain a replacement connection after a [`ConnectionDown`](FalkorDBError::ConnectionDown),
    /// honoring the active strategy and read-only routing. Multiplexed executors hand
    /// out a fresh clone (the underlying manager reconnects on its own); pooled
    /// executors open a brand new connection.
    pub(crate) async fn fresh_connection(
        &self,
        readonly: bool,
    ) -> FalkorResult<FalkorAsyncConnection> {
        let executor = match (&self.readonly, readonly) {
            (Some(executor), true) => executor,
            _ => &self.primary,
        };
        match executor {
            AsyncExecutor::Multiplexed(executor) => Ok(executor.pick()),
            AsyncExecutor::Pooled(_) => {
                if readonly && self.readonly.is_some() {
                    self.get_async_replica_connection().await
                } else {
                    self.get_async_connection().await
                }
            }
        }
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

impl ProvidesSyncConnections for FalkorAsyncClientInner {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Get New Sync Connection From Client",
            skip_all,
            level = "info"
        )
    )]
    fn get_connection(&self) -> FalkorResult<FalkorSyncConnection> {
        let handle = Handle::try_current().map_err(|_| FalkorDBError::NoRuntime)?;
        match handle.runtime_flavor() {
            RuntimeFlavor::CurrentThread => Err(FalkorDBError::SingleThreadedRuntime),
            _ => task::block_in_place(|| handle.block_on(self._inner.lock())).get_connection(),
        }
    }
}

/// This is the publicly exposed API of the asynchronous Falkor Client
/// It makes no assumptions in regard to which database the Falkor module is running on,
/// and will select it based on enabled features and url connection
///
/// # Thread Safety
/// This struct is fully thread safe, it can be cloned and passed between threads without constraints,
/// Its API uses only immutable references
pub struct FalkorAsyncClient {
    inner: Arc<FalkorAsyncClientInner>,
    _connection_info: FalkorConnectionInfo,
}

impl FalkorAsyncClient {
    pub(crate) async fn create(
        mut client: FalkorClientProvider,
        connection_info: FalkorConnectionInfo,
        requested_strategy: ConnectionStrategy,
        max_inflight: Option<usize>,
    ) -> FalkorResult<Self> {
        // A multiplexed ConnectionManager built from a Sentinel-resolved client pins to a
        // single node and reconnects to the same address rather than re-resolving the
        // current master/replica via Sentinel on failover. The pooled strategy opens a
        // fresh, Sentinel-resolved connection on every (re)connect, so for Sentinel
        // deployments we downgrade to an equivalently-sized pool. `connection_strategy()`
        // reports this effective value.
        let strategy = match requested_strategy {
            ConnectionStrategy::Multiplexed { connections } if client.has_sentinel() => {
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
        max_inflight: Option<usize>,
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
            if tx.try_send(conn).is_err() {
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

    /// Whether read-only queries (`ro_query` / `call_procedure_ro`) are routed to
    /// replica nodes. This is `true` only for Redis Sentinel deployments that expose
    /// readable replicas; otherwise read-only queries are served by the primary.
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
    /// The config key can also be "*", which will return ALL the configuration options.
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
    /// The config key can also be "*", which will return ALL the configuration options.
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
        let mut conn = self.borrow_connection().await?;

        let redis_info = conn.as_inner()?.get_redis_info(section).await;

        conn.return_to_pool().await;

        redis_info
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_utils::{
            create_async_test_client, retry_until_async_fn_with_timeout, TestAsyncGraphHandle,
            COPY_RETRY_TIMEOUT,
        },
        FalkorClientBuilder,
    };
    use std::{mem, num::NonZeroU8, thread};
    use tokio::sync::mpsc::error::TryRecvError;

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
    async fn test_reads_from_replicas_single_node() {
        // On a single-node (non-Sentinel) deployment there are no readable
        // replicas, so read-only queries transparently fall back to the primary
        // pool and `reads_from_replicas` reports false.
        let client = create_async_test_client().await;
        assert!(
            !client.reads_from_replicas(),
            "A single-node deployment must not route reads to replicas"
        );

        let mut graph = client.select_graph("test_reads_from_replicas_single_node_async");
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
            result.data.next().is_some(),
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
            #[cfg(feature = "embedded")]
            embedded_server: None,
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
            #[cfg(feature = "embedded")]
            embedded_server: None,
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
            #[cfg(feature = "embedded")]
            embedded_server: None,
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
    async fn test_list_graphs() {
        let client = create_async_test_client().await;
        let res = client.list_graphs().await;
        assert!(res.is_ok());

        let graphs = res.unwrap();
        assert!(graphs.contains(&"imdb".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_select_graph_and_query() {
        let client = create_async_test_client().await;

        let mut graph = client.select_graph("imdb");
        assert_eq!(graph.graph_name(), "imdb".to_string());

        let res = graph
            .query("MATCH (a:actor) return a")
            .execute()
            .await
            .expect("Could not get actors from unmodified graph");

        assert_eq!(res.data.collect::<Vec<_>>().len(), 1317);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_copy_graph() {
        let client = create_async_test_client().await;

        let mut original_graph = client.select_graph("imdb");

        let expected = original_graph
            .query("MATCH (a:actor) RETURN a")
            .execute()
            .await
            .expect("Could not get actors from unmodified graph")
            .data
            .collect::<Vec<_>>();

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
                let mut graph = client
                    .copy_graph("imdb", "imdb_ro_copy_async")
                    .await
                    .expect("Could not copy graph");
                graph
                    .query("MATCH (a:actor) RETURN a")
                    .execute()
                    .await
                    .expect("Could not get actors from copied graph")
                    .data
                    .collect::<Vec<_>>()
            },
            |rows| rows == &expected,
        )
        .await;

        assert_eq!(copied, expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_copy_graph_op_wait() {
        let client = create_async_test_client().await;

        let mut original_graph = client.select_graph("imdb");
        let expected = original_graph
            .query("MATCH (a:actor) RETURN a")
            .execute()
            .await
            .expect("Could not get actors from unmodified graph")
            .data
            .collect::<Vec<_>>();

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
                let mut graph = client
                    .copy_graph_op("imdb", "imdb_op_copy_async_wait")
                    .wait()
                    .await
                    .expect("Could not copy graph");
                graph
                    .query("MATCH (a:actor) RETURN a")
                    .execute()
                    .await
                    .expect("Could not get actors from copied graph")
                    .data
                    .collect::<Vec<_>>()
            },
            |rows| rows == &expected,
        )
        .await;

        assert_eq!(copied, expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_copy_graph_op_execute() {
        let client = create_async_test_client().await;

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
                        .collect::<Vec<_>>(),
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
    async fn test_udf_operations() {
        let client = create_async_test_client().await;

        // Test UDF load
        let script = r#"
#!js api_version=1.0 name=mylib_async

redis.registerFunction('my_func', function(a, b) {
    return a + b;
});
"#;

        // Load a UDF library
        let result = client.udf_load("mylib_async", script, false).await;
        assert!(result.is_ok(), "Failed to load UDF library: {:?}", result);

        // List UDF libraries
        let list_result = client.udf_list(None, false).await;
        assert!(list_result.is_ok(), "Failed to list UDF libraries");

        // List specific library with code
        let list_with_code = client.udf_list(Some("mylib_async"), true).await;
        assert!(
            list_with_code.is_ok(),
            "Failed to list UDF library with code"
        );

        // Delete the UDF library
        let delete_result = client.udf_delete("mylib_async").await;
        assert!(delete_result.is_ok(), "Failed to delete UDF library");

        // Verify library was deleted
        let list_after_delete = client.udf_list(None, false).await;
        assert!(
            list_after_delete.is_ok(),
            "Failed to list UDF libraries after delete"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_udf_load_replace() {
        let client = create_async_test_client().await;

        let script = r#"
#!js api_version=1.0 name=replacelib_async

redis.registerFunction('func1', function(x) {
    return x * 2;
});
"#;

        // Load a UDF library
        let result = client.udf_load("replacelib_async", script, false).await;
        assert!(result.is_ok(), "Failed to load UDF library");

        let updated_script = r#"
#!js api_version=1.0 name=replacelib_async

redis.registerFunction('func1', function(x) {
    return x * 3;
});
"#;

        // Replace the library
        let replace_result = client
            .udf_load("replacelib_async", updated_script, true)
            .await;
        assert!(replace_result.is_ok(), "Failed to replace UDF library");

        // Clean up
        client.udf_delete("replacelib_async").await.ok();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_udf_flush() {
        let client = create_async_test_client().await;

        let script = r#"
#!js api_version=1.0 name=flushlib_async

redis.registerFunction('test_func', function() {
    return 42;
});
"#;

        // Load a UDF library
        client.udf_load("flushlib_async", script, false).await.ok();

        // Flush all UDF libraries
        let flush_result = client.udf_flush().await;
        assert!(flush_result.is_ok(), "Failed to flush UDF libraries");

        // Verify all libraries were flushed
        let list_after_flush = client.udf_list(None, false).await;
        assert!(
            list_after_flush.is_ok(),
            "Failed to list UDF libraries after flush"
        );
    }
}
