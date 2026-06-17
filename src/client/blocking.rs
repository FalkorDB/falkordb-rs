/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    client::{FalkorClientProvider, ProvidesSyncConnections},
    connection::blocking::{BorrowedSyncConnection, FalkorSyncConnection},
    parser::{parse_config_hashmap, redis_value_as_untyped_string_vec},
    ConfigValue, FalkorConnectionInfo, FalkorDBError, FalkorResult, SyncGraph,
};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{mpsc, Arc},
};

/// A connection pool holding a fixed number of connections that callers borrow
/// from and return to. Used both for the primary pool and the optional
/// replica-routed read-only pool.
pub(crate) struct SyncConnectionPool {
    tx: mpsc::SyncSender<FalkorSyncConnection>,
    rx: Mutex<mpsc::Receiver<FalkorSyncConnection>>,
}

/// A user-opaque inner struct, containing the actual implementation of the blocking client
/// The idea is that each member here is either Copy, or locked in some form, and the public struct only has an Arc to this struct
/// allowing thread safe operations and cloning
pub(crate) struct FalkorSyncClientInner {
    _inner: Mutex<FalkorClientProvider>,

    connection_pool_size: u8,
    connection_pool_tx: mpsc::SyncSender<FalkorSyncConnection>,
    connection_pool_rx: Mutex<mpsc::Receiver<FalkorSyncConnection>>,
    /// Dedicated pool serving read-only queries from replica nodes. `None` when the
    /// deployment has no readable replicas, in which case read-only queries reuse
    /// the primary pool (preserving the previous behavior).
    readonly_pool: Option<SyncConnectionPool>,
}

impl FalkorSyncClientInner {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Borrow Connection From Connection Pool",
            skip_all,
            level = "debug"
        )
    )]
    pub(crate) fn borrow_connection(
        &self,
        pool_owner: Arc<Self>,
    ) -> FalkorResult<BorrowedSyncConnection> {
        Ok(BorrowedSyncConnection::new(
            self.connection_pool_rx
                .lock()
                .recv()
                .map_err(|_| FalkorDBError::EmptyConnection)?,
            self.connection_pool_tx.clone(),
            pool_owner,
            false,
        ))
    }

    /// Borrow a connection for a read-only query. When a replica-routed read-only
    /// pool exists the connection is taken from it (serving the query from a
    /// replica), otherwise it falls back to the primary pool.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Borrow Readonly Connection From Connection Pool",
            skip_all,
            level = "debug"
        )
    )]
    pub(crate) fn borrow_readonly_connection(
        &self,
        pool_owner: Arc<Self>,
    ) -> FalkorResult<BorrowedSyncConnection> {
        match &self.readonly_pool {
            Some(pool) => Ok(BorrowedSyncConnection::new(
                pool.rx
                    .lock()
                    .recv()
                    .map_err(|_| FalkorDBError::EmptyConnection)?,
                pool.tx.clone(),
                pool_owner,
                true,
            )),
            None => self.borrow_connection(pool_owner),
        }
    }

    /// Whether read-only queries are routed to replica nodes for this client.
    pub(crate) fn has_readonly_pool(&self) -> bool {
        self.readonly_pool.is_some()
    }

    /// Obtain a fresh connection routed to a replica node without fallback.
    /// Used for read-only pool creation and reconnection so the read-only pool
    /// never receives primary connections.
    pub(crate) fn get_replica_connection(&self) -> FalkorResult<FalkorSyncConnection> {
        self._inner.lock().get_replica_connection()
    }
}

impl ProvidesSyncConnections for FalkorSyncClientInner {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Get New Sync Connection From Client",
            skip_all,
            level = "info"
        )
    )]
    fn get_connection(&self) -> FalkorResult<FalkorSyncConnection> {
        self._inner.lock().get_connection()
    }
}

/// This is the publicly exposed API of the sync Falkor Client
/// It makes no assumptions in regard to which database the Falkor module is running on,
/// and will select it based on enabled features and url connection
///
/// # Thread Safety
/// This struct is fully thread safe, it can be cloned and passed between threads without constraints,
/// Its API uses only immutable references
#[derive(Clone)]
pub struct FalkorSyncClient {
    inner: Arc<FalkorSyncClientInner>,
    _connection_info: FalkorConnectionInfo,
}

impl FalkorSyncClient {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Create Sync Client", skip_all, level = "info")
    )]
    pub(crate) fn create(
        mut client: FalkorClientProvider,
        connection_info: FalkorConnectionInfo,
        num_connections: u8,
    ) -> FalkorResult<Self> {
        let (connection_pool_tx, connection_pool_rx) = mpsc::sync_channel(num_connections as usize);

        // One already exists
        for _ in 0..num_connections {
            let new_conn = client
                .get_connection()
                .map_err(|err| FalkorDBError::RedisError(err.to_string()))?;

            connection_pool_tx
                .send(new_conn)
                .map_err(|_| FalkorDBError::EmptyConnection)?;
        }

        let readonly_pool = Self::create_readonly_pool(&mut client, num_connections);

        Ok(Self {
            inner: Arc::new(FalkorSyncClientInner {
                _inner: client.into(),
                connection_pool_size: num_connections,
                connection_pool_tx,
                connection_pool_rx: Mutex::new(connection_pool_rx),
                readonly_pool,
            }),
            _connection_info: connection_info,
        })
    }

    /// Build the optional replica-routed read-only pool. Returns `None` when the
    /// deployment exposes no readable replicas, or (best-effort) when the replica
    /// connections cannot currently be established, so read-only queries transparently
    /// fall back to the primary pool.
    fn create_readonly_pool(
        client: &mut FalkorClientProvider,
        num_connections: u8,
    ) -> Option<SyncConnectionPool> {
        if !client.has_sentinel_replica() {
            return None;
        }

        let mut connections = Vec::with_capacity(num_connections as usize);
        for _ in 0..num_connections {
            connections.push(client.get_replica_connection().ok()?);
        }
        Self::pool_from_connections(connections)
    }

    /// Build a [`SyncConnectionPool`] pre-filled with the given connections. Returns
    /// `None` if the connections cannot be enqueued.
    fn pool_from_connections(connections: Vec<FalkorSyncConnection>) -> Option<SyncConnectionPool> {
        let (tx, rx) = mpsc::sync_channel(connections.len().max(1));
        for conn in connections {
            if tx.send(conn).is_err() {
                return None;
            }
        }

        Some(SyncConnectionPool {
            tx,
            rx: Mutex::new(rx),
        })
    }

    ///  Get the max number of connections in the client's connection pool
    pub fn connection_pool_size(&self) -> u8 {
        self.inner.connection_pool_size
    }

    /// Whether read-only queries (`ro_query` / `call_procedure_ro`) are routed to
    /// replica nodes. This is `true` only for Redis Sentinel deployments that expose
    /// readable replicas; otherwise read-only queries are served by the primary.
    pub fn reads_from_replicas(&self) -> bool {
        self.inner.has_readonly_pool()
    }

    pub(crate) fn borrow_connection(&self) -> FalkorResult<BorrowedSyncConnection> {
        self.inner.borrow_connection(self.inner.clone())
    }

    /// Return a list of graphs currently residing in the database
    ///
    /// # Returns
    /// A [`Vec`] of [`String`]s, containing the names of available graphs
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "List Graphs", skip_all, level = "info")
    )]
    pub fn list_graphs(&self) -> FalkorResult<Vec<String>> {
        let mut conn = self.borrow_connection()?;
        conn.execute_command(None, "GRAPH.LIST", None, None)
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
    pub fn config_get(
        &self,
        config_key: &str,
    ) -> FalkorResult<HashMap<String, ConfigValue>> {
        self.borrow_connection()
            .and_then(|mut conn| {
                conn.execute_command(None, "GRAPH.CONFIG", Some("GET"), Some(&[config_key]))
            })
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
    pub fn config_set<C: Into<ConfigValue>>(
        &self,
        config_key: &str,
        value: C,
    ) -> FalkorResult<redis::Value> {
        self.borrow_connection().and_then(|mut conn| {
            conn.execute_command(
                None,
                "GRAPH.CONFIG",
                Some("SET"),
                Some(&[config_key, value.into().to_string().as_str()]),
            )
        })
    }

    /// Opens a graph context for queries and operations
    ///
    /// # Arguments
    /// * `graph_name`: A string identifier of the graph to open.
    ///
    /// # Returns
    /// a [`SyncGraph`] object, allowing various graph operations.
    pub fn select_graph<T: ToString>(
        &self,
        graph_name: T,
    ) -> SyncGraph {
        SyncGraph::new(self.inner.clone(), graph_name)
    }

    /// Copies an entire graph and returns the [`SyncGraph`] for the new copied graph.
    ///
    /// # Arguments
    /// * `graph_to_clone`: A string identifier of the graph to copy.
    /// * `new_graph_name`: The name to give the new graph.
    ///
    /// # Returns
    /// If successful, will return the new [`SyncGraph`] object.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Copy Graph", skip_all, level = "info")
    )]
    pub fn copy_graph(
        &self,
        graph_to_clone: &str,
        new_graph_name: &str,
    ) -> FalkorResult<SyncGraph> {
        self.borrow_connection()?.execute_command(
            Some(graph_to_clone),
            "GRAPH.COPY",
            None,
            Some(&[new_graph_name]),
        )?;
        Ok(self.select_graph(new_graph_name))
    }

    /// Retrieves redis information
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Client Get Redis Info", skip_all, level = "info")
    )]
    pub fn redis_info(
        &self,
        section: Option<&str>,
    ) -> FalkorResult<HashMap<String, String>> {
        self.borrow_connection()?
            .as_inner()?
            .get_redis_info(section)
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
    pub fn udf_load(
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
        self.borrow_connection()?
            .execute_command(None, "GRAPH.UDF", Some("LOAD"), Some(&params))
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
    pub fn udf_list(
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

        self.borrow_connection()?
            .execute_command(None, "GRAPH.UDF", Some("LIST"), params_slice)
    }

    /// Flush (remove) all User Defined Function (UDF) libraries.
    ///
    /// # Returns
    /// A [`redis::Value`] indicating the result of the operation.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Flush UDF Libraries", skip_all, level = "info")
    )]
    pub fn udf_flush(&self) -> FalkorResult<redis::Value> {
        self.borrow_connection()?
            .execute_command(None, "GRAPH.UDF", Some("FLUSH"), None)
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
    pub fn udf_delete(
        &self,
        lib: &str,
    ) -> FalkorResult<redis::Value> {
        self.borrow_connection()?
            .execute_command(None, "GRAPH.UDF", Some("DELETE"), Some(&[lib]))
    }
}

#[cfg(test)]
pub(crate) fn create_empty_inner_sync_client() -> Arc<FalkorSyncClientInner> {
    let (tx, rx) = mpsc::sync_channel(1);
    tx.send(FalkorSyncConnection::None).ok();
    Arc::new(FalkorSyncClientInner {
        _inner: Mutex::new(FalkorClientProvider::None),
        connection_pool_size: 0,
        connection_pool_tx: tx,
        connection_pool_rx: Mutex::new(rx),
        readonly_pool: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FalkorValue::Node;
    use crate::{
        test_utils::{
            create_test_client, imdb_test_client, retry_until_with_timeout, TestSyncGraphHandle,
            COPY_RETRY_TIMEOUT,
        },
        FalkorClientBuilder, FalkorValue, LazyResultSet, QueryResult,
    };
    use approx::assert_relative_eq;
    use std::{mem, num::NonZeroU8, sync::mpsc::TryRecvError, thread};

    #[test]
    fn test_borrow_connection() {
        let client = FalkorClientBuilder::new()
            .with_num_connections(NonZeroU8::new(6).expect("Could not create a perfectly valid u8"))
            .build()
            .expect("Could not create client for this test");

        // Client was created with 6 connections
        let _conn_vec: Vec<FalkorResult<BorrowedSyncConnection>> = (0..6)
            .map(|_| {
                let conn = client.borrow_connection();
                assert!(conn.is_ok());
                conn
            })
            .collect();

        let non_existing_conn = client.inner.connection_pool_rx.lock().try_recv();
        assert!(non_existing_conn.is_err());

        let Err(TryRecvError::Empty) = non_existing_conn else {
            panic!("Got error, but not a TryRecvError::Empty, as expected");
        };
    }

    #[test]
    fn test_list_graphs() {
        let client = create_test_client();
        let res = client.list_graphs();
        assert!(res.is_ok());

        let graphs = res.unwrap();
        assert!(graphs.contains(&"imdb".to_string()));
    }

    #[test]
    fn test_read_only_query() {
        let client = create_test_client();
        let mut graph = client.select_graph("test_read_only_query");
        graph
            .query("CREATE (n:Person {name: 'John Doe', age: 30})")
            .execute()
            .expect("Could not create John");

        // test ro_query with a read query
        graph
            .ro_query("MATCH (n:Person {name: 'John Doe', age: 30}) RETURN n")
            .execute()
            .expect("Could not read John");

        // test ro_query with a write query
        let result = graph
            .ro_query("CREATE (n:Person {name: 'John Doe', age: 30})")
            .execute();
        assert!(
            result.is_err(),
            "Expected an error for write operation in read-only query"
        );
        if let Err(e) = result {
            assert!(
                e.to_string()
                    .contains("is to be executed only on read-only queries"),
                "Unexpected error message: {}",
                e
            );
        }

        graph.delete().unwrap();
    }

    #[test]
    fn test_reads_from_replicas_single_node() {
        // On a single-node (non-Sentinel) deployment there are no readable
        // replicas, so read-only queries transparently fall back to the primary
        // pool and `reads_from_replicas` reports false.
        let client = create_test_client();
        assert!(
            !client.reads_from_replicas(),
            "A single-node deployment must not route reads to replicas"
        );

        let mut graph = client.select_graph("test_reads_from_replicas_single_node");
        graph
            .query("CREATE (n:Person {name: 'Jane Doe', age: 25})")
            .execute()
            .expect("Could not create Jane");

        // Read-only query still succeeds via the primary fallback.
        let mut result = graph
            .ro_query("MATCH (n:Person {name: 'Jane Doe'}) RETURN n.age")
            .execute()
            .expect("Could not read Jane via ro_query");
        assert!(
            result.data.next().is_some(),
            "Expected the read-only query to return Jane"
        );

        graph.delete().unwrap();
    }

    #[test]
    fn test_create_readonly_pool_none_without_replica() {
        // Without a Sentinel replica, no read-only pool is created and reads are
        // served by the primary pool.
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let mut provider = FalkorClientProvider::Redis {
            client,
            sentinel: None,
            sentinel_replica: None,
            #[cfg(feature = "embedded")]
            embedded_server: None,
        };
        assert!(FalkorSyncClient::create_readonly_pool(&mut provider, 4).is_none());
    }

    #[test]
    fn test_create_readonly_pool_none_when_replica_unreachable() {
        // With a replica-typed Sentinel client that cannot be reached, pool creation
        // must fail (return None) rather than fall back to primary connections, so the
        // read-only pool is never populated with primary connections.
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
        assert!(FalkorSyncClient::create_readonly_pool(&mut provider, 4).is_none());
    }

    #[test]
    fn test_inner_get_replica_connection_errors_without_replica() {
        // The inner client wrapper forwards to the provider's replica-only getter,
        // which errors (no fallback) when no replica Sentinel is configured.
        let inner = create_empty_inner_sync_client();
        let result = inner.get_replica_connection();
        assert!(matches!(result, Err(FalkorDBError::UnavailableProvider)));
    }

    #[test]
    fn test_pool_from_connections_and_borrow_readonly() {
        // Exercise the read-only pool plumbing (filling the pool, borrowing from it,
        // and returning the connection) with placeholder connections, so the success
        // path is covered without needing a live replica deployment.
        let pool = FalkorSyncClient::pool_from_connections(vec![
            FalkorSyncConnection::None,
            FalkorSyncConnection::None,
        ])
        .expect("pool should build from connections");

        let (tx, rx) = mpsc::sync_channel(1);
        tx.send(FalkorSyncConnection::None).ok();
        let inner = Arc::new(FalkorSyncClientInner {
            _inner: Mutex::new(FalkorClientProvider::None),
            connection_pool_size: 0,
            connection_pool_tx: tx,
            connection_pool_rx: Mutex::new(rx),
            readonly_pool: Some(pool),
        });

        assert!(inner.has_readonly_pool());

        // Borrowing routes to the read-only pool; dropping returns the connection to it.
        let borrowed = inner
            .borrow_readonly_connection(inner.clone())
            .expect("should borrow from the read-only pool");
        drop(borrowed);
        let borrowed_again = inner
            .borrow_readonly_connection(inner.clone())
            .expect("returned connection should be reusable");
        drop(borrowed_again);
    }

    #[test]
    fn test_read_vec32() {
        let client = create_test_client();
        let mut graph = client.select_graph("test_read_vec32");
        graph
            .query("CREATE (p:Document {embedding: vecf32([2.1, 0.82, 1.3]), id: '1'})")
            .execute()
            .expect("Could not create document with embedding");
        let mut res: QueryResult<LazyResultSet> = graph
            .query("MATCH (p:Document) RETURN p")
            .execute()
            .expect("Could not get document");
        for falkor_value in res.data.by_ref() {
            // iterate on a node value
            for value in falkor_value {
                if let Node(node) = value {
                    if let FalkorValue::Vec32(embedding) = &node.properties["embedding"] {
                        assert_eq!(embedding.values.len(), 3);
                        // compare each embedding value with expected value
                        for (actual, expected) in
                            embedding.values.iter().zip([2.1, 0.82, 1.3].iter())
                        {
                            assert_relative_eq!(actual, expected, epsilon = 1e-6);
                        }
                    } else {
                        panic!("Could not get embedding");
                    }
                } else {
                    panic!("MATCH should return a node");
                }
            }
        }
    }

    #[test]
    fn test_select_graph_and_query() {
        let client = imdb_test_client();

        let mut graph = client.select_graph("imdb");
        assert_eq!(graph.graph_name(), "imdb".to_string());

        let res = graph
            .query("MATCH (a:actor) return a")
            .execute()
            .expect("Could not get actors from unmodified graph");

        assert_eq!(res.data.collect::<Vec<_>>().len(), 1317);
    }

    #[test]
    fn test_copy_graph() {
        let client = imdb_test_client();

        let mut original_graph = client.select_graph("imdb");

        let expected = original_graph
            .query("MATCH (a:actor) RETURN a")
            .execute()
            .expect("Could not get actors from unmodified graph")
            .data
            .collect::<Vec<_>>();

        // Ensure the copied graph is cleaned up even if an assertion panics,
        // so leftover state cannot interfere with other parallel tests.
        let _copy_guard = TestSyncGraphHandle {
            inner: client.select_graph("imdb_ro_copy"),
        };

        // GRAPH.COPY is performed by a background fork on the server; when the
        // server is busy forking for other operations the copy can silently
        // complete empty, and waiting never populates it. A successful copy is
        // visible immediately, so re-issue the copy until the new graph reports
        // the same rows as the source graph.
        let copied = retry_until_with_timeout(
            COPY_RETRY_TIMEOUT,
            || {
                client.select_graph("imdb_ro_copy").delete().ok();
                let mut graph = client
                    .copy_graph("imdb", "imdb_ro_copy")
                    .expect("Could not copy graph");
                graph
                    .query("MATCH (a:actor) RETURN a")
                    .execute()
                    .expect("Could not get actors from copied graph")
                    .data
                    .collect::<Vec<_>>()
            },
            |rows| rows == &expected,
        );

        assert_eq!(copied, expected);
    }

    #[test]
    fn test_copy_graph_op_wait() {
        let client = imdb_test_client();

        let mut original_graph = client.select_graph("imdb");
        let expected = original_graph
            .query("MATCH (a:actor) RETURN a")
            .execute()
            .expect("Could not get actors from unmodified graph")
            .data
            .collect::<Vec<_>>();

        let _copy_guard = TestSyncGraphHandle {
            inner: client.select_graph("imdb_op_copy_wait"),
        };

        // `.wait()` retries only transient fork failures; the rare empty-but-OK copy is still
        // possible, so the outer loop re-issues until the destination matches the source.
        let copied = retry_until_with_timeout(
            COPY_RETRY_TIMEOUT,
            || {
                client.select_graph("imdb_op_copy_wait").delete().ok();
                let mut graph = client
                    .copy_graph_op("imdb", "imdb_op_copy_wait")
                    .wait()
                    .expect("Could not copy graph");
                graph
                    .query("MATCH (a:actor) RETURN a")
                    .execute()
                    .expect("Could not get actors from copied graph")
                    .data
                    .collect::<Vec<_>>()
            },
            |rows| rows == &expected,
        );

        assert_eq!(copied, expected);
    }

    #[test]
    fn test_copy_graph_op_execute() {
        let client = imdb_test_client();

        let _copy_guard = TestSyncGraphHandle {
            inner: client.select_graph("imdb_op_copy_execute"),
        };

        let copied = retry_until_with_timeout(
            COPY_RETRY_TIMEOUT,
            || {
                client.select_graph("imdb_op_copy_execute").delete().ok();
                client
                    .copy_graph_op("imdb", "imdb_op_copy_execute")
                    .execute()
                    .and_then(|mut graph| {
                        Ok(graph
                            .query("MATCH (a:actor) RETURN a")
                            .execute()?
                            .data
                            .collect::<Vec<_>>())
                    })
            },
            |rows| matches!(rows, Ok(rows) if !rows.is_empty()),
        );

        assert!(!copied.expect("Could not copy graph").is_empty());
    }

    #[test]
    fn test_get_config() {
        let client = create_test_client();

        let config = client
            .config_get("QUERY_MEM_CAPACITY")
            .expect("Could not get configuration");

        assert_eq!(config.len(), 1);
        assert!(config.contains_key("QUERY_MEM_CAPACITY"));
        assert_eq!(
            mem::discriminant(config.get("QUERY_MEM_CAPACITY").unwrap()),
            mem::discriminant(&ConfigValue::Int64(0))
        );
    }

    #[test]
    fn test_get_config_all() {
        let client = create_test_client();
        let configuration = client.config_get("*").expect("Could not get configuration");
        assert_eq!(
            configuration.get("THREAD_COUNT").cloned().unwrap(),
            ConfigValue::Int64(thread::available_parallelism().unwrap().get() as i64)
        );
    }

    #[test]
    fn test_set_config() {
        let client = create_test_client();

        let config = client
            .config_get("DELTA_MAX_PENDING_CHANGES")
            .expect("Could not get configuration");

        let current_val = config
            .get("DELTA_MAX_PENDING_CHANGES")
            .cloned()
            .unwrap()
            .as_i64()
            .unwrap();

        let desired_val = if current_val == 10000 { 50000 } else { 10000 };

        client
            .config_set("DELTA_MAX_PENDING_CHANGES", desired_val)
            .expect("Could not set config value");

        let new_config = client
            .config_get("DELTA_MAX_PENDING_CHANGES")
            .expect("Could not get configuration");

        assert_eq!(
            new_config
                .get("DELTA_MAX_PENDING_CHANGES")
                .cloned()
                .unwrap()
                .as_i64()
                .unwrap(),
            desired_val
        );

        client
            .config_set("DELTA_MAX_PENDING_CHANGES", current_val)
            .ok();
    }

    #[test]
    fn test_udf_operations() {
        let client = create_test_client();

        // Test UDF load
        let script = r#"
#!js api_version=1.0 name=mylib

redis.registerFunction('my_func', function(a, b) {
    return a + b;
});
"#;

        // Load a UDF library
        let result = client.udf_load("mylib", script, false);
        assert!(result.is_ok(), "Failed to load UDF library: {:?}", result);

        // List UDF libraries
        let list_result = client.udf_list(None, false);
        assert!(list_result.is_ok(), "Failed to list UDF libraries");

        // List specific library with code
        let list_with_code = client.udf_list(Some("mylib"), true);
        assert!(
            list_with_code.is_ok(),
            "Failed to list UDF library with code"
        );

        // Delete the UDF library
        let delete_result = client.udf_delete("mylib");
        assert!(delete_result.is_ok(), "Failed to delete UDF library");

        // Verify library was deleted
        let list_after_delete = client.udf_list(None, false);
        assert!(
            list_after_delete.is_ok(),
            "Failed to list UDF libraries after delete"
        );
    }

    #[test]
    fn test_udf_load_replace() {
        let client = create_test_client();

        let script = r#"
#!js api_version=1.0 name=replacelib

redis.registerFunction('func1', function(x) {
    return x * 2;
});
"#;

        // Load a UDF library
        let result = client.udf_load("replacelib", script, false);
        assert!(result.is_ok(), "Failed to load UDF library");

        let updated_script = r#"
#!js api_version=1.0 name=replacelib

redis.registerFunction('func1', function(x) {
    return x * 3;
});
"#;

        // Replace the library
        let replace_result = client.udf_load("replacelib", updated_script, true);
        assert!(replace_result.is_ok(), "Failed to replace UDF library");

        // Clean up
        client.udf_delete("replacelib").ok();
    }

    #[test]
    fn test_udf_flush() {
        let client = create_test_client();

        let script = r#"
#!js api_version=1.0 name=flushlib

redis.registerFunction('test_func', function() {
    return 42;
});
"#;

        // Load a UDF library
        client.udf_load("flushlib", script, false).ok();

        // Flush all UDF libraries
        let flush_result = client.udf_flush();
        assert!(flush_result.is_ok(), "Failed to flush UDF libraries");

        // Verify all libraries were flushed
        let list_after_flush = client.udf_list(None, false);
        assert!(
            list_after_flush.is_ok(),
            "Failed to list UDF libraries after flush"
        );
    }
}
