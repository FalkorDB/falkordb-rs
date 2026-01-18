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

/// A user-opaque inner struct, containing the actual implementation of the blocking client
/// The idea is that each member here is either Copy, or locked in some form, and the public struct only has an Arc to this struct
/// allowing thread safe operations and cloning
pub(crate) struct FalkorSyncClientInner {
    _inner: Mutex<FalkorClientProvider>,

    connection_pool_size: u8,
    connection_pool_tx: mpsc::SyncSender<FalkorSyncConnection>,
    connection_pool_rx: Mutex<mpsc::Receiver<FalkorSyncConnection>>,
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
        ))
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

        Ok(Self {
            inner: Arc::new(FalkorSyncClientInner {
                _inner: client.into(),
                connection_pool_size: num_connections,
                connection_pool_tx,
                connection_pool_rx: Mutex::new(connection_pool_rx),
            }),
            _connection_info: connection_info,
        })
    }

    ///  Get the max number of connections in the client's connection pool
    pub fn connection_pool_size(&self) -> u8 {
        self.inner.connection_pool_size
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
    ///    The config key can also be "*", which will return ALL the configuration options.
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
    ///    The config key can also be "*", which will return ALL the configuration options.
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FalkorValue::Node;
    use crate::{
        test_utils::{create_test_client, TestSyncGraphHandle},
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
        let client = create_test_client();

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
        let client = create_test_client();

        client.select_graph("imdb_ro_copy").delete().ok();

        let graph = client.copy_graph("imdb", "imdb_ro_copy");
        assert!(graph.is_ok());

        let mut graph = TestSyncGraphHandle {
            inner: graph.unwrap(),
        };

        let mut original_graph = client.select_graph("imdb");

        assert_eq!(
            graph
                .inner
                .query("MATCH (a:actor) RETURN a")
                .execute()
                .expect("Could not get actors from unmodified graph")
                .data
                .collect::<Vec<_>>(),
            original_graph
                .query("MATCH (a:actor) RETURN a")
                .execute()
                .expect("Could not get actors from unmodified graph")
                .data
                .collect::<Vec<_>>()
        )
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
