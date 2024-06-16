/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::{FalkorClientProvider, ProvidesSyncConnections},
    connection::{
        asynchronous::{BorrowedAsyncConnection, FalkorAsyncConnection},
        blocking::FalkorSyncConnection,
    },
    parser::{parse_config_hashmap, redis_value_as_untyped_string_vec},
    AsyncGraph, ConfigValue, FalkorConnectionInfo, FalkorDBError, FalkorResult,
};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    runtime::Handle,
    sync::{mpsc, Mutex, RwLock},
    task,
};

/// A user-opaque inner struct, containing the actual implementation of the asynchronous client
/// The idea is that each member here is either Copy, or locked in some form, and the public struct only has an Arc to this struct
/// allowing thread safe operations and cloning
pub struct FalkorAsyncClientInner {
    _inner: Mutex<FalkorClientProvider>,

    connection_pool_size: u8,
    connection_pool_tx: RwLock<mpsc::Sender<FalkorAsyncConnection>>,
    connection_pool_rx: Mutex<mpsc::Receiver<FalkorAsyncConnection>>,
}

impl FalkorAsyncClientInner {
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
        Ok(BorrowedAsyncConnection::new(
            self.connection_pool_rx
                .lock()
                .await
                .recv()
                .await
                .ok_or(FalkorDBError::EmptyConnection)?,
            self.connection_pool_tx.read().await.clone(),
            pool_owner,
        ))
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
        task::block_in_place(|| Handle::current().block_on(self._inner.lock())).get_connection()
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
        num_connections: u8,
    ) -> FalkorResult<Self> {
        let (connection_pool_tx, connection_pool_rx) = mpsc::channel(num_connections as usize);

        // One already exists
        for _ in 0..num_connections {
            let new_conn = client
                .get_async_connection()
                .await
                .map_err(|err| FalkorDBError::RedisError(err.to_string()))?;

            connection_pool_tx
                .send(new_conn)
                .await
                .map_err(|_| FalkorDBError::EmptyConnection)?;
        }

        Ok(Self {
            inner: Arc::new(FalkorAsyncClientInner {
                _inner: client.into(),

                connection_pool_size: num_connections,
                connection_pool_tx: RwLock::new(connection_pool_tx),
                connection_pool_rx: Mutex::new(connection_pool_rx),
            }),
            _connection_info: connection_info,
        })
    }

    /// Get the max number of connections in the client's connection pool
    pub fn connection_pool_size(&self) -> u8 {
        self.inner.connection_pool_size
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_utils::{create_async_test_client, TestAsyncGraphHandle},
        FalkorClientBuilder,
    };
    use std::{mem, num::NonZeroU8, thread};
    use tokio::sync::mpsc::error::TryRecvError;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_borrow_connection() {
        let client = FalkorClientBuilder::new_async()
            .with_num_connections(NonZeroU8::new(6).expect("Could not create a perfectly valid u8"))
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

        let non_existing_conn = client.inner.connection_pool_rx.lock().await.try_recv();
        assert!(non_existing_conn.is_err());

        let Err(TryRecvError::Empty) = non_existing_conn else {
            panic!("Got error, but not a TryRecvError::Empty, as expected");
        };
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

        client
            .select_graph("imdb_ro_copy_async")
            .delete()
            .await
            .ok();

        let graph = client.copy_graph("imdb", "imdb_ro_copy_async").await;
        assert!(graph.is_ok());

        let mut graph = TestAsyncGraphHandle {
            inner: graph.unwrap(),
        };

        let mut original_graph = client.select_graph("imdb");

        assert_eq!(
            graph
                .inner
                .query("MATCH (a:actor) RETURN a")
                .execute()
                .await
                .expect("Could not get actors from unmodified graph")
                .data
                .collect::<Vec<_>>(),
            original_graph
                .query("MATCH (a:actor) RETURN a")
                .execute()
                .await
                .expect("Could not get actors from unmodified graph")
                .data
                .collect::<Vec<_>>()
        )
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
}
