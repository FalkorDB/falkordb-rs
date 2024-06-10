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
    parser::utils::string_vec_from_val,
    AsyncGraph, ConfigValue, FalkorConnectionInfo, FalkorDBError, FalkorResult, FalkorValue,
};
use std::{collections::HashMap, sync::Arc};
use tokio::runtime::Handle;
use tokio::{
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

    pub(crate) async fn get_async_connection(&self) -> FalkorResult<FalkorAsyncConnection> {
        self._inner.lock().await.get_async_connection().await
    }
}

impl ProvidesSyncConnections for FalkorAsyncClientInner {
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
    pub(crate) _connection_info: FalkorConnectionInfo,
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
    pub async fn list_graphs(&self) -> FalkorResult<Vec<String>> {
        self.borrow_connection()
            .await?
            .execute_command(None, "GRAPH.LIST", None, None)
            .await
            .and_then(string_vec_from_val)
    }

    /// Return the current value of a configuration option in the database.
    ///
    /// # Arguments
    /// * `config_Key`: A [`String`] representation of a configuration's key.
    /// The config key can also be "*", which will return ALL the configuration options.
    ///
    /// # Returns
    /// A [`HashMap`] comprised of [`String`] keys, and [`ConfigValue`] values.
    pub async fn config_get(
        &self,
        config_key: &str,
    ) -> FalkorResult<HashMap<String, ConfigValue>> {
        let config = self
            .borrow_connection()
            .await?
            .execute_command(None, "GRAPH.CONFIG", Some("GET"), Some(&[config_key]))
            .await?
            .into_vec()?;

        if config.len() == 2 {
            let [key, val]: [FalkorValue; 2] = config.try_into().map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 2 elements for configuration option".to_string(),
                )
            })?;

            return Ok(HashMap::from([(
                key.into_string()?,
                ConfigValue::try_from(val)?,
            )]));
        }

        Ok(config
            .into_iter()
            .flat_map(|config| {
                let [key, val]: [FalkorValue; 2] = config.into_vec()?.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "Expected exactly 2 elements for configuration option".to_string(),
                    )
                })?;

                Result::<_, FalkorDBError>::Ok((key.into_string()?, ConfigValue::try_from(val)?))
            })
            .collect::<HashMap<String, ConfigValue>>())
    }

    /// Return the current value of a configuration option in the database.
    ///
    /// # Arguments
    /// * `config_Key`: A [`String`] representation of a configuration's key.
    /// The config key can also be "*", which will return ALL the configuration options.
    /// * `value`: The new value to set, which is anything that can be converted into a [`ConfigValue`], namely string types and i64.
    pub async fn config_set<C: Into<ConfigValue>>(
        &self,
        config_key: &str,
        value: C,
    ) -> FalkorResult<FalkorValue> {
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
    /// a [`SyncGraph`] object, allowing various graph operations.
    pub fn select_graph<T: ToString>(
        &self,
        graph_name: T,
    ) -> AsyncGraph {
        AsyncGraph::new(self.inner.clone(), graph_name)
    }

    /// Copies an entire graph and returns the [`SyncGraph`] for the new copied graph.
    ///
    /// # Arguments
    /// * `graph_to_clone`: A string identifier of the graph to copy.
    /// * `new_graph_name`: The name to give the new graph.
    ///
    /// # Returns
    /// If successful, will return the new [`SyncGraph`] object.
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

    #[cfg(feature = "redis")]
    /// Retrieves redis information
    pub async fn redis_info(
        &self,
        section: Option<&str>,
    ) -> FalkorResult<HashMap<String, String>> {
        self.borrow_connection()
            .await?
            .as_inner()?
            .get_redis_info(section)
            .await
    }
}

#[cfg(test)]
pub(crate) async fn create_empty_inner_async_client() -> Arc<FalkorAsyncClientInner> {
    let (tx, rx) = mpsc::channel(1);
    tx.send(FalkorAsyncConnection::None).await.ok();
    Arc::new(FalkorAsyncClientInner {
        _inner: Mutex::new(FalkorClientProvider::None),
        connection_pool_size: 0,
        connection_pool_tx: RwLock::new(tx),
        connection_pool_rx: Mutex::new(rx),
    })
}
