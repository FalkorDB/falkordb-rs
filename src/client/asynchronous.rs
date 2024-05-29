/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::FalkorClientProvider, connection::asynchronous::BorrowedAsyncConnection,
    parser::utils::string_vec_from_val, AsyncGraph, ConfigValue, FalkorAsyncConnection,
    FalkorDBError, FalkorResult, FalkorValue, GraphSchema,
};
use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;

pub(crate) struct FalkorAsyncClientInner {
    _inner: Arc<FalkorClientProvider>,
    connection_pool_size: u8,
    connection_pool: Arc<Mutex<VecDeque<FalkorAsyncConnection>>>,
}

impl FalkorAsyncClientInner {
    pub(crate) async fn borrow_connection(&self) -> FalkorResult<BorrowedAsyncConnection> {
        let mut conn_pool = self.connection_pool.lock().await;
        let connection = conn_pool
            .pop_front()
            .ok_or(FalkorDBError::EmptyConnection)?;

        Ok(BorrowedAsyncConnection {
            conn: Some(connection),
            conn_pool: self.connection_pool.clone(),
        })
    }
}

pub struct FalkorAsyncClient {
    inner: Arc<FalkorAsyncClientInner>,
}

impl FalkorAsyncClient {
    pub(crate) async fn create(
        client: FalkorClientProvider,
        num_connections: u8,
        timeout: Option<Duration>,
    ) -> FalkorResult<Self> {
        let client = Arc::new(client);

        // Wait for all tasks to complete and collect results
        let connection_pool: VecDeque<_> =
            futures::future::join_all((0..num_connections).map(|_| {
                let client = Arc::clone(&client);
                tokio::task::spawn(async move {
                    client.get_async_connection(timeout).await // Replace `timeout` with your actual timeout value
                })
            }))
            .await
            .into_iter()
            .flatten()
            .flatten()
            .collect();

        if connection_pool.len() != num_connections as usize {
            Err(FalkorDBError::NoConnection)?;
        }

        Ok(Self {
            inner: Arc::new(FalkorAsyncClientInner {
                _inner: client,
                connection_pool_size: num_connections,
                connection_pool: Arc::new(Mutex::new(connection_pool)),
            }),
        })
    }

    /// Get the max number of connections in the client's connection pool
    pub fn connection_pool_size(&self) -> u8 {
        self.inner.connection_pool_size
    }

    pub(crate) async fn borrow_connection(&self) -> FalkorResult<BorrowedAsyncConnection> {
        self.inner.borrow_connection().await
    }

    /// Return a list of graphs currently residing in the database
    ///
    /// # Returns
    /// A [`Vec`] of [`String`]s, containing the names of available graphs
    pub async fn list_graphs(&self) -> FalkorResult<Vec<String>> {
        let mut conn = self.borrow_connection().await?;
        conn.send_command::<&str>(None, "GRAPH.LIST", None, None)
            .await
            .and_then(|res| string_vec_from_val(res))
    }

    /// Return the current value of a configuration option in the database.
    ///
    /// # Arguments
    /// * `config_Key`: A [`String`] representation of a configuration's key.
    /// The config key can also be "*", which will return ALL the configuration options.
    ///
    /// # Returns
    /// A [`HashMap`] comprised of [`String`] keys, and [`ConfigValue`] values.
    pub async fn config_get<T: Display>(
        &self,
        config_key: T,
    ) -> FalkorResult<HashMap<String, ConfigValue>> {
        let mut conn = self.borrow_connection().await?;
        let config = conn
            .send_command(None, "GRAPH.CONFIG", Some("GET"), Some(&[config_key]))
            .await?
            .into_vec()?;

        if config.len() == 2 {
            let [key, val]: [FalkorValue; 2] = config
                .try_into()
                .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

            return Ok(HashMap::from([(
                key.into_string()?,
                ConfigValue::try_from(val)?,
            )]));
        }

        Ok(config
            .into_iter()
            .flat_map(|config| {
                let [key, val]: [FalkorValue; 2] = config
                    .into_vec()?
                    .try_into()
                    .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

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
    pub async fn config_set<T: Into<ConfigValue>, C: Into<ConfigValue>>(
        &self,
        config_key: T,
        value: C,
    ) -> FalkorResult<FalkorValue> {
        self.borrow_connection()
            .await?
            .send_command(
                None,
                "GRAPH.CONFIG",
                Some("SET"),
                Some(&[config_key.into(), value.into()]),
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
        AsyncGraph {
            client: self.inner.clone(),
            graph_name: graph_name.to_string(),
            graph_schema: GraphSchema::new(graph_name.to_string()), // Required for requesting refreshes
        }
    }

    /// Copies an entire graph and returns the [`AsyncGraph`] for the new copied graph.
    ///
    /// # Arguments
    /// * `graph_to_clone`: A string identifier of the graph to copy.
    /// * `new_graph_name`: The name to give the new graph.
    ///
    /// # Returns
    /// If successful, will return the new [`AsyncGraph`] object.
    pub async fn copy_graph(
        &self,
        graph_to_clone: &str,
        new_graph_name: &str,
    ) -> FalkorResult<AsyncGraph> {
        self.borrow_connection()
            .await?
            .send_command(
                Some(graph_to_clone),
                "GRAPH.COPY",
                None,
                Some(&[new_graph_name]),
            )
            .await?;
        Ok(self.select_graph(new_graph_name))
    }
}
