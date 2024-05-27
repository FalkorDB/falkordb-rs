/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::FalkorClientProvider, AsyncGraph, AsyncGraphSchema, ConfigValue, FalkorAsyncConnection,
    FalkorConnectionInfo, FalkorDBError,
};
use anyhow::Result;
use std::{
    collections::{HashMap, VecDeque},
    fmt::{Debug, Formatter},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;

pub(crate) struct FalkorAsyncClientInner {
    _inner: Mutex<FalkorClientProvider>,
    graph_cache: Mutex<HashMap<String, AsyncGraphSchema>>,
    connection_pool_size: u8,
    connection_pool: Mutex<VecDeque<FalkorAsyncConnection>>,
}

impl FalkorAsyncClientInner {
    pub(crate) async fn get_connection(&self) -> Result<FalkorAsyncConnection> {
        let mut conn_pool = self.connection_pool.lock().await;
        let connection = conn_pool
            .pop_front()
            .ok_or(FalkorDBError::EmptyConnection)?;

        let cloned = connection.clone();

        conn_pool.push_back(connection);

        Ok(cloned)
    }
}

unsafe impl Sync for FalkorAsyncClientInner {}
unsafe impl Send for FalkorAsyncClientInner {}

pub struct FalkorAsyncClient {
    inner: Arc<FalkorAsyncClientInner>,
    _connection_info: FalkorConnectionInfo,
}

impl Debug for FalkorAsyncClient {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("FalkorAsyncClient")
            .field("inner", &"<InnerClient>")
            .field("connection_info", &self._connection_info)
            .finish()
    }
}

impl FalkorAsyncClient {
    pub(crate) async fn create(
        client: FalkorClientProvider,
        connection_info: FalkorConnectionInfo,
        num_connections: u8,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let connection_pool = Mutex::new(VecDeque::with_capacity(num_connections as usize));
        {
            let mut connection_pool = connection_pool.lock().await;
            for _ in 0..num_connections {
                connection_pool.push_back(client.get_async_connection(timeout).await?);
            }
        }

        Ok(Self {
            inner: Arc::new(FalkorAsyncClientInner {
                _inner: client.into(),
                graph_cache: Default::default(),
                connection_pool_size: num_connections,
                connection_pool,
            }),
            _connection_info: connection_info,
        })
    }
    pub(crate) async fn get_connection(&self) -> Result<FalkorAsyncConnection> {
        self.inner.get_connection().await
    }

    /// Return a list of graphs currently residing in the database
    ///
    /// # Returns
    /// A [`Vec`] of [`String`]s, containing the names of available graphs
    pub async fn list_graphs(&self) -> Result<Vec<String>> {
        let graph_list = match self.get_connection().await? {
            #[cfg(feature = "redis")]
            FalkorAsyncConnection::Redis(mut redis_conn) => {
                let cmd = redis::cmd("GRAPH.LIST");
                let res = match redis_conn.send_packed_command(&cmd).await {
                    Ok(redis::Value::Bulk(data)) => data,
                    _ => Err(FalkorDBError::InvalidDataReceived)?,
                };

                let mut graph_list = Vec::with_capacity(res.len());
                for graph in res {
                    let graph = match graph {
                        redis::Value::Data(data) => {
                            Ok(String::from_utf8_lossy(data.as_slice()).to_string())
                        }
                        redis::Value::Status(data) => Ok(data),
                        _ => Err(FalkorDBError::ParsingError),
                    }?;

                    graph_list.push(graph);
                }
                graph_list
            }
        };

        Ok(graph_list)
    }

    /// Return the current value of a configuration option in the database.
    ///
    /// # Arguments
    /// * `config_Key`: A [`String`] representation of a configuration's key.
    /// The config key can also be "*", which will return ALL the configuration options.
    ///
    /// # Returns
    /// A [`HashMap`] comprised of [`String`] keys, and [`ConfigValue`] values.
    pub async fn config_get<T: ToString>(
        &self,
        config_key: T,
    ) -> Result<HashMap<String, ConfigValue>> {
        let conn = self.get_connection().await?;
        Ok(match conn {
            #[cfg(feature = "redis")]
            FalkorAsyncConnection::Redis(mut redis_conn) => {
                let bulk_data = match redis_conn
                    .send_packed_command(
                        redis::cmd("GRAPH.CONFIG")
                            .arg("GET")
                            .arg(config_key.to_string()),
                    )
                    .await?
                {
                    redis::Value::Bulk(bulk_data) => bulk_data,
                    _ => return Err(FalkorDBError::InvalidDataReceived.into()),
                };

                if bulk_data.is_empty() {
                    return Err(FalkorDBError::InvalidDataReceived.into());
                } else if bulk_data.len() == 2 {
                    return if let Some(redis::Value::Status(config_key)) = bulk_data.first() {
                        Ok(HashMap::from([(
                            config_key.to_string(),
                            ConfigValue::try_from(&bulk_data[1])?,
                        )]))
                    } else {
                        Err(FalkorDBError::InvalidDataReceived.into())
                    };
                }

                let mut config_map = HashMap::with_capacity(bulk_data.len());
                for raw_map in bulk_data {
                    for (key, val) in raw_map
                        .into_map_iter()
                        .map_err(|_| FalkorDBError::ParsingError)?
                    {
                        let key = match key {
                            redis::Value::Status(config_key) => Ok(config_key),
                            redis::Value::Data(config_key) => {
                                Ok(String::from_utf8_lossy(config_key.as_slice()).to_string())
                            }
                            _ => Err(FalkorDBError::InvalidDataReceived),
                        }?;

                        config_map.insert(key, ConfigValue::try_from(&val)?);
                    }
                }

                config_map
            }
        })
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
    ) -> Result<()> {
        let conn = self.get_connection().await?;
        match conn {
            #[cfg(feature = "redis")]
            FalkorAsyncConnection::Redis(mut redis_conn) => {
                redis_conn
                    .send_packed_command(
                        redis::cmd("GRAPH.CONFIG")
                            .arg("SET")
                            .arg(config_key.into())
                            .arg(value.into()),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    /// Opens a graph context for queries and operations
    ///
    /// # Arguments
    /// * `graph_name`: A string identifier of the graph to open.
    ///
    /// # Returns
    /// a [`SyncGraph`] object, allowing various graph operations.
    pub async fn open_graph<T: ToString>(
        &self,
        graph_name: T,
    ) -> AsyncGraph {
        AsyncGraph {
            client: self.inner.clone(),
            graph_name: graph_name.to_string(),
            graph_schema: self
                .inner
                .graph_cache
                .lock()
                .await
                .entry(graph_name.to_string())
                .or_insert(AsyncGraphSchema::new(graph_name.to_string()))
                .clone(),
        }
    }

    /// Copies an entire graph and returns the [`SyncGraph`] for the new copied graph.
    ///
    /// # Arguments
    /// * `graph_to_clone`: A string identifier of the graph to copy.
    /// * `new_graph_name`: The name to give the new graph.
    ///
    /// # Returns
    /// If successful, will return the new [`SyncGraph`] object.
    pub async fn copy_graph<T: ToString, Z: ToString>(
        &self,
        graph_to_clone: T,
        new_graph_name: Z,
    ) -> Result<AsyncGraph> {
        self.get_connection()
            .await?
            .send_command(
                Some(graph_to_clone.to_string()),
                "GRAPH.COPY",
                None,
                Some(&[new_graph_name.to_string()]),
            )
            .await?;
        Ok(self.open_graph(new_graph_name.to_string()).await)
    }
}
