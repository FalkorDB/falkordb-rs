/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::FalkorClientProvider, connection::asynchronous::BorrowedAsyncConnection, AsyncGraph,
    AsyncGraphSchema, ConfigValue, FalkorAsyncConnection, FalkorConnectionInfo, FalkorDBError,
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
    connection_pool: Arc<Mutex<VecDeque<FalkorAsyncConnection>>>,
}

impl FalkorAsyncClientInner {
    pub(crate) async fn borrow_connection(&self) -> Result<BorrowedAsyncConnection> {
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
        let connection_pool = Arc::new(Mutex::new(VecDeque::with_capacity(
            num_connections as usize,
        )));
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

    /// Get the max number of connections in the client's connection pool
    pub fn connection_pool_size(&self) -> u8 {
        self.inner.connection_pool_size
    }

    pub(crate) async fn borrow_connection(&self) -> Result<BorrowedAsyncConnection> {
        self.inner.borrow_connection().await
    }

    /// Return a list of graphs currently residing in the database
    ///
    /// # Returns
    /// A [`Vec`] of [`String`]s, containing the names of available graphs
    pub async fn list_graphs(&self) -> Result<Vec<String>> {
        let mut conn = self.borrow_connection().await?;

        Ok(conn
            .send_command(None, "GRAPH.LIST", None, None)
            .await?
            .into_vec()?
            .into_iter()
            .flat_map(|data| data.into_string())
            .collect::<Vec<_>>())
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
        let mut conn = self.borrow_connection().await?;
        Ok(match conn.as_inner()? {
            #[cfg(feature = "redis")]
            FalkorAsyncConnection::Redis(redis_conn) => {
                let bulk_data = match redis_conn
                    .send_packed_command(
                        redis::cmd("GRAPH.CONFIG")
                            .arg("GET")
                            .arg(config_key.to_string()),
                    )
                    .await?
                {
                    redis::Value::Bulk(bulk_data) => bulk_data,
                    _ => {
                        return Err(FalkorDBError::InvalidDataReceived)?;
                    }
                };

                if bulk_data.is_empty() {
                    Err(FalkorDBError::InvalidDataReceived)?;
                } else if bulk_data.len() == 2 {
                    return if let Some(value) = bulk_data.first() {
                        let str_key = match value {
                            redis::Value::Data(bytes_data) => {
                                String::from_utf8_lossy(bytes_data.as_slice()).to_string()
                            }
                            redis::Value::Status(status_data) => status_data.to_owned(),
                            _ => Err(FalkorDBError::InvalidDataReceived)?,
                        };
                        Ok(HashMap::from([(
                            str_key.to_string(),
                            ConfigValue::try_from(&bulk_data[1])?,
                        )]))
                    } else {
                        Err(FalkorDBError::InvalidDataReceived)?
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
        let mut conn = self.borrow_connection().await?;
        match conn.as_inner()? {
            #[cfg(feature = "redis")]
            FalkorAsyncConnection::Redis(redis_conn) => {
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
    /// a [`AsyncGraph`] object, allowing various graph operations.
    pub async fn select_graph<T: ToString>(
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

    /// Copies an entire graph and returns the [`AsyncGraph`] for the new copied graph.
    ///
    /// # Arguments
    /// * `graph_to_clone`: A string identifier of the graph to copy.
    /// * `new_graph_name`: The name to give the new graph.
    ///
    /// # Returns
    /// If successful, will return the new [`AsyncGraph`] object.
    pub async fn copy_graph<T: ToString, Z: ToString>(
        &self,
        graph_to_clone: T,
        new_graph_name: Z,
    ) -> Result<AsyncGraph> {
        self.borrow_connection()
            .await?
            .send_command(
                Some(graph_to_clone.to_string()),
                "GRAPH.COPY",
                None,
                Some(&[new_graph_name.to_string()]),
            )
            .await?;
        Ok(self.select_graph(new_graph_name).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::create_async_test_client;
    use std::{mem, thread};

    #[tokio::test]
    async fn test_async_list_graphs() {
        let client = create_async_test_client().await;
        let res = client.list_graphs().await;
        assert!(res.is_ok());

        let graphs = res.unwrap();
        assert_eq!(graphs[0], "imdb");
    }

    #[tokio::test]
    async fn test_async_select_graph_and_query() {
        let client = create_async_test_client().await;

        let graph = client.select_graph("imdb").await;
        assert_eq!(graph.graph_name(), "imdb".to_string());

        let res = graph
            .query("MATCH (a:actor) return a".to_string(), None)
            .await
            .expect("Could not get actors from unmodified graph");

        assert_eq!(res.result_set.len(), 1317);
    }

    #[tokio::test]
    async fn test_async_copy_graph() {
        let client = create_async_test_client().await;

        client
            .select_graph("imdb_async_ro_copy")
            .await
            .delete()
            .await
            .ok();

        let graph = client
            .copy_graph("imdb", "imdb_async_ro_copy")
            .await
            .expect("Could not copy graph");

        let original_graph = client.select_graph("imdb").await;

        assert_eq!(
            graph
                .query("MATCH (a:actor) RETURN a".to_string(), None)
                .await
                .expect("Could not get actors from unmodified graph")
                .result_set,
            original_graph
                .query("MATCH (a:actor) RETURN a".to_string(), None)
                .await
                .expect("Could not get actors from unmodified graph")
                .result_set
        )
    }

    #[tokio::test]
    async fn test_async_get_config() {
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

    #[tokio::test]
    async fn test_async_get_config_all() {
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

    #[tokio::test]
    async fn test_async_set_config() {
        let client = create_async_test_client().await;

        let config = client
            .config_get("EFFECTS_THRESHOLD")
            .await
            .expect("Could not get configuration");

        let current_val = config
            .get("EFFECTS_THRESHOLD")
            .cloned()
            .unwrap()
            .as_i64()
            .unwrap();

        let desired_val = if current_val == 300 { 250 } else { 300 };

        client
            .config_set("EFFECTS_THRESHOLD", desired_val)
            .await
            .expect("Could not set config value");

        let new_config = client
            .config_get("EFFECTS_THRESHOLD")
            .await
            .expect("Could not get configuration");

        assert_eq!(
            new_config
                .get("EFFECTS_THRESHOLD")
                .cloned()
                .unwrap()
                .as_i64()
                .unwrap(),
            desired_val
        );

        client
            .config_set("EFFECTS_THRESHOLD", current_val)
            .await
            .ok();
    }
}
