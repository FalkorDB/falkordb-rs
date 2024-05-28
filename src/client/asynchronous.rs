/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::FalkorClientProvider, connection::asynchronous::BorrowedAsyncConnection,
    parser::utils::string_vec_from_val, AsyncGraph, ConfigValue, FalkorAsyncConnection,
    FalkorDBError, FalkorValue, GraphSchema,
};
use anyhow::Result;
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

pub struct FalkorAsyncClient {
    inner: Arc<FalkorAsyncClientInner>,
}

impl FalkorAsyncClient {
    pub(crate) async fn create(
        client: FalkorClientProvider,
        num_connections: u8,
        timeout: Option<Duration>,
    ) -> Result<Self> {
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

    pub(crate) async fn borrow_connection(&self) -> Result<BorrowedAsyncConnection> {
        self.inner.borrow_connection().await
    }

    /// Return a list of graphs currently residing in the database
    ///
    /// # Returns
    /// A [`Vec`] of [`String`]s, containing the names of available graphs
    pub async fn list_graphs(&self) -> Result<Vec<String>> {
        let mut conn = self.borrow_connection().await?;
        conn.send_command::<&str>(None, "GRAPH.LIST", None, None)
            .await
            .and_then(|res| string_vec_from_val(res).map_err(Into::into))
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
    ) -> Result<HashMap<String, ConfigValue>> {
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
    ) -> Result<FalkorValue> {
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
    ) -> Result<AsyncGraph> {
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

        let mut graph = client.select_graph("imdb");
        assert_eq!(graph.graph_name(), "imdb".to_string());

        let res = graph
            .query("MATCH (a:actor) return a".to_string())
            .await
            .expect("Could not get actors from unmodified graph");

        assert_eq!(res.data.len(), 1317);
    }

    #[tokio::test]
    async fn test_async_copy_graph() {
        let client = create_async_test_client().await;

        client
            .select_graph("imdb_async_ro_copy")
            .delete()
            .await
            .ok();

        let mut graph = client
            .copy_graph("imdb", "imdb_async_ro_copy")
            .await
            .expect("Could not copy graph");

        let mut original_graph = client.select_graph("imdb");

        assert_eq!(
            graph
                .query("MATCH (a:actor) RETURN a".to_string())
                .await
                .expect("Could not get actors from unmodified graph")
                .data,
            original_graph
                .query("MATCH (a:actor) RETURN a".to_string())
                .await
                .expect("Could not get actors from unmodified graph")
                .data
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
