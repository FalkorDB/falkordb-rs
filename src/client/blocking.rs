/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::FalkorClientProvider,
    connection::blocking::{BorrowedSyncConnection, FalkorSyncConnection},
    parser::utils::string_vec_from_val,
    ConfigValue, FalkorConnectionInfo, FalkorDBError, FalkorValue, GraphSchema, SyncGraph,
};
use anyhow::Result;
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    fmt::Display,
    sync::{mpsc, Arc},
    time::Duration,
};

pub(crate) struct FalkorSyncClientInner {
    _inner: Mutex<FalkorClientProvider>,
    connection_pool_size: u8,
    connection_pool_tx: mpsc::SyncSender<FalkorSyncConnection>,
    connection_pool_rx: Mutex<mpsc::Receiver<FalkorSyncConnection>>,
}

impl FalkorSyncClientInner {
    pub(crate) fn borrow_connection(&self) -> Result<BorrowedSyncConnection> {
        Ok(BorrowedSyncConnection::new(
            self.connection_pool_rx.lock().recv()?,
            self.connection_pool_tx.clone(),
        ))
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
    pub(crate) _connection_info: FalkorConnectionInfo,
}

impl FalkorSyncClient {
    pub(crate) fn create(
        client: FalkorClientProvider,
        connection_info: FalkorConnectionInfo,
        num_connections: u8,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let (connection_pool_tx, connection_pool_rx) = mpsc::sync_channel(num_connections as usize);
        for _ in 0..num_connections {
            connection_pool_tx.send(client.get_connection(timeout)?)?;
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

    /// Get the max number of connections in the client's connection pool
    pub fn connection_pool_size(&self) -> u8 {
        self.inner.connection_pool_size
    }

    pub(crate) fn borrow_connection(&self) -> Result<BorrowedSyncConnection> {
        self.inner.borrow_connection()
    }

    /// Return a list of graphs currently residing in the database
    ///
    /// # Returns
    /// A [`Vec`] of [`String`]s, containing the names of available graphs
    pub fn list_graphs(&self) -> Result<Vec<String>> {
        let mut conn = self.borrow_connection()?;
        conn.send_command::<&str>(None, "GRAPH.LIST", None, None)
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
    pub fn config_get<T: Display>(
        &self,
        config_key: T,
    ) -> Result<HashMap<String, ConfigValue>> {
        let mut conn = self.borrow_connection()?;
        let config = conn
            .send_command(None, "GRAPH.CONFIG", Some("GET"), Some(&[config_key]))?
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
    pub fn config_set<T: Into<ConfigValue>, C: Into<ConfigValue>>(
        &self,
        config_key: T,
        value: C,
    ) -> Result<FalkorValue> {
        self.borrow_connection()?.send_command(
            None,
            "GRAPH.CONFIG",
            Some("SET"),
            Some(&[config_key.into(), value.into()]),
        )
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
        SyncGraph {
            client: self.inner.clone(),
            graph_name: graph_name.to_string(),
            graph_schema: GraphSchema::new(graph_name.to_string()), // Required for requesting refreshes
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
    pub fn copy_graph(
        &self,
        graph_to_clone: &str,
        new_graph_name: &str,
    ) -> Result<SyncGraph> {
        self.borrow_connection()?.send_command(
            Some(graph_to_clone),
            "GRAPH.COPY",
            None,
            Some(&[new_graph_name]),
        )?;
        Ok(self.select_graph(new_graph_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_utils::{create_test_client, TestSyncGraphHandle},
        FalkorClientBuilder,
    };
    use std::{mem, sync::mpsc::TryRecvError, thread};

    #[test]
    fn test_borrow_connection() {
        let client = FalkorClientBuilder::new()
            .with_num_connections(6)
            .build()
            .expect("Could not create client for this test");

        // Client was created with 6 connections
        let _conn_vec: Vec<Result<BorrowedSyncConnection, anyhow::Error>> = (0..6)
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
        assert_eq!(graphs[0], "imdb");
    }

    #[test]
    fn test_select_graph_and_query() {
        let client = create_test_client();

        let mut graph = client.select_graph("imdb");
        assert_eq!(graph.graph_name(), "imdb".to_string());

        let res = graph
            .query("MATCH (a:actor) return a".to_string())
            .expect("Could not get actors from unmodified graph");

        assert_eq!(res.data.len(), 1317);
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
                .query("MATCH (a:actor) RETURN a".to_string())
                .expect("Could not get actors from unmodified graph")
                .data,
            original_graph
                .query("MATCH (a:actor) RETURN a".to_string())
                .expect("Could not get actors from unmodified graph")
                .data
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
}
