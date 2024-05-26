/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::FalkorClientProvider,
    connection::blocking::{BorrowedSyncConnection, FalkorSyncConnection},
    ConfigValue, FalkorConnectionInfo, FalkorDBError, SyncGraph, SyncGraphSchema,
};
use anyhow::Result;
use parking_lot::Mutex;
use std::fmt::{Debug, Formatter};
use std::time::Duration;
use std::{
    collections::HashMap,
    sync::{mpsc, Arc},
};

pub(crate) struct FalkorSyncClientInner {
    _inner: Mutex<FalkorClientProvider>,
    graph_cache: Mutex<HashMap<String, SyncGraphSchema>>,
    connection_pool_size: u8,
    connection_pool_tx: mpsc::SyncSender<FalkorSyncConnection>,
    connection_pool_rx: mpsc::Receiver<FalkorSyncConnection>,
}

impl FalkorSyncClientInner {
    pub(crate) fn borrow_connection(&self) -> Result<BorrowedSyncConnection> {
        Ok(BorrowedSyncConnection {
            return_tx: self.connection_pool_tx.clone(),
            conn: Some(self.connection_pool_rx.recv()?),
        })
    }
}

unsafe impl Sync for FalkorSyncClientInner {}
unsafe impl Send for FalkorSyncClientInner {}

/// This is the publicly exposed API of the sync Falkor Client
/// It makes no assumptions in regard to which database the Falkor module is running on,
/// and will select it based on enabled features and url connection
///
/// # Thread Safety
/// This struct is fully thread safe, it can be cloned and passed within threads without constraints,
/// Its API uses only immutable references
#[derive(Clone)]
pub struct FalkorSyncClient {
    inner: Arc<FalkorSyncClientInner>,
    pub(crate) _connection_info: FalkorConnectionInfo,
}

impl Debug for FalkorSyncClient {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("FalkorSyncClient")
            .field("inner", &"<InnerClient>")
            .field("connection_info", &self._connection_info)
            .finish()
    }
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
                graph_cache: Default::default(),
                connection_pool_size: num_connections,
                connection_pool_tx,
                connection_pool_rx,
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

        let graph_list = match conn.as_inner()? {
            #[cfg(feature = "redis")]
            FalkorSyncConnection::Redis(redis_conn) => {
                use redis::ConnectionLike as _;
                let res = match redis_conn.req_command(&redis::cmd("GRAPH.LIST"))? {
                    redis::Value::Bulk(data) => data,
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
    pub fn config_get<T: ToString>(
        &self,
        config_key: T,
    ) -> Result<HashMap<String, ConfigValue>> {
        let mut conn = self.borrow_connection()?;

        Ok(match conn.as_inner()? {
            #[cfg(feature = "redis")]
            FalkorSyncConnection::Redis(redis_conn) => {
                use redis::ConnectionLike as _;

                let bulk_data = match redis_conn.req_command(
                    redis::cmd("GRAPH.CONFIG")
                        .arg("GET")
                        .arg(config_key.to_string()),
                )? {
                    redis::Value::Bulk(bulk_data) => bulk_data,
                    _ => return Err(FalkorDBError::InvalidDataReceived.into()),
                };

                if bulk_data.is_empty() {
                    return Err(FalkorDBError::InvalidDataReceived.into());
                } else if bulk_data.len() == 2 {
                    return if let Some(ConfigValue::String(config_key)) = bulk_data
                        .first()
                        .map(ConfigValue::try_from)
                        .and_then(Result::ok)
                    {
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
    pub fn config_set<T: Into<ConfigValue>, C: Into<ConfigValue>>(
        &self,
        config_key: T,
        value: C,
    ) -> Result<()> {
        let mut conn = self.borrow_connection()?;
        match conn.as_inner()? {
            #[cfg(feature = "redis")]
            FalkorSyncConnection::Redis(redis_conn) => {
                use redis::ConnectionLike as _;
                redis_conn.req_command(
                    redis::cmd("GRAPH.CONFIG")
                        .arg("SET")
                        .arg(config_key.into())
                        .arg(value.into()),
                )?;
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
    pub fn open_graph<T: ToString>(
        &self,
        graph_name: T,
    ) -> SyncGraph {
        SyncGraph {
            client: self.inner.clone(),
            graph_name: graph_name.to_string(),
            graph_schema: self
                .inner
                .graph_cache
                .lock()
                .entry(graph_name.to_string())
                .or_insert(SyncGraphSchema::new(graph_name.to_string()))
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
    pub fn copy_graph<T: ToString, Z: ToString>(
        &self,
        graph_to_clone: T,
        new_graph_name: Z,
    ) -> Result<SyncGraph> {
        self.borrow_connection()?.send_command(
            Some(graph_to_clone.to_string()),
            "GRAPH.COPY",
            None,
            Some(&[new_graph_name.to_string()]),
        )?;
        Ok(self.open_graph(new_graph_name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_utils::{create_test_client, TestGraphHandle},
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
            .into_iter()
            .map(|_| {
                let conn = client.borrow_connection();
                assert!(conn.is_ok());
                conn
            })
            .collect();

        let non_existing_conn = client.inner.connection_pool_rx.try_recv();
        assert!(non_existing_conn.is_err());

        if let Err(TryRecvError::Empty) = non_existing_conn {
            return;
        }
        assert!(false);
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
    fn test_open_graph_and_query() {
        let client = create_test_client();

        let graph = client.open_graph("imdb");
        assert_eq!(graph.graph_name(), "imdb".to_string());

        let res = graph
            .query("MATCH (a:actor) return a".to_string(), None)
            .expect("Could not get actors from unmodified graph");

        assert_eq!(res.result_set.len(), 1317);
    }

    #[test]
    fn test_copy_graph() {
        let client = create_test_client();

        client.open_graph("imdb_ro_copy").delete().ok();

        let graph = client.copy_graph("imdb", "imdb_ro_copy");
        assert!(graph.is_ok());

        let graph = TestGraphHandle {
            inner: graph.unwrap(),
        };

        let original_graph = client.open_graph("imdb");

        assert_eq!(
            graph
                .inner
                .query("MATCH (a:actor) RETURN a".to_string(), None)
                .expect("Could not get actors from unmodified graph")
                .result_set,
            original_graph
                .query("MATCH (a:actor) RETURN a".to_string(), None)
                .expect("Could not get actors from unmodified graph")
                .result_set
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
