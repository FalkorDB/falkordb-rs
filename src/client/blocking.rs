/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::FalkorClientProvider,
    connection::blocking::{BorrowedSyncConnection, FalkorSyncConnection},
    parser::utils::string_vec_from_val,
    ConfigValue, FalkorConnectionInfo, FalkorDBError, FalkorResult, FalkorValue, SyncGraph,
};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::HashMap,
    sync::{mpsc, Arc},
};

pub(crate) struct FalkorSyncClientInner {
    _inner: Mutex<FalkorClientProvider>,

    connection_pool_size: u8,
    connection_pool_tx: RwLock<mpsc::SyncSender<FalkorSyncConnection>>,
    connection_pool_rx: Mutex<mpsc::Receiver<FalkorSyncConnection>>,
}

impl FalkorSyncClientInner {
    pub(crate) fn borrow_connection(
        &self,
        pool_owner: Arc<Self>,
    ) -> FalkorResult<BorrowedSyncConnection> {
        Ok(BorrowedSyncConnection::new(
            self.connection_pool_rx
                .lock()
                .recv()
                .map_err(|_| FalkorDBError::EmptyConnection)?,
            self.connection_pool_tx.read().clone(),
            pool_owner,
        ))
    }

    pub(crate) fn get_connection(&self) -> FalkorResult<FalkorSyncConnection> {
        self._inner.lock().get_connection()
    }
}

#[cfg(feature = "redis")]
fn is_sentinel(conn: &mut FalkorSyncConnection) -> FalkorResult<bool> {
    let info_map = conn.get_redis_info(Some("server"))?;
    Ok(info_map
        .get("redis_mode")
        .map(|redis_mode| redis_mode == "sentinel")
        .unwrap_or_default())
}

#[cfg(feature = "redis")]
pub(crate) fn get_sentinel_client(
    client: &mut FalkorClientProvider,
    connection_info: &redis::ConnectionInfo,
) -> FalkorResult<Option<redis::sentinel::SentinelClient>> {
    let mut conn = client.get_connection()?;
    if !is_sentinel(&mut conn)? {
        return Ok(None);
    }

    // This could have been so simple using the Sentinel API, but it requires a service name
    // Perhaps in the future we can use it if we only support the master instance to be called 'master'?
    let sentinel_masters = conn
        .execute_command(None, "SENTINEL", Some("MASTERS"), None)?
        .into_vec()?;

    if sentinel_masters.len() != 1 {
        return Err(FalkorDBError::SentinelMastersCount);
    }

    let sentinel_master: HashMap<_, _> = sentinel_masters
        .into_iter()
        .next()
        .ok_or(FalkorDBError::SentinelMastersCount)?
        .into_vec()?
        .chunks_exact(2)
        .flat_map(|chunk| TryInto::<[FalkorValue; 2]>::try_into(chunk.to_vec()))
        .flat_map(|[key, val]| {
            Result::<_, FalkorDBError>::Ok((key.into_string()?, val.into_string()?))
        })
        .collect();

    let name = sentinel_master
        .get("name")
        .ok_or(FalkorDBError::SentinelMastersCount)?;

    Ok(Some(
        redis::sentinel::SentinelClient::build(
            vec![connection_info.to_owned()],
            name.to_string(),
            Some(redis::sentinel::SentinelNodeConnectionInfo {
                tls_mode: match connection_info.addr {
                    redis::ConnectionAddr::TcpTls { insecure: true, .. } => {
                        Some(redis::TlsMode::Insecure)
                    }
                    redis::ConnectionAddr::TcpTls {
                        insecure: false, ..
                    } => Some(redis::TlsMode::Secure),
                    _ => None,
                },
                redis_connection_info: Some(connection_info.redis.clone()),
            }),
            redis::sentinel::SentinelServerType::Master,
        )
        .map_err(|err| FalkorDBError::SentinelConnection(err.to_string()))?,
    ))
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

    pub(crate) fn borrow_connection(&self) -> FalkorResult<BorrowedSyncConnection> {
        self.inner.borrow_connection(self.inner.clone())
    }

    /// Return a list of graphs currently residing in the database
    ///
    /// # Returns
    /// A [`Vec`] of [`String`]s, containing the names of available graphs
    pub fn list_graphs(&self) -> FalkorResult<Vec<String>> {
        let mut conn = self.borrow_connection()?;
        conn.execute_command(None, "GRAPH.LIST", None, None)
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
    pub fn config_get(
        &self,
        config_key: &str,
    ) -> FalkorResult<HashMap<String, ConfigValue>> {
        let mut conn = self.borrow_connection()?;
        let config = conn
            .execute_command(None, "GRAPH.CONFIG", Some("GET"), Some(&[config_key]))?
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
    pub fn config_set<C: Into<ConfigValue>>(
        &self,
        config_key: &str,
        value: C,
    ) -> FalkorResult<FalkorValue> {
        self.borrow_connection()?.execute_command(
            None,
            "GRAPH.CONFIG",
            Some("SET"),
            Some(&[config_key, value.into().to_string().as_str()]),
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

    #[cfg(feature = "redis")]
    /// Retrieves redis information
    pub fn redis_info(
        &self,
        section: Option<&str>,
    ) -> FalkorResult<HashMap<String, String>> {
        self.borrow_connection()?
            .as_inner()?
            .get_redis_info(section)
    }
}

pub(crate) fn create_empty_inner_client() -> Arc<FalkorSyncClientInner> {
    let (tx, rx) = mpsc::sync_channel(1);
    tx.send(FalkorSyncConnection::None).ok();
    Arc::new(FalkorSyncClientInner {
        _inner: Mutex::new(FalkorClientProvider::None),
        connection_pool_size: 0,
        connection_pool_tx: RwLock::new(tx),
        connection_pool_rx: Mutex::new(rx),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_utils::{create_test_client, TestSyncGraphHandle},
        FalkorClientBuilder,
    };
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
        assert_eq!(graphs[0], "imdb");
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
}
