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
use parking_lot::Mutex;
use std::{
    collections::HashMap,
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
    pub(crate) fn borrow_connection(&self) -> Result<BorrowedSyncConnection, FalkorDBError> {
        Ok(BorrowedSyncConnection::new(
            self.connection_pool_rx
                .lock()
                .recv()
                .map_err(|_| FalkorDBError::EmptyConnection)?,
            self.connection_pool_tx.clone(),
        ))
    }
}

#[cfg(feature = "redis")]
fn get_redis_info(
    conn: &mut FalkorSyncConnection,
    section: Option<&str>,
) -> FalkorResult<HashMap<String, String>> {
    Ok(conn
        .execute_command(None, "INFO", section, None)?
        .into_string()?
        .split("\r\n")
        .map(|info_item| info_item.split(':').collect::<Vec<_>>())
        .flat_map(TryInto::<[&str; 2]>::try_into)
        .map(|[key, val]| (key.to_string(), val.to_string()))
        .collect())
}

#[cfg(feature = "redis")]
fn is_sentinel(conn: &mut FalkorSyncConnection) -> FalkorResult<bool> {
    let info_map = get_redis_info(conn, Some("server"))?;
    Ok(info_map
        .get("redis_mode")
        .map(|redis_mode| redis_mode == "sentinel")
        .unwrap_or_default())
}

fn get_sentinel_client(
    client: &mut FalkorClientProvider,
    connection_info: &redis::ConnectionInfo,
) -> FalkorResult<Option<redis::Client>> {
    let mut conn = client.get_connection(None)?;
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

    let (_name, host, port) = match (
        sentinel_master.get("name"),
        sentinel_master.get("host"),
        sentinel_master.get("ip"),
        sentinel_master.get("port"),
    ) {
        (Some(name), Some(host), _, Some(port)) => (name, host, port),
        (Some(name), _, Some(ip), Some(port)) => (name, ip, port),
        _ => return Err(FalkorDBError::SentinelMastersCount),
    };

    let user_pass_string = match (
        connection_info.redis.username.as_ref(),
        connection_info.redis.password.as_ref(),
    ) {
        (None, Some(pass)) => format!("{}@", pass), // Password-only authentication is allowed in legacy auth
        (Some(user), Some(pass)) => format!("{user}:{pass}@"),
        _ => "".to_string(),
    };
    let url = format!("{}://{}{host}:{port}", "redis", user_pass_string);

    Ok(Some(redis::Client::open(url).map_err(|err| {
        FalkorDBError::SentinelConnection(err.to_string())
    })?))
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
        timeout: Option<Duration>,
    ) -> FalkorResult<Self> {
        let (connection_pool_tx, connection_pool_rx) = mpsc::sync_channel(num_connections as usize);

        #[cfg(feature = "redis")]
        #[allow(irrefutable_let_patterns)]
        if let FalkorConnectionInfo::Redis(redis_conn_info) = &connection_info {
            if let Some(sentinel) = get_sentinel_client(&mut client, redis_conn_info)? {
                client.set_sentinel(sentinel);
            }
        }

        // One already exists
        for _ in 0..num_connections {
            let new_conn = client
                .get_connection(timeout)
                .map_err(|err| FalkorDBError::RedisConnectionError(err.to_string()))?;

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

    /// Get the max number of connections in the client's connection pool
    pub fn connection_pool_size(&self) -> u8 {
        self.inner.connection_pool_size
    }

    pub(crate) fn borrow_connection(&self) -> FalkorResult<BorrowedSyncConnection> {
        self.inner.borrow_connection()
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
        let mut conn = self.borrow_connection()?;
        get_redis_info(conn.as_inner()?, section)
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
            .perform()
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
                .query("MATCH (a:actor) RETURN a")
                .perform()
                .expect("Could not get actors from unmodified graph")
                .data,
            original_graph
                .query("MATCH (a:actor) RETURN a")
                .perform()
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

    #[cfg(feature = "redis")]
    #[test]
    fn test_redis_sentinel_build() {
        FalkorClientBuilder::new().with_connection_info("falkor://falkordb:123456@singlezonesentinellblb.instance-e0srmv0mc.hc-jx5tis6bc.us-central1.gcp.f2e0a955bb84.cloud:26379".try_into().expect("Could not construct connectioninfo"))
            .build()
            .expect("Could not create sentinel client");
    }
}
