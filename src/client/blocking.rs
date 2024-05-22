/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::FalkorClientImpl,
    connection::blocking::{BorrowedSyncConnection, FalkorSyncConnection},
    graph_schema::blocking::GraphSchema,
    ConfigValue, FalkorDBError, SyncGraph,
};
use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{mpsc, Arc},
};

pub struct SyncFalkorClient {
    _inner: FalkorClientImpl,
    connection_pool_tx: mpsc::SyncSender<FalkorSyncConnection>,
    connection_pool_rx: mpsc::Receiver<FalkorSyncConnection>,
}

unsafe impl Sync for SyncFalkorClient {}
unsafe impl Send for SyncFalkorClient {}

impl SyncFalkorClient {
    pub(crate) fn create(client: FalkorClientImpl, num_connections: u8) -> Result<Arc<Self>> {
        let (connection_pool_tx, connection_pool_rx) = mpsc::sync_channel(num_connections as usize);
        for _ in 0..num_connections {
            connection_pool_tx.send(client.get_connection(None)?)?;
        }

        Ok(Arc::new(Self {
            _inner: client,
            connection_pool_tx,
            connection_pool_rx,
        }))
    }

    pub(crate) fn borrow_connection(&self) -> Result<BorrowedSyncConnection> {
        Ok(BorrowedSyncConnection {
            return_tx: self.connection_pool_tx.clone(),
            conn: Some(self.connection_pool_rx.recv()?),
        })
    }

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

    /// This function returns either an [`FVec`] containing the requested key and val,
    /// or an [`FVec`] of [`FVec`]s, each one containing a key and val pair
    pub fn config_get<T: ToString>(&self, config_key: T) -> Result<HashMap<String, ConfigValue>> {
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

    pub fn open_graph<T: ToString>(&self, graph_name: T) -> SyncGraph {
        SyncGraph {
            client: self,
            graph_name: graph_name.to_string(),
            graph_schema: GraphSchema::new(graph_name.to_string()),
        }
    }
}
