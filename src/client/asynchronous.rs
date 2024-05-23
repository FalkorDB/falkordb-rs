/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::FalkorClientProvider, AsyncGraph, AsyncGraphSchema, ConfigValue, FalkorAsyncConnection,
    FalkorDBError,
};
use anyhow::Result;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

pub struct FalkorAsyncClient {
    _inner: FalkorClientProvider,
    connection: FalkorAsyncConnection,
    graph_cache: HashMap<String, AsyncGraphSchema>,
}

impl FalkorAsyncClient {
    pub(crate) async fn create(client: FalkorClientProvider) -> Result<Arc<Mutex<Self>>> {
        Ok(Arc::new(Mutex::new(Self {
            connection: client.get_async_connection(None).await?,
            _inner: client,
            graph_cache: Default::default(),
        })))
    }
    pub(crate) fn clone_connection(&self) -> FalkorAsyncConnection {
        self.connection.clone()
    }

    pub async fn list_graphs(&self) -> Result<Vec<String>> {
        let conn = self.clone_connection();
        let graph_list = match conn {
            #[cfg(feature = "redis")]
            FalkorAsyncConnection::Redis(mut redis_conn) => {
                let res = match redis::cmd("GRAPH.LIST")
                    .query_async(&mut redis_conn)
                    .await?
                {
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

    pub async fn config_get<T: ToString>(
        &self,
        config_key: T,
    ) -> Result<HashMap<String, ConfigValue>> {
        let conn = self.clone_connection();
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

    pub async fn config_set<T: Into<ConfigValue>, C: Into<ConfigValue>>(
        &self,
        config_key: T,
        value: C,
    ) -> Result<()> {
        let conn = self.clone_connection();
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

    pub fn open_graph<T: ToString>(
        &mut self,
        graph_name: T,
    ) -> AsyncGraph {
        AsyncGraph {
            connection: self.connection.clone(),
            graph_name: graph_name.to_string(),
            graph_schema: self
                .graph_cache
                .entry(graph_name.to_string())
                .or_insert(AsyncGraphSchema::new(graph_name.to_string()))
                .clone(),
        }
    }

    pub async fn copy_graph<T: ToString>(
        &mut self,
        cloned_graph_name: T,
    ) -> Result<AsyncGraph> {
        self.connection
            .clone()
            .send_command(Some(cloned_graph_name.to_string()), "GRAPH.COPY", None)
            .await?;
        Ok(self.open_graph(cloned_graph_name))
    }
}
