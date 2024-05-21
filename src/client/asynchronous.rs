/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::client::FalkorClientImpl;
use crate::connection::asynchronous::FalkorAsyncConnection;
use crate::error::FalkorDBError;
use anyhow::Result;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub struct AsyncFalkorClient {
    _inner: FalkorClientImpl,
    connection: FalkorAsyncConnection,
    runtime: Runtime,
}

impl AsyncFalkorClient {
    pub(crate) async fn create(client: FalkorClientImpl, runtime: Runtime) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            connection: client.get_async_connection(None).await?,
            _inner: client,
            runtime,
        }))
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
}
