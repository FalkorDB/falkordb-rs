/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::graph::utils::construct_query;
use crate::FalkorValue;
use anyhow::Result;
use std::collections::HashMap;

#[derive(Clone)]
pub enum FalkorAsyncConnection {
    #[cfg(feature = "redis")]
    Redis(redis::aio::MultiplexedConnection),
}

impl FalkorAsyncConnection {
    pub(crate) async fn send_command(
        &mut self,
        graph_name: Option<String>,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[String]>,
    ) -> Result<FalkorValue> {
        Ok(match self {
            #[cfg(feature = "redis")]
            FalkorAsyncConnection::Redis(redis_conn) => {
                let mut cmd = redis::cmd(command);
                cmd.arg(subcommand);
                cmd.arg(graph_name);
                if let Some(params) = params {
                    for param in params {
                        cmd.arg(param);
                    }
                }
                redis::FromRedisValue::from_owned_redis_value(
                    redis_conn.send_packed_command(&cmd).await?,
                )?
            }
        })
    }

    async fn query_internal<Q: ToString, T: ToString, Z: ToString>(
        &mut self,
        graph_name: String,
        command: &str,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
        timeout: Option<u64>,
    ) -> Result<FalkorValue> {
        let query = construct_query(query_string, params);

        Ok(match self {
            #[cfg(feature = "redis")]
            FalkorAsyncConnection::Redis(redis_conn) => {
                use redis::FromRedisValue;
                let redis_val = redis_conn
                    .send_packed_command(
                        redis::cmd(command)
                            .arg(graph_name.as_str())
                            .arg(query)
                            .arg("--compact")
                            .arg(timeout.map(|timeout| format!("timeout {timeout}"))),
                    )
                    .await?;

                FalkorValue::from_owned_redis_value(redis_val)?
            }
        })
    }

    pub(crate) async fn query<Q: ToString, T: ToString, Z: ToString>(
        &mut self,
        graph_name: String,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
        timeout: Option<u64>,
    ) -> Result<FalkorValue> {
        self.query_internal(graph_name, "GRAPH.QUERY", query_string, params, timeout)
            .await
    }

    pub(crate) async fn query_readonly<Q: ToString, T: ToString, Z: ToString>(
        &mut self,
        graph_name: String,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
        timeout: Option<u64>,
    ) -> Result<FalkorValue> {
        self.query_internal(graph_name, "GRAPH.QUERY_RO", query_string, params, timeout)
            .await
    }
}
