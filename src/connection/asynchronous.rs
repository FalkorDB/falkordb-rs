/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::FalkorValue;

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
        params: Option<String>,
    ) -> anyhow::Result<FalkorValue> {
        Ok(match self {
            #[cfg(feature = "redis")]
            FalkorAsyncConnection::Redis(redis_conn) => {
                redis::FromRedisValue::from_owned_redis_value(
                    redis_conn
                        .send_packed_command(redis::cmd(command).arg(graph_name).arg(params))
                        .await?,
                )?
            }
        })
    }
}
