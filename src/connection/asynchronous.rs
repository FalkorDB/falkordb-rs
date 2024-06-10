/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::asynchronous::FalkorAsyncClientInner, connection::utils::map_redis_error,
    FalkorDBError, FalkorResult, FalkorValue,
};
use std::{collections::HashMap, convert::TryInto, sync::Arc};
use tokio::sync::mpsc;

pub(crate) enum FalkorAsyncConnection {
    #[allow(unused)]
    None,
    #[cfg(feature = "redis")]
    Redis(redis::aio::MultiplexedConnection),
}

impl FalkorAsyncConnection {
    pub(crate) async fn execute_command(
        &mut self,
        graph_name: Option<&str>,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[&str]>,
    ) -> FalkorResult<FalkorValue> {
        match self {
            #[cfg(feature = "redis")]
            FalkorAsyncConnection::Redis(redis_conn) => {
                let mut cmd = redis::cmd(command);
                cmd.arg(subcommand);
                cmd.arg(graph_name);
                if let Some(params) = params {
                    for param in params {
                        cmd.arg(param.to_string());
                    }
                }
                redis::FromRedisValue::from_owned_redis_value(
                    redis_conn
                        .send_packed_command(&cmd)
                        .await
                        .map_err(map_redis_error)?,
                )
                .map_err(|err| FalkorDBError::RedisParsingError(err.to_string()))
            }
            FalkorAsyncConnection::None => Ok(FalkorValue::None),
        }
    }

    #[cfg(feature = "redis")]
    pub(crate) async fn get_redis_info(
        &mut self,
        section: Option<&str>,
    ) -> FalkorResult<HashMap<String, String>> {
        Ok(self
            .execute_command(None, "INFO", section, None)
            .await?
            .into_string()?
            .split("\r\n")
            .map(|info_item| info_item.split(':').collect::<Vec<_>>())
            .flat_map(TryInto::<[&str; 2]>::try_into)
            .map(|[key, val]| (key.to_string(), val.to_string()))
            .collect())
    }

    #[cfg(feature = "redis")]
    pub(crate) async fn check_is_redis_sentinel(&mut self) -> FalkorResult<bool> {
        let info_map = self.get_redis_info(Some("server")).await?;
        Ok(info_map
            .get("redis_mode")
            .map(|redis_mode| redis_mode == "sentinel")
            .unwrap_or_default())
    }
}

/// A container for a connection that is borrowed from the pool.
/// Upon going out of scope, it will return the connection to the pool.
///
/// This is publicly exposed for user-implementations of [`FalkorParsable`](crate::FalkorParsable)
pub struct BorrowedAsyncConnection {
    conn: Option<FalkorAsyncConnection>,
    return_tx: mpsc::Sender<FalkorAsyncConnection>,
    client: Arc<FalkorAsyncClientInner>,
}

impl BorrowedAsyncConnection {
    pub(crate) fn new(
        conn: FalkorAsyncConnection,
        return_tx: mpsc::Sender<FalkorAsyncConnection>,
        client: Arc<FalkorAsyncClientInner>,
    ) -> Self {
        Self {
            conn: Some(conn),
            return_tx,
            client,
        }
    }

    pub(crate) fn as_inner(&mut self) -> FalkorResult<&mut FalkorAsyncConnection> {
        self.conn.as_mut().ok_or(FalkorDBError::EmptyConnection)
    }

    pub(crate) async fn execute_command(
        mut self,
        graph_name: Option<&str>,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[&str]>,
    ) -> FalkorResult<FalkorValue> {
        let res = match self
            .as_inner()?
            .execute_command(graph_name, command, subcommand, params)
            .await
        {
            Err(FalkorDBError::ConnectionDown) => {
                if let Ok(new_conn) = self.client.get_async_connection().await {
                    self.conn = Some(new_conn);
                    return Err(FalkorDBError::ConnectionDown);
                }
                Err(FalkorDBError::NoConnection)
            }
            res => res,
        };

        self.return_to_pool().await;
        res
    }

    pub(crate) async fn return_to_pool(self) {
        if let Some(conn) = self.conn {
            self.return_tx.send(conn).await.ok();
        }
    }
}
