/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorValue};
use anyhow::Result;
use std::{collections::VecDeque, fmt::Display, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum FalkorAsyncConnection {
    #[cfg(feature = "redis")]
    Redis(redis::aio::MultiplexedConnection),
}

/// A container for a connection that is borrowed from the pool.
/// Upon going out of scope, it will return the connection to the pool.
///
/// This is publicly exposed for user-implementations of [`FalkorParsable`](crate::FalkorParsable)
pub struct BorrowedAsyncConnection {
    pub(crate) conn: Option<FalkorAsyncConnection>,
    pub(crate) conn_pool: Arc<Mutex<VecDeque<FalkorAsyncConnection>>>,
}

impl BorrowedAsyncConnection {
    pub(crate) fn as_inner(&mut self) -> Result<&mut FalkorAsyncConnection, FalkorDBError> {
        self.conn.as_mut().ok_or(FalkorDBError::EmptyConnection)
    }

    pub(crate) async fn send_command<P: Display>(
        &mut self,
        graph_name: Option<&str>,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[P]>,
    ) -> Result<FalkorValue> {
        Ok(match self.as_inner()? {
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
                    redis_conn.send_packed_command(&cmd).await?,
                )?
            }
        })
    }
}

impl Drop for BorrowedAsyncConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let handle = tokio::runtime::Handle::try_current();
            match handle {
                Ok(handle) => {
                    let pool = self.conn_pool.clone();
                    handle.spawn_blocking(move || {
                        pool.blocking_lock().push_back(conn);
                    });
                }
                Err(_) => {
                    self.conn_pool.blocking_lock().push_back(conn);
                }
            }
        }
    }
}
