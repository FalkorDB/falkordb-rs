/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::error::FalkorDBError;
use crate::value::FalkorValue;
use anyhow::Result;
use redis::ConnectionLike;
use std::sync::mpsc;

pub enum FalkorSyncConnection {
    #[cfg(feature = "redis")]
    Redis(redis::Connection),
}

pub struct BorrowedSyncConnection {
    pub(crate) conn: Option<FalkorSyncConnection>,
    pub(crate) return_tx: mpsc::SyncSender<FalkorSyncConnection>,
}

impl BorrowedSyncConnection {
    pub(crate) fn as_inner(&mut self) -> Result<&mut FalkorSyncConnection> {
        self.conn
            .as_mut()
            .ok_or(FalkorDBError::EmptyConnection.into())
    }

    pub(crate) fn send_command(
        &mut self,
        graph_name: Option<String>,
        command: &str,
        params: Option<String>,
    ) -> Result<FalkorValue> {
        Ok(
            match self.conn.as_mut().ok_or(FalkorDBError::EmptyConnection)? {
                #[cfg(feature = "redis")]
                FalkorSyncConnection::Redis(redis_conn) => {
                    redis::FromRedisValue::from_owned_redis_value(
                        redis_conn.req_command(redis::cmd(command).arg(graph_name).arg(params))?,
                    )?
                }
            },
        )
    }
}

impl Drop for BorrowedSyncConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.return_tx.send(conn).ok();
        }
    }
}
