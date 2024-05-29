/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorValue};
use anyhow::Result;
use std::{fmt::Display, sync::mpsc};

pub(crate) enum FalkorSyncConnection {
    #[cfg(feature = "redis")]
    Redis(redis::Connection),
}

/// A container for a connection that is borrowed from the pool.
/// Upon going out of scope, it will return the connection to the pool.
///
/// This is publicly exposed for user-implementations of [`FalkorParsable`](crate::FalkorParsable)
pub struct BorrowedSyncConnection {
    conn: Option<FalkorSyncConnection>,
    return_tx: mpsc::SyncSender<FalkorSyncConnection>,
}

impl BorrowedSyncConnection {
    pub(crate) fn new(
        conn: FalkorSyncConnection,
        return_tx: mpsc::SyncSender<FalkorSyncConnection>,
    ) -> Self {
        Self {
            conn: Some(conn),
            return_tx,
        }
    }

    pub(crate) fn as_inner(&mut self) -> Result<&mut FalkorSyncConnection, FalkorDBError> {
        self.conn.as_mut().ok_or(FalkorDBError::EmptyConnection)
    }

    pub(crate) fn send_command<P: Display>(
        &mut self,
        graph_name: Option<&str>,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[P]>,
    ) -> Result<FalkorValue, FalkorDBError> {
        match self.as_inner()? {
            #[cfg(feature = "redis")]
            FalkorSyncConnection::Redis(redis_conn) => {
                use redis::ConnectionLike as _;
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
                        .req_command(&cmd)
                        .map_err(|err| FalkorDBError::RedisConnectionError(err.to_string()))?,
                )
                .map_err(|err| FalkorDBError::RedisParsingError(err.to_string()))
            }
        }
    }
}

impl Drop for BorrowedSyncConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.return_tx.send(conn).ok();
        }
    }
}
