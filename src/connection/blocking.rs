/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::blocking::FalkorSyncClientInner, parser::redis_value_as_string, FalkorDBError,
    FalkorResult,
};
use std::{
    collections::HashMap,
    sync::{mpsc, Arc},
};

pub(crate) enum FalkorSyncConnection {
    #[allow(unused)]
    None,
    Redis(redis::Connection),
}

impl FalkorSyncConnection {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Connection Inner Execute Command", skip_all, level = "debug")
    )]
    pub(crate) fn execute_command(
        &mut self,
        graph_name: Option<&str>,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[&str]>,
    ) -> FalkorResult<redis::Value> {
        match self {
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
                redis_conn
                    .req_command(&cmd)
                    .map_err(|err| match err.kind() {
                        redis::ErrorKind::IoError
                        | redis::ErrorKind::ClusterConnectionNotFound
                        | redis::ErrorKind::ClusterDown
                        | redis::ErrorKind::MasterDown => FalkorDBError::ConnectionDown,
                        _ => FalkorDBError::RedisError(err.to_string()),
                    })
            }
            FalkorSyncConnection::None => Ok(redis::Value::Nil),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Connection Get Redis Info", skip_all, level = "info")
    )]
    pub(crate) fn get_redis_info(
        &mut self,
        section: Option<&str>,
    ) -> FalkorResult<HashMap<String, String>> {
        self.execute_command(None, "INFO", section, None)
            .and_then(|res| {
                redis_value_as_string(res)
                    .map(|info| {
                        info.split("\r\n")
                            .map(|info_item| info_item.split(':').collect::<Vec<_>>())
                            .flat_map(TryInto::<[&str; 2]>::try_into)
                            .map(|[key, val]| (key.to_string(), val.to_string()))
                            .collect()
                    })
                    .map_err(|_| FalkorDBError::ParsingFString)
            })
    }
}

/// A container for a connection that is borrowed from the pool.
/// Upon going out of scope, it will return the connection to the pool.
///
/// This is publicly exposed for user-implementations of [`FalkorParsable`](crate::FalkorParsable)
pub struct BorrowedSyncConnection {
    conn: Option<FalkorSyncConnection>,
    return_tx: mpsc::SyncSender<FalkorSyncConnection>,
    client: Arc<FalkorSyncClientInner>,
}

impl BorrowedSyncConnection {
    pub(crate) fn new(
        conn: FalkorSyncConnection,
        return_tx: mpsc::SyncSender<FalkorSyncConnection>,
        client: Arc<FalkorSyncClientInner>,
    ) -> Self {
        Self {
            conn: Some(conn),
            return_tx,
            client,
        }
    }

    pub(crate) fn as_inner(&mut self) -> FalkorResult<&mut FalkorSyncConnection> {
        self.conn.as_mut().ok_or(FalkorDBError::EmptyConnection)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Borrowed Connection Execute Command",
            skip_all,
            level = "trace"
        )
    )]
    pub(crate) fn execute_command(
        &mut self,
        graph_name: Option<&str>,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[&str]>,
    ) -> Result<redis::Value, FalkorDBError> {
        match self
            .as_inner()?
            .execute_command(graph_name, command, subcommand, params)
        {
            Err(FalkorDBError::ConnectionDown) => {
                if let Ok(new_conn) = self.client.get_connection() {
                    self.conn = Some(new_conn);
                    return Err(FalkorDBError::ConnectionDown);
                }
                Err(FalkorDBError::NoConnection)
            }
            res => res,
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
