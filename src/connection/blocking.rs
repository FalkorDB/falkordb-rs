/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    client::{blocking::FalkorSyncClientInner, ProvidesSyncConnections},
    connection::map_redis_err,
    parser::parse_redis_info,
    FalkorDBError, FalkorResult,
};
use std::{
    collections::HashMap,
    sync::{mpsc, Arc},
};

pub(crate) enum FalkorSyncConnection {
    #[cfg(test)]
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
                redis_conn.req_command(&cmd).map_err(map_redis_err)
            }
            #[cfg(test)]
            FalkorSyncConnection::None => Ok(redis::Value::Nil),
        }
    }

    /// Dispatch a whole [`redis::Pipeline`] in one round-trip, returning one [`redis::Value`] per
    /// command (a failed command is a `Value::ServerError` in its slot, not a transport error).
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Connection Inner Execute Pipeline", skip_all, level = "debug")
    )]
    pub(crate) fn execute_pipeline(
        &mut self,
        pipeline: &redis::Pipeline,
    ) -> FalkorResult<Vec<redis::Value>> {
        match self {
            FalkorSyncConnection::Redis(redis_conn) => {
                use redis::ConnectionLike as _;
                redis_conn
                    .req_packed_commands(&pipeline.get_packed_pipeline(), 0, pipeline.len())
                    .map_err(map_redis_err)
            }
            #[cfg(test)]
            FalkorSyncConnection::None => Ok(Vec::new()),
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
            .and_then(parse_redis_info)
    }

    pub(crate) fn check_is_redis_sentinel(&mut self) -> FalkorResult<bool> {
        let info_map = self.get_redis_info(Some("server"))?;
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
pub struct BorrowedSyncConnection {
    conn: Option<FalkorSyncConnection>,
    return_tx: mpsc::SyncSender<FalkorSyncConnection>,
    client: Arc<FalkorSyncClientInner>,
    readonly: bool,
}

impl BorrowedSyncConnection {
    pub(crate) fn new(
        conn: FalkorSyncConnection,
        return_tx: mpsc::SyncSender<FalkorSyncConnection>,
        client: Arc<FalkorSyncClientInner>,
        readonly: bool,
    ) -> Self {
        Self {
            conn: Some(conn),
            return_tx,
            client,
            readonly,
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
        let result = self
            .as_inner()?
            .execute_command(graph_name, command, subcommand, params);
        self.recover_on_connection_down(result)
    }

    /// Dispatch a whole pipeline in one round-trip, with the same dead-connection recovery as
    /// [`execute_command`](Self::execute_command).
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Borrowed Connection Execute Pipeline",
            skip_all,
            level = "trace"
        )
    )]
    pub(crate) fn execute_pipeline(
        &mut self,
        pipeline: &redis::Pipeline,
    ) -> FalkorResult<Vec<redis::Value>> {
        let result = self.as_inner()?.execute_pipeline(pipeline);
        self.recover_on_connection_down(result)
    }

    /// On a dead-connection error, swap in a fresh connection so a live one is returned to the pool
    /// on drop, then re-surface `ConnectionDown` (or `NoConnection` if none is available); other
    /// results pass through unchanged.
    fn recover_on_connection_down<T>(
        &mut self,
        result: FalkorResult<T>,
    ) -> FalkorResult<T> {
        match result {
            Err(FalkorDBError::ConnectionDown) => {
                let new_conn = if self.readonly {
                    self.client.get_replica_connection()
                } else {
                    self.client.get_connection()
                };
                if let Ok(new_conn) = new_conn {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::blocking::create_empty_inner_sync_client;

    fn empty_borrowed() -> BorrowedSyncConnection {
        let (tx, _rx) = mpsc::sync_channel(1);
        BorrowedSyncConnection::new(
            FalkorSyncConnection::None,
            tx,
            create_empty_inner_sync_client(),
            false,
        )
    }

    #[test]
    fn execute_pipeline_on_none_connection_is_empty() {
        let mut conn = FalkorSyncConnection::None;
        let out = conn
            .execute_pipeline(&redis::pipe())
            .expect("none -> empty");
        assert!(out.is_empty());
    }

    #[test]
    fn recover_on_connection_down_passes_other_results_through() {
        let mut conn = empty_borrowed();
        let out: FalkorResult<i32> = conn.recover_on_connection_down(Ok(7));
        assert_eq!(out.expect("ok passes through"), 7);
        let err: FalkorResult<i32> =
            conn.recover_on_connection_down(Err(FalkorDBError::EmptyConnection));
        assert!(matches!(err, Err(FalkorDBError::EmptyConnection)));
    }

    #[test]
    fn recover_on_connection_down_handles_dead_connection() {
        let mut conn = empty_borrowed();
        // The empty client cannot supply a healthy replacement, so the dead-connection error is
        // re-surfaced (as ConnectionDown if a stub was swapped in, else NoConnection).
        let out: FalkorResult<redis::Value> =
            conn.recover_on_connection_down(Err(FalkorDBError::ConnectionDown));
        assert!(out.is_err());
    }
}
