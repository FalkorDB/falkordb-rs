/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    client::asynchronous::FalkorAsyncClientInner, connection::map_redis_err,
    parser::parse_redis_info, FalkorDBError, FalkorResult,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;

pub(crate) enum FalkorAsyncConnection {
    Redis(redis::aio::MultiplexedConnection),
    /// A shared, auto-reconnecting multiplexed connection. Cheaply cloneable: each
    /// clone shares the same underlying socket and routes responses back to the right
    /// caller, so many clones can have commands in flight concurrently.
    Managed(redis::aio::ConnectionManager),
}

impl FalkorAsyncConnection {
    /// Clone this connection handle. Both variants are cheap, reference-counted handles
    /// over a shared multiplexed socket, so the clone can carry commands concurrently.
    pub(crate) fn clone_handle(&self) -> Self {
        match self {
            FalkorAsyncConnection::Redis(conn) => FalkorAsyncConnection::Redis(conn.clone()),
            FalkorAsyncConnection::Managed(conn) => FalkorAsyncConnection::Managed(conn.clone()),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Connection Inner Execute Command", skip_all, level = "debug")
    )]
    pub(crate) async fn execute_command(
        &mut self,
        graph_name: Option<&str>,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[&str]>,
    ) -> FalkorResult<redis::Value> {
        let mut cmd = redis::cmd(command);
        cmd.arg(subcommand);
        cmd.arg(graph_name);
        if let Some(params) = params {
            for param in params {
                cmd.arg(param.to_string());
            }
        }
        match self {
            FalkorAsyncConnection::Redis(redis_conn) => redis_conn
                .send_packed_command(&cmd)
                .await
                .map_err(map_redis_err),
            FalkorAsyncConnection::Managed(redis_conn) => {
                use redis::aio::ConnectionLike as _;
                redis_conn
                    .req_packed_command(&cmd)
                    .await
                    .map_err(map_redis_err)
            }
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Connection Get Redis Info", skip_all, level = "info")
    )]
    pub(crate) async fn get_redis_info(
        &mut self,
        section: Option<&str>,
    ) -> FalkorResult<HashMap<String, String>> {
        self.execute_command(None, "INFO", section, None)
            .await
            .and_then(parse_redis_info)
    }

    pub(crate) async fn check_is_redis_sentinel(&mut self) -> FalkorResult<bool> {
        let info_map = self.get_redis_info(Some("server")).await?;
        Ok(info_map
            .get("redis_mode")
            .map(|redis_mode| redis_mode == "sentinel")
            .unwrap_or_default())
    }
}

/// How a [`BorrowedAsyncConnection`] returns its connection once it is done with it.
enum ConnReturn {
    /// Send the connection back to a bounded pool (the `Pooled` strategy).
    Pool(mpsc::Sender<FalkorAsyncConnection>),
    /// Drop the connection. Multiplexed connections are cheap `Arc` clones over a
    /// shared socket, so there is nothing to return.
    Discard,
}

/// A container for a connection that is borrowed from the pool.
/// Upon going out of scope, it will return the connection to the pool.
///
/// This is publicly exposed for user-implementations of [`FalkorParsable`](crate::FalkorParsable)
pub struct BorrowedAsyncConnection {
    conn: Option<FalkorAsyncConnection>,
    return_to: ConnReturn,
    client: Arc<FalkorAsyncClientInner>,
    readonly: bool,
}

impl BorrowedAsyncConnection {
    /// Create a borrowed connection that returns to a bounded pool on drop.
    pub(crate) fn new(
        conn: FalkorAsyncConnection,
        return_tx: mpsc::Sender<FalkorAsyncConnection>,
        client: Arc<FalkorAsyncClientInner>,
        readonly: bool,
    ) -> Self {
        Self {
            conn: Some(conn),
            return_to: ConnReturn::Pool(return_tx),
            client,
            readonly,
        }
    }

    /// Create a borrowed connection wrapping a multiplexed connection clone. The clone
    /// is discarded on drop rather than returned to a pool.
    pub(crate) fn new_multiplexed(
        conn: FalkorAsyncConnection,
        client: Arc<FalkorAsyncClientInner>,
        readonly: bool,
    ) -> Self {
        Self {
            conn: Some(conn),
            return_to: ConnReturn::Discard,
            client,
            readonly,
        }
    }

    pub(crate) fn as_inner(&mut self) -> FalkorResult<&mut FalkorAsyncConnection> {
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
    pub(crate) async fn execute_command(
        mut self,
        graph_name: Option<&str>,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[&str]>,
    ) -> FalkorResult<redis::Value> {
        let res = match self
            .as_inner()?
            .execute_command(graph_name, command, subcommand, params)
            .await
        {
            Err(FalkorDBError::ConnectionDown) => {
                if let Ok(new_conn) = self.client.fresh_connection(self.readonly).await {
                    self.conn = Some(new_conn);
                    tokio::spawn(async { self.return_to_pool().await });
                    return Err(FalkorDBError::ConnectionDown);
                }
                Err(FalkorDBError::NoConnection)
            }
            res => res,
        };

        tokio::spawn(async { self.return_to_pool().await });
        res
    }

    pub(crate) async fn return_to_pool(self) {
        if let (Some(conn), ConnReturn::Pool(return_tx)) = (self.conn, self.return_to) {
            return_tx.send(conn).await.ok();
        }
    }
}
