/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    client::asynchronous::FalkorAsyncClientInner,
    connection::{map_redis_err, reject_server_error, ClusterRoute},
    parser::parse_redis_info,
    FalkorDBError, FalkorResult,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;

pub(crate) enum FalkorAsyncConnection {
    Redis(redis::aio::MultiplexedConnection),
    /// A shared, auto-reconnecting multiplexed connection. Cheaply cloneable: each
    /// clone shares the same underlying socket and routes responses back to the right
    /// caller, so many clones can have commands in flight concurrently.
    Managed(redis::aio::ConnectionManager),
    /// A connection to a Redis Cluster, routing each command to the node owning the graph's slot.
    /// Like [`Managed`](Self::Managed) it is a cheap, shareable handle over multiplexed sockets.
    /// Its query response timeout is enforced per command here, since the connection's own would
    /// also have bounded its setup.
    Cluster(
        redis::cluster_async::ClusterConnection,
        Option<std::time::Duration>,
    ),
}

/// Bound `request` by the query response timeout, failing as a timed-out read does.
async fn within_response_timeout<T>(
    timeout: Option<std::time::Duration>,
    request: impl std::future::Future<Output = redis::RedisResult<T>>,
) -> redis::RedisResult<T> {
    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, request)
            .await
            .unwrap_or_else(|_| Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into())),
        None => request.await,
    }
}

impl FalkorAsyncConnection {
    /// Clone this connection handle. Every variant is a cheap, reference-counted handle over
    /// shared multiplexed sockets, so the clone can carry commands concurrently.
    pub(crate) fn clone_handle(&self) -> Self {
        match self {
            FalkorAsyncConnection::Redis(conn) => FalkorAsyncConnection::Redis(conn.clone()),
            FalkorAsyncConnection::Managed(conn) => FalkorAsyncConnection::Managed(conn.clone()),
            FalkorAsyncConnection::Cluster(conn, response_timeout) => {
                FalkorAsyncConnection::Cluster(conn.clone(), *response_timeout)
            }
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
                cmd.arg(*param);
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
            FalkorAsyncConnection::Cluster(cluster_conn, response_timeout) => {
                use redis::aio::ConnectionLike as _;
                let route = ClusterRoute::for_command(command, subcommand);
                within_response_timeout(*response_timeout, async move {
                    match route.routing_info() {
                        None => cluster_conn.req_packed_command(&cmd).await,
                        Some(routing) => cluster_conn
                            .route_command(cmd, routing)
                            .await
                            .map(|reply| route.collapse_reply(command, reply)),
                    }
                })
                .await
                .map_err(map_redis_err)
            }
        }
        .and_then(reject_server_error)
    }

    /// Dispatch a whole [`redis::Pipeline`] in one round-trip, returning one [`redis::Value`] per
    /// command (a failed command is a `Value::ServerError` in its slot, not a transport error).
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Connection Inner Execute Pipeline", skip_all, level = "debug")
    )]
    pub(crate) async fn execute_pipeline(
        &mut self,
        pipeline: &redis::Pipeline,
    ) -> FalkorResult<Vec<redis::Value>> {
        use redis::aio::ConnectionLike as _;
        let count = pipeline.len();
        match self {
            FalkorAsyncConnection::Redis(redis_conn) => redis_conn
                .req_packed_commands(pipeline, 0, count)
                .await
                .map_err(map_redis_err),
            FalkorAsyncConnection::Managed(redis_conn) => redis_conn
                .req_packed_commands(pipeline, 0, count)
                .await
                .map_err(map_redis_err),
            FalkorAsyncConnection::Cluster(cluster_conn, response_timeout) => {
                within_response_timeout(
                    *response_timeout,
                    cluster_conn.req_packed_commands(pipeline, 0, count),
                )
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

    /// The server's `redis_mode` from `INFO server`: `standalone`, `sentinel` or `cluster`.
    pub(crate) async fn redis_mode(&mut self) -> FalkorResult<Option<String>> {
        self.get_redis_info(Some("server"))
            .await
            .map(|mut info_map| info_map.remove("redis_mode"))
    }
}

/// How a [`BorrowedAsyncConnection`] returns its connection once it is done with it.
enum ConnReturn {
    /// Send the pool slot back to a bounded pool (the `Pooled` strategy).
    Pool(mpsc::Sender<Option<FalkorAsyncConnection>>),
    /// Drop the connection. Multiplexed connections are cheap `Arc` clones over a
    /// shared socket, so there is nothing to return.
    Discard,
}

/// A container for a connection that is borrowed from the pool.
/// Upon going out of scope, it returns its pool slot: the live connection, or an empty slot if
/// the connection died and could not be replaced, so a dead connection is never handed out again.
///
/// This is publicly exposed for user-implementations of [`FalkorParsable`](crate::FalkorParsable)
pub struct BorrowedAsyncConnection {
    /// `None` while a pooled slot has no live connection; the next command opens one.
    conn: Option<FalkorAsyncConnection>,
    return_to: ConnReturn,
    client: Arc<FalkorAsyncClientInner>,
    readonly: bool,
}

impl BorrowedAsyncConnection {
    /// Create a borrowed connection that returns its slot to a bounded pool on drop.
    pub(crate) fn new(
        conn: Option<FalkorAsyncConnection>,
        return_tx: mpsc::Sender<Option<FalkorAsyncConnection>>,
        client: Arc<FalkorAsyncClientInner>,
        readonly: bool,
    ) -> Self {
        #[cfg(feature = "metrics")]
        crate::observability::connection_borrow_started(readonly);
        Self {
            conn,
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
        #[cfg(feature = "metrics")]
        crate::observability::connection_borrow_started(readonly);
        Self {
            conn: Some(conn),
            return_to: ConnReturn::Discard,
            client,
            readonly,
        }
    }

    /// The slot's connection, opening a fresh one first if the slot is empty.
    async fn as_inner(&mut self) -> FalkorResult<&mut FalkorAsyncConnection> {
        let conn = match self.conn.take() {
            Some(conn) => conn,
            None => self.open_replacement().await?,
        };
        Ok(self.conn.insert(conn))
    }

    /// Open a connection to replace a dead pooled one, routed like the pool this slot belongs to
    /// (a replica for the read-only pool), reporting any failure as `NoConnection`.
    async fn open_replacement(&self) -> FalkorResult<FalkorAsyncConnection> {
        let conn = if self.readonly {
            self.client.get_async_replica_connection().await
        } else {
            self.client.get_async_connection().await
        };
        #[cfg(feature = "tracing")]
        if let Err(ref err) = conn {
            crate::observability::record_reconnect_failure(self.readonly, err);
        }
        conn.map_err(|_| FalkorDBError::NoConnection)
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
        let result = self
            .as_inner()
            .await?
            .execute_command(graph_name, command, subcommand, params)
            .await;
        self.recover_on_connection_down(result).await
        // `self` is dropped here, returning the connection (see the `Drop` impl below).
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
    pub(crate) async fn execute_pipeline(
        mut self,
        pipeline: &redis::Pipeline,
    ) -> FalkorResult<Vec<redis::Value>> {
        let result = self.as_inner().await?.execute_pipeline(pipeline).await;
        self.recover_on_connection_down(result).await
    }

    /// Fetch `INFO` for `section`, with the same dead-connection recovery as
    /// [`execute_command`](Self::execute_command).
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Borrowed Connection Get Redis Info",
            skip_all,
            level = "trace"
        )
    )]
    pub(crate) async fn get_redis_info(
        mut self,
        section: Option<&str>,
    ) -> FalkorResult<HashMap<String, String>> {
        let result = self.as_inner().await?.get_redis_info(section).await;
        self.recover_on_connection_down(result).await
    }

    /// On a dead pooled connection, drop it and swap in a fresh one so a live one is returned to the
    /// pool, then re-surface `ConnectionDown`. If no replacement can be opened, the slot is left
    /// empty for the next command to reconnect and `NoConnection` is returned. A multiplexed
    /// clone is left alone: its `ConnectionManager` or cluster connection reconnects on its own,
    /// and the clone is discarded on drop anyway. Other results pass through unchanged.
    async fn recover_on_connection_down<T>(
        &mut self,
        result: FalkorResult<T>,
    ) -> FalkorResult<T> {
        match result {
            Err(FalkorDBError::ConnectionDown) if matches!(self.return_to, ConnReturn::Pool(_)) => {
                // Drop the dead connection first, so it is gone even if no replacement opens.
                self.conn = None;
                self.conn = Some(self.open_replacement().await?);
                Err(FalkorDBError::ConnectionDown)
            }
            res => res,
        }
    }
}

impl Drop for BorrowedAsyncConnection {
    /// Return the connection to its origin. Pooled borrows hand their slot back through the
    /// bounded channel (empty if the connection died), which always has room because this
    /// borrow consumed one; multiplexed clones are cheap shared handles and are simply dropped.
    fn drop(&mut self) {
        #[cfg(feature = "metrics")]
        crate::observability::connection_borrow_finished(self.readonly);
        if let ConnReturn::Pool(return_tx) = &self.return_to {
            match return_tx.try_send(self.conn.take()) {
                Ok(()) => {}
                // The pool was dropped before this connection returned; nothing to reclaim.
                Err(mpsc::error::TrySendError::Closed(_)) => {}
                // Each borrow consumes a channel slot, so a return can never find it full.
                // A full channel means the pool's slot accounting is broken: surface it in
                // debug builds while still discarding the connection in release.
                Err(mpsc::error::TrySendError::Full(_)) => {
                    debug_assert!(
                        false,
                        "pool channel unexpectedly full while returning a borrowed connection"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::asynchronous::create_empty_inner_async_client;
    use crate::test_utils::create_async_test_client;
    use crate::{ConnectionStrategy, FalkorAsyncClient, FalkorClientBuilder};
    use std::num::NonZeroU8;

    /// A live connection, standing in for one that has just died.
    async fn stand_in_connection() -> FalkorAsyncConnection {
        create_async_test_client()
            .await
            .borrow_connection()
            .await
            .expect("borrow from the test client")
            .conn
            .take()
            .expect("a multiplexed borrow holds a connection")
    }

    /// A borrow from a one-slot pool whose client cannot open connections, so any reconnect
    /// attempt fails, along with the pool's receiving end.
    fn borrowed_from_unreachable(
        conn: Option<FalkorAsyncConnection>,
        readonly: bool,
    ) -> (
        BorrowedAsyncConnection,
        mpsc::Receiver<Option<FalkorAsyncConnection>>,
    ) {
        let (tx, rx) = mpsc::channel(1);
        let borrowed =
            BorrowedAsyncConnection::new(conn, tx, create_empty_inner_async_client(), readonly);
        (borrowed, rx)
    }

    async fn one_connection_client() -> FalkorAsyncClient {
        FalkorClientBuilder::new_async()
            .with_connection_strategy(ConnectionStrategy::Pooled {
                size: NonZeroU8::MIN,
            })
            .build()
            .await
            .expect("Could not build a pooled client")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_recovery_returns_an_empty_slot_instead_of_the_dead_connection() {
        let (mut conn, mut rx) =
            borrowed_from_unreachable(Some(stand_in_connection().await), false);
        let result: FalkorResult<()> = conn
            .recover_on_connection_down(Err(FalkorDBError::ConnectionDown))
            .await;
        assert_eq!(result, Err(FalkorDBError::NoConnection));

        drop(conn);
        assert_eq!(
            rx.try_recv().map(|slot| slot.is_none()),
            Ok(true),
            "the slot must come back empty, not holding the dead connection"
        );
        assert!(rx.try_recv().is_err(), "exactly one slot comes back");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_slot_stays_in_the_pool_when_reconnecting_fails() {
        for readonly in [false, true] {
            let (conn, mut rx) = borrowed_from_unreachable(None, readonly);
            assert_eq!(
                conn.execute_command(None, "PING", None, None).await,
                Err(FalkorDBError::NoConnection)
            );
            assert_eq!(
                rx.try_recv().map(|slot| slot.is_none()),
                Ok(true),
                "the slot must not be lost"
            );
        }

        let (conn, mut rx) = borrowed_from_unreachable(None, false);
        assert_eq!(
            conn.get_redis_info(None).await,
            Err(FalkorDBError::NoConnection)
        );
        assert_eq!(
            rx.try_recv().map(|slot| slot.is_none()),
            Ok(true),
            "the slot must not be lost"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_slot_reconnects_on_next_use() {
        let client = one_connection_client().await;
        let mut borrowed = client
            .borrow_connection()
            .await
            .expect("borrow the only slot");
        borrowed.conn = None;
        drop(borrowed);

        let pong = client
            .borrow_connection()
            .await
            .expect("borrow the empty slot")
            .execute_command(None, "PING", None, None)
            .await;
        assert!(
            pong.is_ok(),
            "the next command opens a fresh connection: {pong:?}"
        );
        assert!(
            client
                .borrow_connection()
                .await
                .expect("borrow the slot again")
                .conn
                .is_some(),
            "the fresh connection goes back into the slot"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn successful_recovery_swaps_in_a_live_connection() {
        let client = one_connection_client().await;
        let mut borrowed = client
            .borrow_connection()
            .await
            .expect("borrow the only slot");
        let result: FalkorResult<()> = borrowed
            .recover_on_connection_down(Err(FalkorDBError::ConnectionDown))
            .await;
        assert_eq!(result, Err(FalkorDBError::ConnectionDown));
        assert!(
            borrowed
                .execute_command(None, "PING", None, None)
                .await
                .is_ok(),
            "the replacement connection is live"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multiplexed_borrow_does_not_reconnect_on_connection_down() {
        // The client cannot open connections, so a reconnect attempt would surface as
        // `NoConnection` rather than the original `ConnectionDown`.
        let mut conn = BorrowedAsyncConnection::new_multiplexed(
            stand_in_connection().await,
            create_empty_inner_async_client(),
            false,
        );
        let result: FalkorResult<()> = conn
            .recover_on_connection_down(Err(FalkorDBError::ConnectionDown))
            .await;
        assert_eq!(result, Err(FalkorDBError::ConnectionDown));
        assert!(conn.conn.is_some(), "the multiplexed clone is left as is");
    }
}
