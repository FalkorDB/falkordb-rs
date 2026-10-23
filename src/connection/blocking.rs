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

    /// Set the socket read and write deadline for the next commands (`None` waits forever).
    pub(crate) fn set_timeouts(
        &self,
        timeout: Option<std::time::Duration>,
    ) -> FalkorResult<()> {
        match self {
            FalkorSyncConnection::Redis(redis_conn) => redis_conn
                .set_read_timeout(timeout)
                .and_then(|()| redis_conn.set_write_timeout(timeout))
                .map_err(map_redis_err),
            #[cfg(test)]
            FalkorSyncConnection::None => Ok(()),
        }
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
/// Upon going out of scope, it returns its pool slot: the live connection, or an empty slot if
/// the connection died and could not be replaced, so a dead connection is never handed out again.
///
/// This is publicly exposed for user-implementations of [`FalkorParsable`](crate::FalkorParsable)
pub struct BorrowedSyncConnection {
    /// `None` while the slot has no live connection; the next command opens one.
    conn: Option<FalkorSyncConnection>,
    return_tx: mpsc::SyncSender<Option<FalkorSyncConnection>>,
    client: Arc<FalkorSyncClientInner>,
    readonly: bool,
}

impl BorrowedSyncConnection {
    pub(crate) fn new(
        conn: Option<FalkorSyncConnection>,
        return_tx: mpsc::SyncSender<Option<FalkorSyncConnection>>,
        client: Arc<FalkorSyncClientInner>,
        readonly: bool,
    ) -> Self {
        #[cfg(feature = "metrics")]
        crate::observability::connection_borrow_started(readonly);
        Self {
            conn,
            return_tx,
            client,
            readonly,
        }
    }

    /// The slot's connection, opening a fresh one first if the slot is empty.
    fn as_inner(&mut self) -> FalkorResult<&mut FalkorSyncConnection> {
        let conn = match self.conn.take() {
            Some(conn) => conn,
            None => self.open_replacement()?,
        };
        Ok(self.conn.insert(conn))
    }

    /// Open a connection to replace a dead one, routed like the pool this slot belongs to (a
    /// replica for the read-only pool), reporting any failure as `NoConnection`.
    fn open_replacement(&self) -> FalkorResult<FalkorSyncConnection> {
        let conn = if self.readonly {
            self.client.get_replica_connection()
        } else {
            self.client.get_connection()
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
    pub(crate) fn get_redis_info(
        &mut self,
        section: Option<&str>,
    ) -> FalkorResult<HashMap<String, String>> {
        let result = self.as_inner()?.get_redis_info(section);
        self.recover_on_connection_down(result)
    }

    /// On a dead-connection error, drop the dead connection and swap in a fresh one so a live one is
    /// returned to the pool, then re-surface `ConnectionDown`. If no replacement can be opened, the
    /// slot is left empty for the next command to reconnect and `NoConnection` is returned. Other
    /// results pass through unchanged.
    fn recover_on_connection_down<T>(
        &mut self,
        result: FalkorResult<T>,
    ) -> FalkorResult<T> {
        match result {
            Err(FalkorDBError::ConnectionDown) => {
                // Drop the dead connection first, so it is gone even if no replacement opens.
                self.conn = None;
                self.conn = Some(self.open_replacement()?);
                Err(FalkorDBError::ConnectionDown)
            }
            res => res,
        }
    }
}

impl Drop for BorrowedSyncConnection {
    fn drop(&mut self) {
        #[cfg(feature = "metrics")]
        crate::observability::connection_borrow_finished(self.readonly);
        // An empty slot goes back too, so the pool never loses capacity.
        self.return_tx.send(self.conn.take()).ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::blocking::create_empty_inner_sync_client;
    use crate::{FalkorClientBuilder, FalkorSyncClient};
    use std::num::NonZeroU8;

    /// A borrow from a one-slot pool whose client cannot open connections, so any reconnect
    /// attempt fails, along with the pool's receiving end.
    fn borrowed_from_unreachable(
        conn: Option<FalkorSyncConnection>,
        readonly: bool,
    ) -> (
        BorrowedSyncConnection,
        mpsc::Receiver<Option<FalkorSyncConnection>>,
    ) {
        let (tx, rx) = mpsc::sync_channel(1);
        let borrowed =
            BorrowedSyncConnection::new(conn, tx, create_empty_inner_sync_client(), readonly);
        (borrowed, rx)
    }

    fn one_connection_client() -> FalkorSyncClient {
        FalkorClientBuilder::new()
            .with_num_connections(NonZeroU8::MIN)
            .build()
            .expect("Could not create client for this test")
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
        let (mut conn, _rx) = borrowed_from_unreachable(Some(FalkorSyncConnection::None), false);
        let out: FalkorResult<i32> = conn.recover_on_connection_down(Ok(7));
        assert_eq!(out.expect("ok passes through"), 7);
        let err: FalkorResult<i32> =
            conn.recover_on_connection_down(Err(FalkorDBError::EmptyConnection));
        assert!(matches!(err, Err(FalkorDBError::EmptyConnection)));
    }

    #[test]
    fn failed_recovery_returns_an_empty_slot_instead_of_the_dead_connection() {
        let (mut conn, rx) = borrowed_from_unreachable(Some(FalkorSyncConnection::None), false);
        let result: FalkorResult<()> =
            conn.recover_on_connection_down(Err(FalkorDBError::ConnectionDown));
        assert_eq!(result, Err(FalkorDBError::NoConnection));

        drop(conn);
        assert_eq!(
            rx.try_recv().map(|slot| slot.is_none()),
            Ok(true),
            "the slot must come back empty, not holding the dead connection"
        );
        assert!(rx.try_recv().is_err(), "exactly one slot comes back");
    }

    #[test]
    fn empty_slot_stays_in_the_pool_when_reconnecting_fails() {
        for readonly in [false, true] {
            let (mut conn, rx) = borrowed_from_unreachable(None, readonly);
            assert_eq!(
                conn.execute_command(None, "PING", None, None),
                Err(FalkorDBError::NoConnection)
            );
            assert_eq!(conn.get_redis_info(None), Err(FalkorDBError::NoConnection));

            drop(conn);
            assert_eq!(
                rx.try_recv().map(|slot| slot.is_none()),
                Ok(true),
                "the slot must not be lost"
            );
        }
    }

    #[test]
    fn empty_slot_reconnects_on_next_use() {
        let client = one_connection_client();
        let mut borrowed = client.borrow_connection().expect("borrow the only slot");
        borrowed.conn = None;
        drop(borrowed);

        let mut borrowed = client.borrow_connection().expect("borrow the empty slot");
        assert!(
            borrowed.execute_command(None, "PING", None, None).is_ok(),
            "the next command opens a fresh connection"
        );
        drop(borrowed);
        assert!(
            client
                .borrow_connection()
                .expect("borrow the slot again")
                .conn
                .is_some(),
            "the fresh connection goes back into the slot"
        );
    }

    #[test]
    fn successful_recovery_swaps_in_a_live_connection() {
        let client = one_connection_client();
        let mut borrowed = client.borrow_connection().expect("borrow the only slot");
        let result: FalkorResult<()> =
            borrowed.recover_on_connection_down(Err(FalkorDBError::ConnectionDown));
        assert_eq!(result, Err(FalkorDBError::ConnectionDown));
        assert!(
            borrowed.execute_command(None, "PING", None, None).is_ok(),
            "the replacement connection is live"
        );
    }
}
