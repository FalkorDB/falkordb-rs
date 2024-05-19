/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::error::FalkorDBError;
use anyhow::Result;
use tokio::sync::mpsc;

pub enum FalkorAsyncConnection {
    #[cfg(feature = "redis")]
    Redis(redis::aio::MultiplexedConnection),
}

pub(crate) struct BorrowedAsyncConnectionGuard {
    conn: Option<FalkorAsyncConnection>,
    return_tx: mpsc::Sender<FalkorAsyncConnection>,
}

impl BorrowedAsyncConnectionGuard {
    pub(crate) fn as_inner(&mut self) -> Result<&mut FalkorAsyncConnection> {
        self.conn
            .as_mut()
            .ok_or(FalkorDBError::EmptyConnection.into())
    }
}

impl Drop for BorrowedAsyncConnectionGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.return_tx.blocking_send(conn).ok();
        }
    }
}
