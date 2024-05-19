use crate::error::FalkorDBError;
use std::sync::mpsc;

pub enum FalkorSyncConnection {
    #[cfg(feature = "redis")]
    Redis(redis::Connection),
}

pub(crate) struct BorrowedSyncConnectionGuard {
    pub(crate) conn: Option<FalkorSyncConnection>,
    pub(crate) return_tx: mpsc::SyncSender<FalkorSyncConnection>,
}

impl BorrowedSyncConnectionGuard {
    pub(crate) fn as_inner(&mut self) -> anyhow::Result<&mut FalkorSyncConnection> {
        self.conn
            .as_mut()
            .ok_or(FalkorDBError::EmptyConnection.into())
    }
}

impl Drop for BorrowedSyncConnectionGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.return_tx.send(conn).ok();
        }
    }
}
