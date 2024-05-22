/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{connection::blocking::BorrowedSyncConnection, FalkorValue, SyncGraphSchema};
use anyhow::Result;
use std::future::Future;

pub trait FalkorParsable: Sized {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &SyncGraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> Result<Self>;
}

#[cfg(feature = "tokio")]
pub trait FalkorAsyncParseable: Sized {
    fn from_falkor_value_async(
        value: FalkorValue,
        graph_schema: &crate::AsyncGraphSchema,
        conn: &mut crate::FalkorAsyncConnection,
    ) -> impl Future<Output = Result<Self>> + Send;
}