/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection,
    graph_schema::blocking::GraphSchema as SyncGraphSchema, FalkorValue,
};
use anyhow::Result;

#[cfg(feature = "tokio")]
use crate::{
    connection::asynchronous::FalkorAsyncConnection,
    graph_schema::asynchronous::GraphSchema as AsyncGraphSchema,
};

pub trait FalkorParsable: Sized {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &SyncGraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> Result<Self>;
}

#[cfg(feature = "tokio")]
pub trait FalkorAsyncParseable: Sized {
    async fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &AsyncGraphSchema,
        conn: &mut FalkorAsyncConnection,
    ) -> Result<Self>;
}
