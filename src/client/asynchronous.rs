/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::client::FalkorClientImpl;
use crate::connection::asynchronous::FalkorAsyncConnection;
use anyhow::Result;
use std::collections::VecDeque;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

pub(crate) struct AsyncFalkorClient {
    _inner: FalkorClientImpl,
    num_connections: u8,
    connection_pool: Mutex<VecDeque<FalkorAsyncConnection>>,
    runtime: Runtime,
}

impl AsyncFalkorClient {
    pub(crate) async fn create(
        client: FalkorClientImpl,
        num_connections: u8,
        runtime: Runtime,
    ) -> Result<Self> {
        Ok(Self {
            _inner: client,
            num_connections,
            connection_pool: Default::default(),
            runtime,
        })
    }
}
