/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{client::FalkorClientImpl, FalkorConnectionInfo, FalkorDBError, SyncFalkorClient};
use anyhow::Result;
use std::sync::Arc;

// This doesn't have a default implementation because a specific const char is required
// and I don't want to leave that up to the user
pub struct FalkorDBClientBuilder<const R: char> {
    connection_info: Option<FalkorConnectionInfo>,
    num_connections: u8,
    multithreaded_rt: bool,
    #[cfg(feature = "tokio")]
    runtime: Option<tokio::runtime::Runtime>,
}

impl<const R: char> FalkorDBClientBuilder<R> {
    pub fn with_connection_info(self, falkor_connection_info: FalkorConnectionInfo) -> Self {
        Self {
            connection_info: Some(falkor_connection_info),
            ..self
        }
    }

    pub fn with_num_connections(self, num_connections: u8) -> Self {
        Self {
            num_connections,
            ..self
        }
    }
}

fn get_client<T: TryInto<FalkorConnectionInfo>>(connection_info: T) -> Result<FalkorClientImpl>
where
    anyhow::Error: From<T::Error>,
{
    let connection_info = connection_info.try_into()?;
    Ok(match connection_info {
        FalkorConnectionInfo::Redis(connection_info) => {
            FalkorClientImpl::Redis(redis::Client::open(connection_info.clone())?)
        }
    })
}

impl FalkorDBClientBuilder<'S'> {
    // We wish this to be explicit, and implementing Default is pub
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        FalkorDBClientBuilder {
            connection_info: None,
            num_connections: 4,
            multithreaded_rt: false,
            #[cfg(feature = "tokio")]
            runtime: None,
        }
    }

    pub fn build(self) -> Result<Arc<SyncFalkorClient>> {
        if self.num_connections < 1 || self.num_connections > 32 {
            return Err(FalkorDBError::InvalidConnectionPoolSize.into());
        }

        let connection_info = self
            .connection_info
            .unwrap_or("falkor://127.0.0.1:6379".try_into()?);

        SyncFalkorClient::create(get_client(connection_info.clone())?, self.num_connections)
    }
}

#[cfg(feature = "tokio")]
impl FalkorDBClientBuilder<'A'> {
    pub fn new_async() -> Self {
        FalkorDBClientBuilder {
            connection_info: None,
            num_connections: 4,
            multithreaded_rt: false,
            #[cfg(feature = "tokio")]
            runtime: None,
        }
    }

    pub fn with_multithreaded_runtime(self) -> Self {
        Self {
            multithreaded_rt: true,
            ..self
        }
    }

    /// This overrides with_multithreaded_runtime()
    pub fn with_runtime(self, runtime: tokio::runtime::Runtime) -> Self {
        Self {
            runtime: Some(runtime),
            ..self
        }
    }

    pub async fn build(self) -> Result<Arc<crate::AsyncFalkorClient>> {
        let connection_info = self
            .connection_info
            .unwrap_or("falkor://127.0.0.1:6379".try_into()?);

        let runtime = self.runtime.unwrap_or(
            if self.multithreaded_rt {
                tokio::runtime::Builder::new_multi_thread()
            } else {
                tokio::runtime::Builder::new_current_thread()
            }
            .build()?,
        );

        crate::AsyncFalkorClient::create(get_client(connection_info)?, runtime).await
    }
}
