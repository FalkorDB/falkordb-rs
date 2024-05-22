/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{client::FalkorClientProvider, FalkorConnectionInfo, FalkorDBError, FalkorSyncClient};
use anyhow::Result;

/// A Builder-pattern implementation struct for creating a new Falkor client, sync or async.
pub struct FalkorClientBuilder<const R: char> {
    connection_info: Option<FalkorConnectionInfo>,
    num_connections: u8,
}

impl<const R: char> FalkorClientBuilder<R> {
    /// Provide a connection info for the database connection
    /// Will otherwise use the default connection details.
    ///
    /// # Arguments
    /// * `falkor_connection_info`: the [`FalkorConnectionInfo`] to provide
    ///
    /// # Returns
    /// The consumed and modified self.
    pub fn with_connection_info(self, falkor_connection_info: FalkorConnectionInfo) -> Self {
        Self {
            connection_info: Some(falkor_connection_info),
            ..self
        }
    }

    /// Specify how large a connection pool to maintain, for concurrent operations.
    ///
    /// # Arguments
    /// * `num_connections`: the numer of connections, a non-negative integer, between 1 and 32
    ///
    /// # Returns
    /// The consumed and modified self.
    pub fn with_num_connections(self, num_connections: u8) -> Self {
        Self {
            num_connections,
            ..self
        }
    }
}

fn get_client<T: TryInto<FalkorConnectionInfo>>(connection_info: T) -> Result<FalkorClientProvider>
where
    anyhow::Error: From<T::Error>,
{
    let connection_info = connection_info.try_into()?;
    Ok(match connection_info {
        FalkorConnectionInfo::Redis(connection_info) => {
            FalkorClientProvider::Redis(redis::Client::open(connection_info.clone())?)
        }
    })
}

impl FalkorClientBuilder<'S'> {
    /// Creates a new [`FalkorClientBuilder`] for a sync client.
    ///
    /// # Returns
    /// The new [`FalkorClientBuilder`]
    pub fn new() -> Self {
        FalkorClientBuilder {
            connection_info: None,
            num_connections: 4,
        }
    }

    /// Consume the builder, returning the newly constructed sync client
    ///
    /// # Returns
    /// a new [`FalkorSyncClient`]
    pub fn build(self) -> Result<FalkorSyncClient> {
        if self.num_connections < 1 || self.num_connections > 32 {
            return Err(FalkorDBError::InvalidConnectionPoolSize.into());
        }

        let connection_info = self
            .connection_info
            .unwrap_or("falkor://127.0.0.1:6379".try_into()?);

        FalkorSyncClient::create(get_client(connection_info.clone())?, self.num_connections)
    }
}

#[cfg(feature = "tokio")]
impl FalkorClientBuilder<'A'> {
    pub fn new_async() -> Self {
        FalkorClientBuilder {
            connection_info: None,
            num_connections: 4,
        }
    }

    pub async fn build(
        self,
    ) -> Result<std::sync::Arc<tokio::sync::Mutex<crate::FalkorAsyncClient>>> {
        let connection_info = self
            .connection_info
            .unwrap_or("falkor://127.0.0.1:6379".try_into()?);

        crate::FalkorAsyncClient::create(get_client(connection_info)?).await
    }
}
