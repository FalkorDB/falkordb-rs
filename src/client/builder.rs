/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    FalkorConnectionInfo, FalkorDBError, FalkorResult, FalkorSyncClient,
    client::FalkorClientProvider,
};
use std::num::NonZeroU8;

#[cfg(feature = "tokio")]
use crate::FalkorAsyncClient;

/// A Builder-pattern implementation struct for creating a new Falkor client.
pub struct FalkorClientBuilder<const R: char> {
    connection_info: Option<FalkorConnectionInfo>,
    num_connections: NonZeroU8,
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
    pub fn with_connection_info(
        self,
        falkor_connection_info: FalkorConnectionInfo,
    ) -> Self {
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
    pub fn with_num_connections(
        self,
        num_connections: NonZeroU8,
    ) -> Self {
        Self {
            num_connections,
            ..self
        }
    }

    fn get_client<E: ToString, T: TryInto<FalkorConnectionInfo, Error = E>>(
        connection_info: T
    ) -> FalkorResult<FalkorClientProvider> {
        let connection_info = connection_info
            .try_into()
            .map_err(|err| FalkorDBError::InvalidConnectionInfo(err.to_string()))?;
        Ok(match connection_info {
            FalkorConnectionInfo::Redis(connection_info) => FalkorClientProvider::Redis {
                client: redis::Client::open(connection_info.clone())
                    .map_err(|err| FalkorDBError::RedisError(err.to_string()))?,
                sentinel: None,
            },
        })
    }
}

impl FalkorClientBuilder<'S'> {
    /// Creates a new [`FalkorClientBuilder`] for a sync client.
    ///
    /// # Returns
    /// The new [`FalkorClientBuilder`]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        FalkorClientBuilder {
            connection_info: None,
            num_connections: NonZeroU8::new(8).expect("Error creating perfectly valid u8"),
        }
    }

    /// Consume the builder, returning the newly constructed sync client
    ///
    /// # Returns
    /// a new [`FalkorSyncClient`]
    pub fn build(self) -> FalkorResult<FalkorSyncClient> {
        let connection_info = self
            .connection_info
            .unwrap_or("falkor://127.0.0.1:6379".try_into()?);

        let mut client = Self::get_client(connection_info.clone())?;

        #[allow(irrefutable_let_patterns)]
        if let FalkorConnectionInfo::Redis(redis_conn_info) = &connection_info {
            if let Some(sentinel) = client.get_sentinel_client(redis_conn_info)? {
                client.set_sentinel(sentinel);
            }
        }
        FalkorSyncClient::create(client, connection_info, self.num_connections.get())
    }
}

#[cfg(feature = "tokio")]
impl FalkorClientBuilder<'A'> {
    /// Creates a new [`FalkorClientBuilder`] for an asynchronous client.
    ///
    /// # Returns
    /// The new [`FalkorClientBuilder`]
    pub fn new_async() -> Self {
        FalkorClientBuilder {
            connection_info: None,
            num_connections: NonZeroU8::new(8).expect("Error creating perfectly valid u8"),
        }
    }

    /// Consume the builder, returning the newly constructed async client
    ///
    /// # Returns
    /// a new [`FalkorAsyncClient`]
    pub async fn build(self) -> FalkorResult<FalkorAsyncClient> {
        let connection_info = self
            .connection_info
            .unwrap_or("falkor://127.0.0.1:6379".try_into()?);

        let mut client = Self::get_client(connection_info.clone())?;

        #[allow(irrefutable_let_patterns)]
        if let FalkorConnectionInfo::Redis(redis_conn_info) = &connection_info {
            if let Some(sentinel) = client.get_sentinel_client_async(redis_conn_info).await? {
                client.set_sentinel(sentinel);
            }
        }
        FalkorAsyncClient::create(client, connection_info, self.num_connections.get()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_builder() {
        let connection_info = "falkor://127.0.0.1:6379".try_into();
        assert!(connection_info.is_ok());

        assert!(
            FalkorClientBuilder::new()
                .with_connection_info(connection_info.unwrap())
                .build()
                .is_ok()
        );
    }

    #[test]
    fn test_connection_pool_size() {
        let client = FalkorClientBuilder::new()
            .with_num_connections(NonZeroU8::new(16).expect("Could not create a perfectly fine u8"))
            .build();
        assert!(client.is_ok());

        assert_eq!(client.unwrap().connection_pool_size(), 16);
    }
}
