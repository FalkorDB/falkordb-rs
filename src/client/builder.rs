/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{client::FalkorClientProvider, FalkorConnectionInfo, FalkorDBError, FalkorSyncClient};
use anyhow::Result;
use std::time::Duration;

/// A Builder-pattern implementation struct for creating a new Falkor client, sync or async.
pub struct FalkorClientBuilder<const R: char> {
    connection_info: Option<FalkorConnectionInfo>,
    timeout: Option<Duration>,
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
        num_connections: u8,
    ) -> Self {
        Self {
            num_connections,
            ..self
        }
    }

    /// Specify a timeout duration for requests and connections.
    ///
    /// # Arguments
    /// * `timeout`: a [`Duration`], after which a timeout error will be returned from the connection.
    ///
    /// # Returns
    /// The consumed and modified self.
    pub fn with_timeout(
        self,
        timeout: Duration,
    ) -> Self {
        Self {
            timeout: Some(timeout),
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
        #[cfg(feature = "redis")]
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
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        FalkorClientBuilder {
            connection_info: None,
            timeout: None,
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

        FalkorSyncClient::create(
            get_client(connection_info.clone())?,
            connection_info,
            self.num_connections,
            self.timeout,
        )
    }
}

#[cfg(feature = "tokio")]
impl FalkorClientBuilder<'A'> {
    pub fn new_async() -> Self {
        FalkorClientBuilder {
            connection_info: None,
            num_connections: 4,
            timeout: None,
        }
    }

    pub async fn build(self) -> Result<crate::FalkorAsyncClient> {
        let connection_info = self
            .connection_info
            .unwrap_or("falkor://127.0.0.1:6379".try_into()?);

        crate::FalkorAsyncClient::create(
            get_client(connection_info.clone())?,
            connection_info,
            self.num_connections,
            self.timeout,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_builder() {
        let conneciton_info = "redis://127.0.0.1:6379".try_into();
        assert!(conneciton_info.is_ok());

        assert!(FalkorClientBuilder::new()
            .with_num_connections(4)
            .with_connection_info(conneciton_info.unwrap())
            .build()
            .is_ok());
    }

    #[test]
    #[cfg(feature = "redis")]
    #[allow(irrefutable_let_patterns)]
    fn test_sync_builder_redis_fallback() {
        let client = FalkorClientBuilder::new().build();
        assert!(client.is_ok());

        let FalkorConnectionInfo::Redis(redis_info) = client.unwrap()._connection_info;
        assert_eq!(redis_info.addr.to_string().as_str(), "127.0.0.1:6379");
    }

    #[test]
    fn test_connection_pool_size() {
        let client = FalkorClientBuilder::new().with_num_connections(16).build();
        assert!(client.is_ok());

        assert_eq!(client.unwrap().connection_pool_size(), 16);
    }

    #[test]
    fn test_invalid_connection_pool_size() {
        // Connection pool size must be between 0 and 32

        let zero = FalkorClientBuilder::new().with_num_connections(0).build();

        let too_many = FalkorClientBuilder::new().with_num_connections(36).build();

        assert!(zero.is_err() && too_many.is_err());
    }

    #[test]
    fn test_timeout() {
        {
            let client = FalkorClientBuilder::new()
                .with_timeout(Duration::from_millis(100))
                .build();
            assert!(client.is_ok());
        }

        let impossible_client = FalkorClientBuilder::new()
            .with_timeout(Duration::from_nanos(10))
            .build();
        assert!(impossible_client.is_err());
    }
}
