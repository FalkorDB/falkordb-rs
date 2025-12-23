/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{FalkorDBError, FalkorResult};

#[cfg(feature = "embedded")]
use crate::embedded::EmbeddedConfig;

/// An agnostic container which allows maintaining of various connection details.
/// The different enum variants are enabled based on compilation features
#[derive(Clone, Debug)]
pub enum FalkorConnectionInfo {
    /// A Redis database connection
    Redis(redis::ConnectionInfo),
    /// An embedded FalkorDB server (requires the "embedded" feature)
    #[cfg(feature = "embedded")]
    Embedded(EmbeddedConfig),
}

impl FalkorConnectionInfo {
    fn fallback_provider(mut full_url: String) -> FalkorResult<FalkorConnectionInfo> {
        Ok(FalkorConnectionInfo::Redis({
            if full_url.starts_with("falkor://") {
                full_url = full_url.replace("falkor://", "redis://");
            } else if full_url.starts_with("falkors://") {
                full_url = full_url.replace("falkors://", "rediss://");
            }
            redis::IntoConnectionInfo::into_connection_info(full_url)
                .map_err(|err| FalkorDBError::InvalidConnectionInfo(err.to_string()))?
        }))
    }

    /// Retrieves the internally stored address for this connection info
    ///
    /// # Returns
    /// A [`String`] representation of the address and port, or a UNIX socket path
    pub fn address(&self) -> String {
        match self {
            FalkorConnectionInfo::Redis(redis_info) => redis_info.addr.to_string(),
            #[cfg(feature = "embedded")]
            FalkorConnectionInfo::Embedded(_) => "embedded".to_string(),
        }
    }
}

impl TryFrom<&str> for FalkorConnectionInfo {
    type Error = FalkorDBError;

    fn try_from(value: &str) -> FalkorResult<Self> {
        let (url, url_schema) = regex::Regex::new(r"^(?P<schema>[a-zA-Z][a-zA-Z0-9+\-.]*):")
            .map_err(|err| FalkorDBError::ParsingError(format!("Error constructing regex: {err}")))?
            .captures(value)
            .and_then(|cap| cap.get(1))
            .map(|m| (value.to_string(), m.as_str()))
            .unwrap_or((format!("falkor://{value}"), "falkor"));

        match url_schema {
            "redis" | "rediss" => Ok(FalkorConnectionInfo::Redis(
                redis::IntoConnectionInfo::into_connection_info(value)
                    .map_err(|err| FalkorDBError::InvalidConnectionInfo(err.to_string()))?,
            )),
            _ => FalkorConnectionInfo::fallback_provider(url),
        }
    }
}

impl TryFrom<String> for FalkorConnectionInfo {
    type Error = FalkorDBError;

    #[inline]
    fn try_from(value: String) -> FalkorResult<Self> {
        Self::try_from(value.as_str())
    }
}

impl<T: ToString> TryFrom<(T, u16)> for FalkorConnectionInfo {
    type Error = FalkorDBError;

    #[inline]
    fn try_from(value: (T, u16)) -> FalkorResult<Self> {
        Self::try_from(format!("{}:{}", value.0.to_string(), value.1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{mem, str::FromStr};

    #[test]
    fn test_redis_fallback_provider() {
        let result =
            FalkorConnectionInfo::fallback_provider("redis://127.0.0.1:6379".to_string()).unwrap();
        match result {
            FalkorConnectionInfo::Redis(redis) => {
                assert_eq!(redis.addr.to_string(), "127.0.0.1:6379".to_string());
            }
            #[cfg(feature = "embedded")]
            _ => panic!("Expected Redis connection info"),
        }
    }

    #[test]
    fn test_try_from_redis() {
        let res = FalkorConnectionInfo::try_from("redis://0.0.0.0:1234");
        assert!(res.is_ok());

        let redis_conn = res.unwrap();
        let raw_redis_conn = redis::ConnectionInfo::from_str("redis://0.0.0.0:1234").unwrap();
        assert_eq!(
            mem::discriminant(&redis_conn),
            mem::discriminant(&FalkorConnectionInfo::Redis(raw_redis_conn.clone()))
        );

        match redis_conn {
            FalkorConnectionInfo::Redis(conn) => {
                assert_eq!(conn.addr, raw_redis_conn.addr);
            }
            #[cfg(feature = "embedded")]
            _ => panic!("Expected Redis connection info"),
        }
    }

    #[test]
    fn test_from_addr_port() {
        let res = FalkorConnectionInfo::try_from(("127.0.0.1", 1234));
        assert!(res.is_ok());
        assert_eq!(res.unwrap().address(), "127.0.0.1:1234".to_string());
    }

    #[test]
    fn test_invalid_scheme() {
        let result = FalkorConnectionInfo::try_from("http://127.0.0.1:6379");
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_host() {
        let result = FalkorConnectionInfo::try_from("redis://:6379");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_port() {
        let result = FalkorConnectionInfo::try_from("redis://127.0.0.1:abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_unsupported_feature() {
        let result = FalkorConnectionInfo::try_from("custom://127.0.0.1:6379");
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_scheme() {
        let result = FalkorConnectionInfo::try_from("127.0.0.1:6379");
        assert!(result.is_ok());
    }

    #[test]
    #[cfg(feature = "embedded")]
    fn test_embedded_connection_info_address() {
        use crate::EmbeddedConfig;
        let config = EmbeddedConfig::default();
        let conn_info = FalkorConnectionInfo::Embedded(config);
        assert_eq!(conn_info.address(), "embedded");
    }

    #[test]
    #[cfg(feature = "embedded")]
    fn test_embedded_connection_info_clone() {
        use crate::EmbeddedConfig;
        use std::path::PathBuf;

        let config = EmbeddedConfig {
            redis_server_path: Some(PathBuf::from("/path/redis")),
            falkordb_module_path: Some(PathBuf::from("/path/falkordb.so")),
            ..Default::default()
        };
        let conn_info1 = FalkorConnectionInfo::Embedded(config);
        let conn_info2 = conn_info1.clone();

        // Both should have the same address
        assert_eq!(conn_info1.address(), conn_info2.address());
    }

    #[test]
    fn test_redis_connection_info_debug() {
        let conn_info = FalkorConnectionInfo::try_from("redis://127.0.0.1:6379").unwrap();
        let debug_str = format!("{:?}", conn_info);
        assert!(debug_str.contains("Redis"));
    }

    #[test]
    #[cfg(feature = "embedded")]
    fn test_embedded_connection_info_debug() {
        use crate::EmbeddedConfig;
        let config = EmbeddedConfig::default();
        let conn_info = FalkorConnectionInfo::Embedded(config);
        let debug_str = format!("{:?}", conn_info);
        assert!(debug_str.contains("Embedded"));
    }

    #[test]
    fn test_falkor_scheme_with_port() {
        let result = FalkorConnectionInfo::try_from("falkor://192.168.1.1:7000");
        assert!(result.is_ok());
        if let Ok(FalkorConnectionInfo::Redis(info)) = result {
            assert_eq!(info.addr.to_string(), "192.168.1.1:7000");
        }
    }

    #[test]
    fn test_falkors_scheme() {
        let result = FalkorConnectionInfo::try_from("falkors://secure.example.com:6379");
        assert!(result.is_ok());
        // Should be converted to rediss://
    }

    #[test]
    fn test_from_tuple_different_ports() {
        let result1 = FalkorConnectionInfo::try_from(("localhost", 6379));
        let result2 = FalkorConnectionInfo::try_from(("localhost", 7000));

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_ne!(result1.unwrap().address(), result2.unwrap().address());
    }
}
