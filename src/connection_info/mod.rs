/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorResult};

/// An agnostic container which allows maintaining of various connection details.
/// The different enum variants are enabled based on compilation features
#[derive(Clone, Debug)]
pub enum FalkorConnectionInfo {
    #[cfg(feature = "redis")]
    /// A Redis database connection
    Redis(redis::ConnectionInfo),
}

impl FalkorConnectionInfo {
    fn fallback_provider(mut full_url: String) -> FalkorResult<FalkorConnectionInfo> {
        #[cfg(feature = "redis")]
        Ok(FalkorConnectionInfo::Redis({
            if full_url.starts_with("falkor://") {
                full_url = full_url.replace("falkor://", "redis://");
            } else if full_url.starts_with("falkors://") {
                full_url = full_url.replace("falkors://", "rediss://");
            }
            redis::IntoConnectionInfo::into_connection_info(full_url)
                .map_err(|_| FalkorDBError::InvalidConnectionInfo)?
        }))
    }

    /// Retrieves the internally stored address for this connection info
    ///
    /// # Returns
    /// A [`String`] representation of the address and port, or a UNIX socket path
    pub fn address(&self) -> String {
        match self {
            #[cfg(feature = "redis")]
            FalkorConnectionInfo::Redis(redis_info) => redis_info.addr.to_string(),
        }
    }
}

impl TryFrom<&str> for FalkorConnectionInfo {
    type Error = FalkorDBError;

    fn try_from(value: &str) -> FalkorResult<Self> {
        let url = url_parse::core::Parser::new(None)
            .parse(value)
            .map_err(|_| FalkorDBError::InvalidConnectionInfo)?;

        // The url_parse serializer seems ***ed up for some reason
        let scheme = url.scheme.unwrap_or("falkor".to_string());
        let user_pass_string = match url.user_pass {
            (Some(pass), None) => format!("{}@", pass), // Password-only authentication is allowed in legacy auth
            (Some(user), Some(pass)) => format!("{user}:{pass}@"),
            _ => "".to_string(),
        };
        let subdomain = url
            .subdomain
            .map(|subdomain| format!("{subdomain}."))
            .unwrap_or_default();

        let domain = url.domain.unwrap_or("127.0.0.1".to_string());
        let top_level_domain = url
            .top_level_domain
            .map(|top_level_domain| format!(".{top_level_domain}"))
            .unwrap_or_default();
        let port = url.port.unwrap_or(6379); // Might need to change in accordance with the default fallback
        let serialized = format!(
            "{}://{}{}{}{}:{}",
            scheme, user_pass_string, subdomain, domain, top_level_domain, port
        );

        match scheme.as_str() {
            "redis" | "rediss" | "redis+unix" => {
                #[cfg(feature = "redis")]
                return Ok(FalkorConnectionInfo::Redis(
                    redis::IntoConnectionInfo::into_connection_info(serialized)
                        .map_err(|_| FalkorDBError::InvalidConnectionInfo)?,
                ));
                #[cfg(not(feature = "redis"))]
                return Err(FalkorDBError::UnavailableProvider);
            }
            _ => FalkorConnectionInfo::fallback_provider(serialized),
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
    #[cfg(feature = "redis")]
    fn test_redis_fallback_provider() {
        let FalkorConnectionInfo::Redis(redis) =
            FalkorConnectionInfo::fallback_provider("redis://127.0.0.1:6379".to_string()).unwrap();

        assert_eq!(redis.addr.to_string(), "127.0.0.1:6379".to_string());
    }

    #[test]
    #[cfg(feature = "redis")]
    fn test_try_from_redis() {
        let res = FalkorConnectionInfo::try_from("redis://0.0.0.0:1234");
        assert!(res.is_ok());

        let redis_conn = res.unwrap();
        let raw_redis_conn = redis::ConnectionInfo::from_str("redis://0.0.0.0:1234").unwrap();
        assert_eq!(
            mem::discriminant(&redis_conn),
            mem::discriminant(&FalkorConnectionInfo::Redis(raw_redis_conn.clone()))
        );

        let FalkorConnectionInfo::Redis(conn) = redis_conn;
        assert_eq!(conn.addr, raw_redis_conn.addr);
    }

    #[test]
    fn test_from_addr_port() {
        let res = FalkorConnectionInfo::try_from(("127.0.0.1", 1234));
        assert!(res.is_ok());
        assert_eq!(res.unwrap().address(), "127.0.0.1:1234".to_string());
    }
}
