/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::FalkorClientProvider, connection::blocking::FalkorSyncConnection, ConfigValue,
    FalkorConnectionInfo, FalkorDBError, FalkorValue,
};
use anyhow::Result;
use redis::{FromRedisValue, RedisResult, RedisWrite, ToRedisArgs};

impl From<redis::Connection> for FalkorSyncConnection {
    fn from(value: redis::Connection) -> Self {
        Self::Redis(value)
    }
}

#[cfg(feature = "tokio")]
impl From<redis::aio::MultiplexedConnection> for crate::FalkorAsyncConnection {
    fn from(value: redis::aio::MultiplexedConnection) -> Self {
        Self::Redis(value)
    }
}

impl From<redis::ConnectionInfo> for FalkorConnectionInfo {
    fn from(value: redis::ConnectionInfo) -> Self {
        Self::Redis(value)
    }
}

impl From<redis::Client> for FalkorClientProvider {
    fn from(value: redis::Client) -> Self {
        Self::Redis(value)
    }
}

impl ToRedisArgs for ConfigValue {
    fn write_redis_args<W>(
        &self,
        out: &mut W,
    ) where
        W: ?Sized + RedisWrite,
    {
        match self {
            ConfigValue::String(str_val) => str_val.write_redis_args(out),
            ConfigValue::Int64(int_val) => int_val.write_redis_args(out),
        }
    }
}

impl TryFrom<&redis::Value> for ConfigValue {
    type Error = FalkorDBError;
    fn try_from(value: &redis::Value) -> Result<ConfigValue, Self::Error> {
        Ok(match value {
            redis::Value::Int(int_val) => ConfigValue::Int64(*int_val),
            redis::Value::Data(str_data) => {
                ConfigValue::String(String::from_utf8_lossy(str_data.as_slice()).to_string())
            }
            _ => return Err(FalkorDBError::InvalidDataReceived),
        })
    }
}

impl FromRedisValue for FalkorValue {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        Ok(match v {
            redis::Value::Nil => FalkorValue::None,
            redis::Value::Int(int_val) => FalkorValue::Int64(*int_val),
            redis::Value::Data(str_val) => {
                FalkorValue::FString(String::from_utf8_lossy(str_val.as_slice()).to_string())
            }
            redis::Value::Bulk(bulk) => FalkorValue::FArray(
                bulk.iter()
                    .flat_map(FalkorValue::from_redis_value)
                    .collect(),
            ),
            redis::Value::Status(status) => FalkorValue::FString(status.to_string()),
            redis::Value::Okay => FalkorValue::None,
        })
    }
}
