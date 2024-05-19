/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::client::FalkorClientImpl;
use crate::connection::{asynchronous::FalkorAsyncConnection, blocking::FalkorSyncConnection};
use crate::connection_info::FalkorConnectionInfo;
use crate::error::FalkorDBError;
use crate::value::config::ConfigValue;
use crate::value::FalkorValue;
use redis::{FromRedisValue, RedisResult, RedisWrite, ToRedisArgs};

impl From<redis::Connection> for FalkorSyncConnection {
    fn from(value: redis::Connection) -> Self {
        Self::Redis(value)
    }
}

#[cfg(feature = "tokio")]
impl From<redis::aio::MultiplexedConnection> for FalkorAsyncConnection {
    fn from(value: redis::aio::MultiplexedConnection) -> Self {
        Self::Redis(value)
    }
}

impl From<redis::ConnectionInfo> for FalkorConnectionInfo {
    fn from(value: redis::ConnectionInfo) -> Self {
        Self::Redis(value)
    }
}

impl From<redis::Client> for FalkorClientImpl {
    fn from(value: redis::Client) -> Self {
        Self::Redis(value)
    }
}

impl ToRedisArgs for ConfigValue {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            ConfigValue::String(str_val) => str_val.write_redis_args(out),
            ConfigValue::Int64(int_val) => int_val.write_redis_args(out),
        }
    }
}

impl ToRedisArgs for FalkorValue {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            FalkorValue::None => {}
            FalkorValue::Int64(int_data) => int_data.write_redis_args(out),
            FalkorValue::UInt64(uint_data) => uint_data.write_redis_args(out),
            FalkorValue::F64(f_data) => f_data.write_redis_args(out),
            FalkorValue::FString(str_data) => str_data.write_redis_args(out),
            FalkorValue::FVec(bulk_data) => {
                ToRedisArgs::write_args_from_slice(bulk_data.as_slice(), out)
            }
        }
    }

    fn describe_numeric_behavior(&self) -> redis::NumericBehavior {
        match self {
            FalkorValue::Int64(_) => redis::NumericBehavior::NumberIsInteger,
            FalkorValue::UInt64(_) => redis::NumericBehavior::NumberIsInteger,
            FalkorValue::F64(_) => redis::NumericBehavior::NumberIsFloat,
            _ => redis::NumericBehavior::NonNumeric,
        }
    }

    fn is_single_arg(&self) -> bool {
        match self {
            FalkorValue::None => false,
            FalkorValue::FVec(bulk_data) => bulk_data.is_single_arg(),
            _ => true,
        }
    }
}

impl TryFrom<&redis::Value> for ConfigValue {
    type Error = anyhow::Error;
    fn try_from(value: &redis::Value) -> Result<ConfigValue, Self::Error> {
        Ok(match value {
            redis::Value::Int(int_val) => ConfigValue::Int64(*int_val),
            redis::Value::Data(str_data) => {
                ConfigValue::String(String::from_utf8_lossy(str_data.as_slice()).to_string())
            }
            _ => return Err(FalkorDBError::InvalidDataReceived.into()),
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
            redis::Value::Bulk(bulk) => FalkorValue::FVec({
                let mut new_vec = Vec::with_capacity(bulk.len());
                for element in bulk {
                    new_vec.push(FalkorValue::from_redis_value(element)?);
                }
                new_vec
            }),
            redis::Value::Status(status) => FalkorValue::FString(status.to_string()),
            redis::Value::Okay => FalkorValue::None,
        })
    }
}
