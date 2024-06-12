/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{ConfigValue, FalkorDBError, FalkorResult, FalkorValue};
use redis::{FromRedisValue, RedisResult, RedisWrite, ToRedisArgs};

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

impl TryFrom<redis::Value> for ConfigValue {
    type Error = FalkorDBError;

    fn try_from(value: redis::Value) -> Result<Self, Self::Error> {
        Ok(match value {
            redis::Value::Int(int_val) => ConfigValue::Int64(int_val),
            redis::Value::Data(str_data) => ConfigValue::String(
                String::from_utf8(str_data).map_err(|_| FalkorDBError::ParsingFString)?,
            ),
            _ => return Err(FalkorDBError::InvalidDataReceived),
        })
    }
}

impl FromRedisValue for FalkorValue {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        Ok(match v {
            redis::Value::Nil => FalkorValue::None,
            redis::Value::Int(int_val) => FalkorValue::I64(*int_val),
            redis::Value::Data(str_val) => {
                FalkorValue::String(String::from_utf8_lossy(str_val.as_slice()).to_string())
            }
            redis::Value::Bulk(bulk) => FalkorValue::Array(
                bulk.iter()
                    .flat_map(FalkorValue::from_redis_value)
                    .collect(),
            ),
            redis::Value::Status(status) => FalkorValue::String(status.to_string()),
            redis::Value::Okay => FalkorValue::None,
        })
    }
}

pub(crate) fn redis_value_as_string(value: redis::Value) -> FalkorResult<String> {
    match value {
        redis::Value::Data(data) => {
            String::from_utf8(data).map_err(|_| FalkorDBError::ParsingFString)
        }
        redis::Value::Status(status) => Ok(status),
        _ => Err(FalkorDBError::ParsingFString),
    }
}

pub(crate) fn redis_value_as_int(value: redis::Value) -> FalkorResult<i64> {
    match value {
        redis::Value::Int(int_val) => Ok(int_val),
        _ => Err(FalkorDBError::ParsingI64),
    }
}

pub(crate) fn redis_value_as_bool(value: redis::Value) -> FalkorResult<bool> {
    redis_value_as_string(value).and_then(|string_val| match string_val.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(FalkorDBError::ParsingBool),
    })
}

pub(crate) fn redis_value_as_double(value: redis::Value) -> FalkorResult<f64> {
    redis_value_as_string(value)
        .and_then(|string_val| string_val.parse().map_err(|_| FalkorDBError::ParsingF64))
}
