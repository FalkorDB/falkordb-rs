/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{FalkorDBError, FalkorValue};
use redis::{RedisWrite, ToRedisArgs};
use std::fmt::{Display, Formatter};

/// An enum representing the two viable types for a config value
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigValue {
    /// A string value
    String(String),
    /// An int value, also used to represent booleans
    Int64(i64),
}

impl ConfigValue {
    /// Returns a copy of the contained int value, if there is one.
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self::String(_) => None,
            Self::Int64(i64) => Some(*i64),
        }
    }
}

impl Display for ConfigValue {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Self::String(str_val) => str_val.fmt(f),
            Self::Int64(int_val) => int_val.fmt(f),
        }
    }
}

impl From<i64> for ConfigValue {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<String> for ConfigValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for ConfigValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl TryFrom<FalkorValue> for ConfigValue {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::String(str_val) => Ok(Self::String(str_val)),
            FalkorValue::I64(int_val) => Ok(Self::Int64(int_val)),
            _ => Err(FalkorDBError::ParsingConfigValue),
        }
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
            Self::String(str_val) => str_val.write_redis_args(out),
            Self::Int64(int_val) => int_val.write_redis_args(out),
        }
    }
}

impl TryFrom<&redis::Value> for ConfigValue {
    type Error = FalkorDBError;
    fn try_from(value: &redis::Value) -> Result<Self, Self::Error> {
        Ok(match value {
            redis::Value::Int(int_val) => Self::Int64(*int_val),
            redis::Value::BulkString(str_data) => {
                Self::String(String::from_utf8_lossy(str_data.as_slice()).to_string())
            }
            redis::Value::SimpleString(data) => Self::String(data.to_string()),
            _ => return Err(FalkorDBError::InvalidDataReceived),
        })
    }
}

impl TryFrom<redis::Value> for ConfigValue {
    type Error = FalkorDBError;

    fn try_from(value: redis::Value) -> Result<Self, Self::Error> {
        Ok(match value {
            redis::Value::Int(int_val) => Self::Int64(int_val),
            redis::Value::BulkString(str_data) => {
                Self::String(String::from_utf8(str_data).map_err(|_| FalkorDBError::ParsingString)?)
            }
            redis::Value::SimpleString(data) => Self::String(data),
            _ => return Err(FalkorDBError::InvalidDataReceived),
        })
    }
}
