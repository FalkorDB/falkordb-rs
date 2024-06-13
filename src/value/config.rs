/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorValue};
use redis::{RedisWrite, ToRedisArgs};
use std::fmt::{Display, Formatter};

/// An enum representing the two viable types for a config value
#[derive(Clone, Debug, PartialEq)]
pub enum ConfigValue {
    /// A string value
    String(String),
    /// An int value, also used to represent booleans
    Int64(i64),
}

impl ConfigValue {
    /// Returns a copy of the contained int value, if there is one.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ConfigValue::String(_) => None,
            ConfigValue::Int64(i64) => Some(*i64),
        }
    }
}

impl Display for ConfigValue {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            ConfigValue::String(str_val) => str_val.fmt(f),
            ConfigValue::Int64(int_val) => int_val.fmt(f),
        }
    }
}

impl From<i64> for ConfigValue {
    fn from(value: i64) -> Self {
        ConfigValue::Int64(value)
    }
}

impl From<String> for ConfigValue {
    fn from(value: String) -> Self {
        ConfigValue::String(value)
    }
}

impl From<&str> for ConfigValue {
    fn from(value: &str) -> Self {
        ConfigValue::String(value.to_string())
    }
}

impl TryFrom<FalkorValue> for ConfigValue {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::String(str_val) => Ok(ConfigValue::String(str_val)),
            FalkorValue::I64(int_val) => Ok(ConfigValue::Int64(int_val)),
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
                String::from_utf8(str_data).map_err(|_| FalkorDBError::ParsingString)?,
            ),
            _ => return Err(FalkorDBError::InvalidDataReceived),
        })
    }
}
