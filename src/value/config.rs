/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
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
            redis::Value::BulkString(str_data) => {
                ConfigValue::String(String::from_utf8_lossy(str_data.as_slice()).to_string())
            }
            redis::Value::SimpleString(data) => ConfigValue::String(data.to_string()),
            _ => return Err(FalkorDBError::InvalidDataReceived),
        })
    }
}

impl TryFrom<redis::Value> for ConfigValue {
    type Error = FalkorDBError;

    fn try_from(value: redis::Value) -> Result<Self, Self::Error> {
        Ok(match value {
            redis::Value::Int(int_val) => ConfigValue::Int64(int_val),
            redis::Value::BulkString(str_data) => ConfigValue::String(
                String::from_utf8(str_data).map_err(|_| FalkorDBError::ParsingString)?,
            ),
            redis::Value::SimpleString(data) => ConfigValue::String(data),
            _ => return Err(FalkorDBError::InvalidDataReceived),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_value_as_i64() {
        let int_val = ConfigValue::Int64(42);
        assert_eq!(int_val.as_i64(), Some(42));

        let str_val = ConfigValue::String("test".to_string());
        assert_eq!(str_val.as_i64(), None);
    }

    #[test]
    fn test_config_value_display() {
        let int_val = ConfigValue::Int64(100);
        assert_eq!(format!("{}", int_val), "100");

        let str_val = ConfigValue::String("hello".to_string());
        assert_eq!(format!("{}", str_val), "hello");
    }

    #[test]
    fn test_config_value_from_i64() {
        let val: ConfigValue = 123i64.into();
        assert_eq!(val, ConfigValue::Int64(123));
    }

    #[test]
    fn test_config_value_from_string() {
        let val: ConfigValue = "test".to_string().into();
        assert_eq!(val, ConfigValue::String("test".to_string()));
    }

    #[test]
    fn test_config_value_from_str() {
        let val: ConfigValue = "test".into();
        assert_eq!(val, ConfigValue::String("test".to_string()));
    }

    #[test]
    fn test_config_value_try_from_falkor_value_string() {
        let falkor_val = FalkorValue::String("test".to_string());
        let result = ConfigValue::try_from(falkor_val);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ConfigValue::String("test".to_string()));
    }

    #[test]
    fn test_config_value_try_from_falkor_value_i64() {
        let falkor_val = FalkorValue::I64(42);
        let result = ConfigValue::try_from(falkor_val);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ConfigValue::Int64(42));
    }

    #[test]
    fn test_config_value_try_from_falkor_value_error() {
        let falkor_val = FalkorValue::Bool(true);
        let result = ConfigValue::try_from(falkor_val);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FalkorDBError::ParsingConfigValue));
    }

    #[test]
    fn test_config_value_try_from_redis_value_int() {
        let redis_val = redis::Value::Int(42);
        let result = ConfigValue::try_from(redis_val);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ConfigValue::Int64(42));
    }

    #[test]
    fn test_config_value_try_from_redis_value_bulk_string() {
        let redis_val = redis::Value::BulkString("test".as_bytes().to_vec());
        let result = ConfigValue::try_from(redis_val);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ConfigValue::String("test".to_string()));
    }

    #[test]
    fn test_config_value_try_from_redis_value_simple_string() {
        let redis_val = redis::Value::SimpleString("test".to_string());
        let result = ConfigValue::try_from(redis_val);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ConfigValue::String("test".to_string()));
    }

    #[test]
    fn test_config_value_try_from_redis_value_error() {
        let redis_val = redis::Value::Nil;
        let result = ConfigValue::try_from(redis_val);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FalkorDBError::InvalidDataReceived));
    }

    #[test]
    fn test_config_value_try_from_redis_value_ref_int() {
        let redis_val = redis::Value::Int(42);
        let result = ConfigValue::try_from(&redis_val);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ConfigValue::Int64(42));
    }

    #[test]
    fn test_config_value_try_from_redis_value_ref_bulk_string() {
        let redis_val = redis::Value::BulkString("test".as_bytes().to_vec());
        let result = ConfigValue::try_from(&redis_val);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ConfigValue::String("test".to_string()));
    }

    #[test]
    fn test_config_value_try_from_redis_value_ref_simple_string() {
        let redis_val = redis::Value::SimpleString("test".to_string());
        let result = ConfigValue::try_from(&redis_val);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), ConfigValue::String("test".to_string()));
    }

    #[test]
    fn test_config_value_try_from_redis_value_ref_error() {
        let redis_val = redis::Value::Nil;
        let result = ConfigValue::try_from(&redis_val);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FalkorDBError::InvalidDataReceived));
    }

    #[test]
    fn test_config_value_clone() {
        let val1 = ConfigValue::Int64(42);
        let val2 = val1.clone();
        assert_eq!(val1, val2);

        let val3 = ConfigValue::String("test".to_string());
        let val4 = val3.clone();
        assert_eq!(val3, val4);
    }

    #[test]
    fn test_config_value_debug() {
        let int_val = ConfigValue::Int64(42);
        assert_eq!(format!("{:?}", int_val), "Int64(42)");

        let str_val = ConfigValue::String("test".to_string());
        assert!(format!("{:?}", str_val).contains("test"));
    }

    #[test]
    fn test_config_value_to_redis_args() {
        use redis::ToRedisArgs;
        
        let int_val = ConfigValue::Int64(42);
        let args = int_val.to_redis_args();
        assert!(!args.is_empty());

        let str_val = ConfigValue::String("test".to_string());
        let args = str_val.to_redis_args();
        assert!(!args.is_empty());
    }
}
