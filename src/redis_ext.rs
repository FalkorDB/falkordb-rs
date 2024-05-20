/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::client::FalkorClientImpl;
use crate::connection::{asynchronous::FalkorAsyncConnection, blocking::FalkorSyncConnection};
use crate::connection_info::FalkorConnectionInfo;
use crate::error::FalkorDBError;
use crate::value::config::ConfigValue;
use crate::value::query_result::QueryResult;
use crate::value::FalkorValue;
use anyhow::Result;
use redis::{FromRedisValue, RedisResult, RedisWrite, ToRedisArgs};
use std::collections::HashMap;

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

fn query_parse_header(header: redis::Value) -> Result<Vec<String>> {
    let header_vec = header
        .into_sequence()
        .map_err(|_| FalkorDBError::ParsingError)?;
    let header_len = header_vec.len();

    let keys = header_vec
        .into_iter()
        .flat_map(redis::Value::into_sequence)
        .flat_map(TryInto::<[redis::Value; 2]>::try_into)
        .map(|element| element.into_iter().nth(1).unwrap())
        .filter_map(|element| match element {
            redis::Value::Data(data) => Some(String::from_utf8_lossy(data.as_slice()).to_string()),
            redis::Value::Status(data) => Some(data),
            _ => None,
        })
        .collect::<Vec<_>>();

    if keys.len() != header_len {
        Err(FalkorDBError::ParsingError)?;
    }

    Ok(keys)
}

fn query_parse_stats(stats: redis::Value) -> Result<Vec<String>> {
    let stats_vec = stats
        .into_sequence()
        .map_err(|_| FalkorDBError::ParsingError)?;
    let stats_len = stats_vec.len();

    let stats_strings = stats_vec
        .into_iter()
        .filter_map(|element| match element {
            redis::Value::Data(data) => Some(String::from_utf8_lossy(data.as_slice()).to_string()),
            redis::Value::Status(data) => Some(data),
            _ => None,
        })
        .collect::<Vec<_>>();

    if stats_strings.len() != stats_len {
        Err(FalkorDBError::ParsingError)?;
    }

    Ok(stats_strings)
}

// TODO: add support for map here
fn parse_toplevel_result(value: Vec<redis::Value>) -> Result<FalkorValue> {
    let [type_marker, val]: [redis::Value; 2] =
        value.try_into().map_err(|_| FalkorDBError::ParsingError)?;

    let _type_marker = match type_marker {
        redis::Value::Int(type_marker) => Ok(type_marker),
        _ => Err(FalkorDBError::ParsingError),
    }?;

    Ok(FalkorValue::from_owned_redis_value(val)?)
}

impl TryFrom<redis::Value> for QueryResult {
    type Error = anyhow::Error;

    fn try_from(value: redis::Value) -> Result<Self, Self::Error> {
        let value_vec = value
            .into_sequence()
            .map_err(|_| FalkorDBError::ParsingError)?;

        // Empty result
        if value_vec.is_empty() {
            return Ok(QueryResult::default());
        }

        // Result only contains stats
        if value_vec.len() == 1 {
            let first_element = value_vec.into_iter().nth(0).unwrap();
            return Ok(QueryResult {
                stats: query_parse_stats(first_element)?,
                ..Default::default()
            });
        }

        // Invalid response
        if value_vec.len() == 2 {
            Err(FalkorDBError::InvalidDataReceived)?;
        }

        // Full result
        let [header, data, stats]: [redis::Value; 3] = value_vec
            .try_into()
            .map_err(|_| FalkorDBError::ParsingError)?;

        let header_keys = query_parse_header(header)?;
        let stats_strings = query_parse_stats(stats)?;

        let data_vec = data
            .into_sequence()
            .map_err(|_| FalkorDBError::ParsingError)?;
        let data_len = data_vec.len();

        let result_set: Vec<_> = data_vec
            .into_iter()
            .flat_map(|element| {
                let element_as_vec = match element.into_sequence() {
                    Ok(element_as_vec) => element_as_vec,
                    Err(_) => {
                        return Err(FalkorDBError::ParsingError);
                    }
                };

                let element_falkor_vals = element_as_vec
                    .into_iter()
                    .flat_map(|element| {
                        let top_level_element = element
                            .into_sequence()
                            .map_err(|_| FalkorDBError::ParsingError)?;

                        parse_toplevel_result(top_level_element)
                    })
                    .collect::<Vec<_>>();

                if element_falkor_vals.len() != header_keys.len() {
                    return Err(FalkorDBError::ParsingError);
                }

                Ok(header_keys
                    .iter()
                    .cloned()
                    .zip(element_falkor_vals)
                    .collect::<HashMap<_, _>>())
            })
            .collect();

        if result_set.len() != data_len {
            Err(FalkorDBError::ParsingError)?;
        }

        Ok(QueryResult {
            stats: stats_strings,
            header: header_keys,
            result_set,
        })
    }
}
