/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::error::FalkorDBError;
use anyhow::Result;
use std::fmt::{Debug, Display, Formatter};

pub mod config;
mod query_result;
pub mod slowlog_entry;

#[derive(Clone, Debug)]
pub enum FalkorValue {
    None,
    Int64(i64),
    UInt64(u64),
    F64(f64),
    FString(String),
    FVec(Vec<FalkorValue>),
}

macro_rules! impl_to_falkordb_value {
    ($t:ty, $falkordbtype:expr) => {
        impl From<$t> for FalkorValue {
            fn from(value: $t) -> Self {
                $falkordbtype(value as _)
            }
        }
    };
}

impl_to_falkordb_value!(i8, Self::Int64);
impl_to_falkordb_value!(i32, Self::Int64);
impl_to_falkordb_value!(i64, Self::Int64);

impl_to_falkordb_value!(u8, Self::UInt64);
impl_to_falkordb_value!(u32, Self::UInt64);
impl_to_falkordb_value!(u64, Self::UInt64);

impl_to_falkordb_value!(f32, Self::F64);
impl_to_falkordb_value!(f64, Self::F64);

impl Display for FalkorValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FalkorValue::None => Ok(()),
            FalkorValue::Int64(val) => Display::fmt(val, f),
            FalkorValue::UInt64(val) => Display::fmt(val, f),
            FalkorValue::F64(val) => Display::fmt(val, f),
            FalkorValue::FString(val) => f.write_fmt(format_args!("\"{val}\"")),
            FalkorValue::FVec(val) => {
                f.write_str("[")?;
                let val_len = val.len() - 1;
                for (idx, element) in val.iter().enumerate() {
                    Display::fmt(element, f)?;
                    if idx < val_len {
                        f.write_str(",")?;
                    }
                }
                f.write_str("]")
            }
        }
    }
}

impl TryFrom<&FalkorValue> for i64 {
    type Error = FalkorDBError;

    fn try_from(value: &FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::Int64(val) => Some(*val),
            FalkorValue::UInt64(val) => (*val).try_into().ok(),
            FalkorValue::FString(val) => val.as_str().parse().ok(),
            _ => None,
        }
        .ok_or(FalkorDBError::ParsingError)
    }
}

impl TryFrom<FalkorValue> for i64 {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::Int64(val) => Some(val),
            FalkorValue::UInt64(val) => val.try_into().ok(),
            FalkorValue::FString(val) => val.as_str().parse().ok(),
            _ => None,
        }
        .ok_or(FalkorDBError::ParsingError)
    }
}

impl TryFrom<&FalkorValue> for u64 {
    type Error = FalkorDBError;

    fn try_from(value: &FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::UInt64(val) => Some(*val),
            FalkorValue::Int64(val) => (*val).try_into().ok(),
            FalkorValue::FString(val) => val.as_str().parse().ok(),
            _ => None,
        }
        .ok_or(FalkorDBError::ParsingError)
    }
}

impl TryFrom<FalkorValue> for u64 {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::UInt64(val) => Some(val),
            FalkorValue::Int64(val) => val.try_into().ok(),
            FalkorValue::FString(val) => val.as_str().parse().ok(),
            _ => None,
        }
        .ok_or(FalkorDBError::ParsingError)
    }
}

impl FalkorValue {
    pub fn as_vec(&self) -> Result<&Vec<Self>> {
        match self {
            FalkorValue::FVec(val) => Some(val),
            _ => None,
        }
        .ok_or(FalkorDBError::ParsingError.into())
    }
    pub fn into_vec(self) -> Result<Vec<Self>> {
        match self {
            FalkorValue::FVec(val) => Some(val),
            _ => None,
        }
        .ok_or(FalkorDBError::ParsingError.into())
    }

    pub fn as_string(&self) -> Result<&String> {
        match self {
            FalkorValue::FString(val) => Some(val),
            _ => None,
        }
        .ok_or(FalkorDBError::ParsingError.into())
    }

    pub fn into_string(self) -> Result<String> {
        match self {
            FalkorValue::FString(val) => Some(val),
            _ => None,
        }
        .ok_or(FalkorDBError::ParsingError.into())
    }
}

impl From<String> for FalkorValue {
    fn from(value: String) -> Self {
        Self::FString(value)
    }
}

impl From<&str> for FalkorValue {
    fn from(value: &str) -> Self {
        Self::FString(value.to_string())
    }
}

impl<T> From<Vec<T>> for FalkorValue
where
    FalkorValue: From<T>,
{
    fn from(value: Vec<T>) -> Self {
        Self::FVec(
            value
                .into_iter()
                .map(|element| FalkorValue::from(element))
                .collect(),
        )
    }
}
