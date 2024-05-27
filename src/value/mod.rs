/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection, FalkorDBError, FalkorParsable, SyncGraphSchema,
};
use anyhow::Result;
use graph_entities::{Edge, Node};
use path::Path;
use point::Point;
use std::{collections::HashMap, fmt::Debug};

#[cfg(feature = "tokio")]
use crate::{
    connection::asynchronous::BorrowedAsyncConnection, AsyncGraphSchema, FalkorAsyncParseable,
};

pub(crate) mod config;
pub(crate) mod graph_entities;
pub(crate) mod map;
pub(crate) mod path;
pub(crate) mod point;
pub(crate) mod utils;

#[cfg(feature = "tokio")]
pub(crate) mod utils_async;

/// An enum of all the supported Falkor types
#[derive(Clone, Debug, PartialEq)]
pub enum FalkorValue {
    FNode(Node),
    FEdge(Edge),
    FArray(Vec<FalkorValue>),
    FMap(HashMap<String, FalkorValue>),
    FString(String),
    FBool(bool),
    Int64(i64),
    F64(f64),
    FPoint(Point),
    FPath(Path),
    None,
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

impl_to_falkordb_value!(u8, Self::Int64);
impl_to_falkordb_value!(u32, Self::Int64);
impl_to_falkordb_value!(u64, Self::Int64);

impl_to_falkordb_value!(f32, Self::F64);
impl_to_falkordb_value!(f64, Self::F64);

impl_to_falkordb_value!(String, Self::FString);

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
        Self::FArray(
            value
                .into_iter()
                .map(|element| FalkorValue::from(element))
                .collect(),
        )
    }
}

impl TryFrom<FalkorValue> for Vec<FalkorValue> {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::FArray(val) => Ok(val),
            _ => Err(FalkorDBError::ParsingFArray),
        }
    }
}

impl TryFrom<FalkorValue> for f64 {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::FString(f64_str) => f64_str.parse().map_err(|_| FalkorDBError::ParsingF64),
            FalkorValue::F64(f64_val) => Ok(f64_val),
            _ => Err(FalkorDBError::ParsingF64),
        }
    }
}

impl TryFrom<FalkorValue> for String {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::FString(val) => Ok(val),
            _ => Err(FalkorDBError::ParsingFString),
        }
    }
}

impl TryFrom<FalkorValue> for Edge {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::FEdge(edge) => Ok(edge),
            _ => Err(FalkorDBError::ParsingFEdge),
        }
    }
}

impl TryFrom<FalkorValue> for Node {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::FNode(node) => Ok(node),
            _ => Err(FalkorDBError::ParsingFNode),
        }
    }
}

impl TryFrom<FalkorValue> for Path {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::FPath(path) => Ok(path),
            _ => Err(FalkorDBError::ParsingFPath),
        }
    }
}

impl TryFrom<FalkorValue> for HashMap<String, FalkorValue> {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::FMap(map) => Ok(map),
            _ => Err(FalkorDBError::ParsingFMap),
        }
    }
}

impl TryFrom<FalkorValue> for Point {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        match value {
            FalkorValue::FPoint(point) => Ok(point),
            _ => Err(FalkorDBError::ParsingFPoint),
        }
    }
}

impl FalkorParsable for FalkorValue {
    fn from_falkor_value(
        value: FalkorValue,
        _graph_schema: &SyncGraphSchema,
        _conn: &mut BorrowedSyncConnection,
    ) -> Result<Self> {
        Ok(value)
    }
}

#[cfg(feature = "tokio")]
impl FalkorAsyncParseable for FalkorValue {
    async fn from_falkor_value_async(
        value: FalkorValue,
        _graph_schema: &AsyncGraphSchema,
        _conn: &mut BorrowedAsyncConnection,
    ) -> Result<Self> {
        Ok(value)
    }
}

impl FalkorValue {
    /// Returns a reference to the internal [`Vec`] if this is an FArray variant.
    ///
    /// # Returns
    /// A reference to the internal [`Vec`]
    pub fn as_vec(&self) -> Option<&Vec<Self>> {
        match self {
            FalkorValue::FArray(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a reference to the internal [`String`] if this is an FString variant.
    ///
    /// # Returns
    /// A reference to the internal [`String`]
    pub fn as_string(&self) -> Option<&String> {
        match self {
            FalkorValue::FString(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a reference to the internal [`Edge`] if this is an FEdge variant.
    ///
    /// # Returns
    /// A reference to the internal [`Edge`]
    pub fn as_edge(&self) -> Option<&Edge> {
        match self {
            FalkorValue::FEdge(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a reference to the internal [`Node`] if this is an FNode variant.
    ///
    /// # Returns
    /// A reference to the internal [`Node`]
    pub fn as_node(&self) -> Option<&Node> {
        match self {
            FalkorValue::FNode(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a reference to the internal [`Path`] if this is an FPath variant.
    ///
    /// # Returns
    /// A reference to the internal [`Path`]
    pub fn as_path(&self) -> Option<&Path> {
        match self {
            FalkorValue::FPath(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a reference to the internal [`HashMap`] if this is an FMap variant.
    ///
    /// # Returns
    /// A reference to the internal [`HashMap`]
    pub fn as_map(&self) -> Option<&HashMap<String, FalkorValue>> {
        match self {
            FalkorValue::FMap(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a reference to the internal [`Point`] if this is an FPoint variant.
    ///
    /// # Returns
    /// A reference to the internal [`Point`]
    pub fn as_point(&self) -> Option<&Point> {
        match self {
            FalkorValue::FPoint(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a Copy of the inner [`i64`] if this is an Int64 variant
    ///
    /// # Returns
    /// A copy of the inner [`i64`]
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            FalkorValue::Int64(val) => Some(*val),
            _ => None,
        }
    }

    /// Returns a Copy of the inner [`bool`] if this is an FBool variant
    ///
    /// # Returns
    /// A copy of the inner [`bool`]
    pub fn to_bool(&self) -> Option<bool> {
        match self {
            FalkorValue::FBool(val) => Some(*val),
            FalkorValue::FString(bool_str) => match bool_str.as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    /// Returns a Copy of the inner [`f64`] if this is an F64 variant
    ///
    /// # Returns
    /// A copy of the inner [`f64`]
    pub fn to_f64(&self) -> Option<f64> {
        match self {
            FalkorValue::F64(val) => Some(*val),
            _ => None,
        }
    }

    /// Consumes itself and returns the inner [`Vec`] if this is an FArray variant
    ///
    /// # Returns
    /// The inner [`Vec`]
    pub fn into_vec(self) -> Result<Vec<Self>, FalkorDBError> {
        self.try_into()
    }

    /// Consumes itself and returns the inner [`String`] if this is an FString variant
    ///
    /// # Returns
    /// The inner [`String`]
    pub fn into_string(self) -> Result<String, FalkorDBError> {
        self.try_into()
    }
}
