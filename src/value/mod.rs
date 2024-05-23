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

pub(crate) mod config;
pub(crate) mod graph_entities;
pub(crate) mod map;
pub(crate) mod path;
pub(crate) mod point;
pub(crate) mod utils;

#[cfg(feature = "tokio")]
pub(crate) mod utils_async;

/// An enum of all the supported Falkor types
#[derive(Clone, Debug)]
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
    FVector(Vec<f32>),
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
impl crate::FalkorAsyncParseable for FalkorValue {
    async fn from_falkor_value_async(
        value: FalkorValue,
        _graph_schema: &crate::AsyncGraphSchema,
        _conn: &mut crate::FalkorAsyncConnection,
    ) -> Result<Self> {
        Ok(value)
    }
}

// By-reference conversions
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
}

// Consuming conversions
// TODO: convert these to TryFrom/TryInto traits?
impl FalkorValue {
    /// Consumes this variant and returns the underlying [`Vec`] if this is an FArray variant
    ///
    /// # Returns
    /// The inner [`Vec`]
    pub fn into_vec(self) -> Result<Vec<Self>, FalkorDBError> {
        match self {
            FalkorValue::FArray(val) => Some(val),
            _ => None,
        }
        .ok_or(FalkorDBError::ParsingFArray)
    }

    /// Consumes this variant and returns the underlying [`String`] if this is an FString variant
    ///
    /// # Returns
    /// The inner [`String`]
    pub fn into_string(self) -> Result<String, FalkorDBError> {
        match self {
            FalkorValue::FString(val) => Ok(val),
            _ => Err(FalkorDBError::ParsingFString),
        }
    }

    /// Consumes this variant and returns the underlying [`Edge`] if this is an FEdge variant
    ///
    /// # Returns
    /// The inner [`Edge`]
    pub fn into_edge(self) -> Result<Edge, FalkorDBError> {
        match self {
            Self::FEdge(edge) => Ok(edge),
            _ => Err(FalkorDBError::ParsingFEdge),
        }
    }

    /// Consumes this variant and returns the underlying [`Node`] if this is an FNode variant
    ///
    /// # Returns
    /// The inner [`Node`]
    pub fn into_node(self) -> Result<Node, FalkorDBError> {
        match self {
            Self::FNode(node) => Ok(node),
            _ => Err(FalkorDBError::ParsingFNode),
        }
    }

    /// Consumes this variant and returns the underlying [`Path`] if this is an FPath variant
    ///
    /// # Returns
    /// The inner [`Path`]
    pub fn into_path(self) -> Result<Path, FalkorDBError> {
        match self {
            Self::FPath(path) => Ok(path),
            _ => Err(FalkorDBError::ParsingFPath),
        }
    }

    /// Consumes this variant and returns the underlying [`HashMap`] if this is an FMap variant
    ///
    /// # Returns
    /// The inner [`HashMap`]
    pub fn into_map(self) -> Result<HashMap<String, FalkorValue>, FalkorDBError> {
        match self {
            FalkorValue::FMap(map) => Ok(map),
            _ => Err(FalkorDBError::ParsingFMap),
        }
    }

    /// Consumes this variant and returns the underlying [`Point`] if this is an FPoint variant
    ///
    /// # Returns
    /// The inner [`Point`]
    pub fn into_point(self) -> Result<Point, FalkorDBError> {
        match self {
            Self::FPoint(point) => Ok(point),
            _ => Err(FalkorDBError::ParsingFPoint),
        }
    }
}

// For types implementing Copy
impl FalkorValue {
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
}
