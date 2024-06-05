/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorParsable, FalkorResult, GraphSchema};
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

/// An enum of all the supported Falkor types
#[derive(Clone, Debug, PartialEq)]
pub enum FalkorValue {
    /// See [`Node`]
    Node(Node),
    /// See [`Edge`]
    Edge(Edge),
    /// A [`Vec`] of other [`FalkorValue`]
    Array(Vec<FalkorValue>),
    /// A [`HashMap`] of [`String`] as keys, and other [`FalkorValue`] as values
    Map(HashMap<String, FalkorValue>),
    /// Plain old string
    String(String),
    /// A boolean value
    Bool(bool),
    /// An [`i64`] value, Falkor only supports signed integers
    I64(i64),
    /// An [`f64`] value, Falkor only supports double precisions when not in Vectors
    F64(f64),
    /// See [`Point`]
    Point(Point),
    /// See [`Path`]
    Path(Path),
    /// A NULL type
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

impl_to_falkordb_value!(i8, Self::I64);
impl_to_falkordb_value!(i32, Self::I64);
impl_to_falkordb_value!(i64, Self::I64);

impl_to_falkordb_value!(u8, Self::I64);
impl_to_falkordb_value!(u32, Self::I64);
impl_to_falkordb_value!(u64, Self::I64);

impl_to_falkordb_value!(f32, Self::F64);
impl_to_falkordb_value!(f64, Self::F64);

impl_to_falkordb_value!(String, Self::String);

impl From<&str> for FalkorValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl<T> From<Vec<T>> for FalkorValue
where
    FalkorValue: From<T>,
{
    fn from(value: Vec<T>) -> Self {
        Self::Array(
            value
                .into_iter()
                .map(|element| FalkorValue::from(element))
                .collect(),
        )
    }
}

impl TryFrom<FalkorValue> for Vec<FalkorValue> {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::Array(val) => Ok(val),
            _ => Err(FalkorDBError::ParsingFArray),
        }
    }
}

impl TryFrom<FalkorValue> for f64 {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::String(f64_str) => f64_str.parse().map_err(|_| FalkorDBError::ParsingF64),
            FalkorValue::F64(f64_val) => Ok(f64_val),
            _ => Err(FalkorDBError::ParsingF64),
        }
    }
}

impl TryFrom<FalkorValue> for String {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::String(val) => Ok(val),
            _ => Err(FalkorDBError::ParsingFString),
        }
    }
}

impl TryFrom<FalkorValue> for Edge {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::Edge(edge) => Ok(edge),
            _ => Err(FalkorDBError::ParsingFEdge),
        }
    }
}

impl TryFrom<FalkorValue> for Node {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::Node(node) => Ok(node),
            _ => Err(FalkorDBError::ParsingFNode),
        }
    }
}

impl TryFrom<FalkorValue> for Path {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::Path(path) => Ok(path),
            _ => Err(FalkorDBError::ParsingFPath),
        }
    }
}

impl TryFrom<FalkorValue> for HashMap<String, FalkorValue> {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::Map(map) => Ok(map),
            _ => Err(FalkorDBError::ParsingFMap(
                "Attempting to get a non-map element as a map".to_string(),
            )),
        }
    }
}

impl TryFrom<FalkorValue> for Point {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::Point(point) => Ok(point),
            _ => Err(FalkorDBError::ParsingFPoint),
        }
    }
}

impl FalkorValue {
    /// Returns a reference to the internal [`Vec`] if this is an FArray variant.
    ///
    /// # Returns
    /// A reference to the internal [`Vec`]
    pub fn as_vec(&self) -> Option<&Vec<Self>> {
        match self {
            FalkorValue::Array(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a reference to the internal [`String`] if this is an FString variant.
    ///
    /// # Returns
    /// A reference to the internal [`String`]
    pub fn as_string(&self) -> Option<&String> {
        match self {
            FalkorValue::String(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a reference to the internal [`Edge`] if this is an FEdge variant.
    ///
    /// # Returns
    /// A reference to the internal [`Edge`]
    pub fn as_edge(&self) -> Option<&Edge> {
        match self {
            FalkorValue::Edge(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a reference to the internal [`Node`] if this is an FNode variant.
    ///
    /// # Returns
    /// A reference to the internal [`Node`]
    pub fn as_node(&self) -> Option<&Node> {
        match self {
            FalkorValue::Node(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a reference to the internal [`Path`] if this is an FPath variant.
    ///
    /// # Returns
    /// A reference to the internal [`Path`]
    pub fn as_path(&self) -> Option<&Path> {
        match self {
            FalkorValue::Path(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a reference to the internal [`HashMap`] if this is an FMap variant.
    ///
    /// # Returns
    /// A reference to the internal [`HashMap`]
    pub fn as_map(&self) -> Option<&HashMap<String, FalkorValue>> {
        match self {
            FalkorValue::Map(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a reference to the internal [`Point`] if this is an FPoint variant.
    ///
    /// # Returns
    /// A reference to the internal [`Point`]
    pub fn as_point(&self) -> Option<&Point> {
        match self {
            FalkorValue::Point(val) => Some(val),
            _ => None,
        }
    }

    /// Returns a Copy of the inner [`i64`] if this is an Int64 variant
    ///
    /// # Returns
    /// A copy of the inner [`i64`]
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            FalkorValue::I64(val) => Some(*val),
            _ => None,
        }
    }

    /// Returns a Copy of the inner [`bool`] if this is an FBool variant
    ///
    /// # Returns
    /// A copy of the inner [`bool`]
    pub fn to_bool(&self) -> Option<bool> {
        match self {
            FalkorValue::Bool(val) => Some(*val),
            FalkorValue::String(bool_str) => match bool_str.as_str() {
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
    pub fn into_vec(self) -> FalkorResult<Vec<Self>> {
        self.try_into()
    }

    /// Consumes itself and returns the inner [`String`] if this is an FString variant
    ///
    /// # Returns
    /// The inner [`String`]
    pub fn into_string(self) -> FalkorResult<String> {
        self.try_into()
    }
}

impl FalkorParsable for FalkorValue {
    fn from_falkor_value(
        value: FalkorValue,
        _: &mut GraphSchema,
    ) -> FalkorResult<Self> {
        Ok(value)
    }
}
