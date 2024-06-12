/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorResult};
use graph_entities::{Edge, Node};
use path::Path;
use point::Point;
use std::{collections::HashMap, fmt::Debug};

pub(crate) mod config;
pub(crate) mod graph_entities;
pub(crate) mod path;
pub(crate) mod point;

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
    /// Failed parsing this value
    Unparseable,
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
            _ => Err(FalkorDBError::ParsingArray),
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
            _ => Err(FalkorDBError::ParsingFMap),
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
    /// Returns a reference to the internal [`Vec`] if this is an Array variant.
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

    /// Consumes itself and returns the inner [`Vec`] if this is an Array variant
    ///
    /// # Returns
    /// The inner [`Vec`]
    pub fn into_vec(self) -> FalkorResult<Vec<Self>> {
        if let FalkorValue::Array(array) = self {
            Ok(array)
        } else {
            Err(FalkorDBError::ParsingFMap)
        }
    }

    /// Consumes itself and returns the inner [`String`] if this is an FString variant
    ///
    /// # Returns
    /// The inner [`String`]
    pub fn into_string(self) -> FalkorResult<String> {
        if let FalkorValue::String(string) = self {
            Ok(string)
        } else {
            Err(FalkorDBError::ParsingFString)
        }
    }
    /// Consumes itself and returns the inner [`HashMap`] if this is a Map variant
    ///
    /// # Returns
    /// The inner [`HashMap`]
    pub fn into_map(self) -> FalkorResult<HashMap<String, FalkorValue>> {
        if let FalkorValue::Map(map) = self {
            Ok(map)
        } else {
            Err(FalkorDBError::ParsingFMap)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::f64::consts::PI;

    #[test]
    fn test_as_vec() {
        let vec_val = FalkorValue::Array(vec![FalkorValue::I64(1), FalkorValue::I64(2)]);
        assert_eq!(vec_val.as_vec().unwrap().len(), 2);

        let non_vec_val = FalkorValue::I64(42);
        assert!(non_vec_val.as_vec().is_none());
    }

    #[test]
    fn test_as_string() {
        let string_val = FalkorValue::String(String::from("hello"));
        assert_eq!(string_val.as_string().unwrap(), "hello");

        let non_string_val = FalkorValue::I64(42);
        assert!(non_string_val.as_string().is_none());
    }

    #[test]
    fn test_as_edge() {
        let edge = Edge::default(); // Assuming Edge::new() is a valid constructor
        let edge_val = FalkorValue::Edge(edge);
        assert!(edge_val.as_edge().is_some());

        let non_edge_val = FalkorValue::I64(42);
        assert!(non_edge_val.as_edge().is_none());
    }

    #[test]
    fn test_as_node() {
        let node = Node::default(); // Assuming Node::new() is a valid constructor
        let node_val = FalkorValue::Node(node);
        assert!(node_val.as_node().is_some());

        let non_node_val = FalkorValue::I64(42);
        assert!(non_node_val.as_node().is_none());
    }

    #[test]
    fn test_as_path() {
        let path = Path::default(); // Assuming Path::new() is a valid constructor
        let path_val = FalkorValue::Path(path);
        assert!(path_val.as_path().is_some());

        let non_path_val = FalkorValue::I64(42);
        assert!(non_path_val.as_path().is_none());
    }

    #[test]
    fn test_as_map() {
        let mut map = HashMap::new();
        map.insert(String::from("key"), FalkorValue::I64(42));
        let map_val = FalkorValue::Map(map);
        assert!(map_val.as_map().is_some());

        let non_map_val = FalkorValue::I64(42);
        assert!(non_map_val.as_map().is_none());
    }

    #[test]
    fn test_as_point() {
        let point = Point::default(); // Assuming Point::new() is a valid constructor
        let point_val = FalkorValue::Point(point);
        assert!(point_val.as_point().is_some());

        let non_point_val = FalkorValue::I64(42);
        assert!(non_point_val.as_point().is_none());
    }

    #[test]
    fn test_to_i64() {
        let int_val = FalkorValue::I64(42);
        assert_eq!(int_val.to_i64().unwrap(), 42);

        let non_int_val = FalkorValue::String(String::from("hello"));
        assert!(non_int_val.to_i64().is_none());
    }

    #[test]
    fn test_to_bool() {
        let bool_val = FalkorValue::Bool(true);
        assert!(bool_val.to_bool().unwrap());

        let bool_str_val = FalkorValue::String(String::from("false"));
        assert!(!bool_str_val.to_bool().unwrap());

        let invalid_bool_str_val = FalkorValue::String(String::from("notabool"));
        assert!(invalid_bool_str_val.to_bool().is_none());

        let non_bool_val = FalkorValue::I64(42);
        assert!(non_bool_val.to_bool().is_none());
    }

    #[test]
    fn test_to_f64() {
        let float_val = FalkorValue::F64(PI);
        assert_eq!(float_val.to_f64().unwrap(), PI);

        let non_float_val = FalkorValue::String(String::from("hello"));
        assert!(non_float_val.to_f64().is_none());
    }

    #[test]
    fn test_into_vec() {
        let vec_val = FalkorValue::Array(vec![FalkorValue::I64(1), FalkorValue::I64(2)]);
        assert_eq!(vec_val.into_vec().unwrap().len(), 2);

        let non_vec_val = FalkorValue::I64(42);
        assert!(non_vec_val.into_vec().is_err());
    }

    #[test]
    fn test_into_string() {
        let string_val = FalkorValue::String(String::from("hello"));
        assert_eq!(string_val.into_string().unwrap(), "hello");

        let non_string_val = FalkorValue::I64(42);
        assert!(non_string_val.into_string().is_err());
    }
}
