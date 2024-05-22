/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::connection::blocking::BorrowedSyncConnection;
use crate::error::FalkorDBError;
use crate::graph::schema::GraphSchema;
use crate::parser::FalkorParsable;
use crate::value::point::Point;
use anyhow::Result;
use graph_entities::{Edge, Node};
use path::Path;
use std::collections::HashMap;
use std::fmt::Debug;

pub(crate) mod config;
pub(crate) mod constraint;
pub(crate) mod execution_plan;
pub(crate) mod graph_entities;
pub(crate) mod map;
pub(crate) mod path;
pub(crate) mod point;
pub(crate) mod query_result;
pub(crate) mod slowlog_entry;
pub(crate) mod utils;

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
        _graph_schema: &GraphSchema,
        _conn: &mut BorrowedSyncConnection,
    ) -> Result<Self> {
        Ok(value)
    }
}

impl FalkorValue {
    pub fn as_vec(&self) -> Option<&Vec<Self>> {
        match self {
            FalkorValue::FArray(val) => Some(val),
            _ => None,
        }
    }
    pub fn into_vec(self) -> Result<Vec<Self>> {
        match self {
            FalkorValue::FArray(val) => Some(val),
            _ => None,
        }
        .ok_or_else(|| FalkorDBError::ParsingFArray.into())
    }

    pub fn as_string(&self) -> Option<&String> {
        match self {
            FalkorValue::FString(val) => Some(val),
            _ => None,
        }
    }

    pub fn into_string(self) -> Result<String> {
        match self {
            FalkorValue::FString(val) => Some(val),
            _ => None,
        }
        .ok_or(FalkorDBError::ParsingFString.into())
    }

    pub fn into_edge(self) -> Result<Edge> {
        match self {
            Self::FEdge(edge) => Ok(edge),
            _ => Err(FalkorDBError::ParsingFEdge)?,
        }
    }

    pub fn into_node(self) -> Result<Node> {
        match self {
            Self::FNode(node) => Ok(node),
            _ => Err(FalkorDBError::ParsingFNode)?,
        }
    }

    pub fn into_path(self) -> Result<Path> {
        match self {
            Self::FPath(path) => Ok(path),
            _ => Err(FalkorDBError::ParsingFPath)?,
        }
    }

    pub fn into_map(self) -> Result<HashMap<String, FalkorValue>> {
        match self {
            FalkorValue::FMap(map) => Ok(map),
            _ => Err(FalkorDBError::ParsingFMap)?,
        }
    }

    pub fn into_point(self) -> Result<Point> {
        match self {
            Self::FPoint(point) => Ok(point),
            _ => Err(FalkorDBError::ParsingFPoint)?,
        }
    }

    pub fn to_i64(&self) -> Option<i64> {
        match self {
            FalkorValue::Int64(val) => Some(*val),
            _ => None,
        }
    }

    pub fn to_bool(&self) -> Option<bool> {
        match self {
            FalkorValue::FBool(val) => Some(*val),
            _ => None,
        }
    }

    pub fn to_f64(&self) -> Option<f64> {
        match self {
            FalkorValue::F64(val) => Some(*val),
            _ => None,
        }
    }
}
