/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::connection::blocking::BorrowedSyncConnection;
use crate::error::FalkorDBError;
use crate::graph::schema::GraphSchema;
use crate::value::map::parse_map;
use crate::value::point::Point;
use anyhow::Result;
use graph_entities::{Edge, Node};
use path::Path;
use std::collections::HashMap;
use std::fmt::Debug;

pub mod config;
pub mod constraint;
pub mod execution_plan;
pub mod graph_entities;
pub mod map;
pub mod path;
pub mod point;
pub mod query_result;
pub mod slowlog_entry;

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

pub(crate) fn type_val_from_value(value: FalkorValue) -> Result<(i64, FalkorValue)> {
    let [type_marker, val]: [FalkorValue; 2] = value
        .into_vec()?
        .try_into()
        .map_err(|_| FalkorDBError::ParsingError)?;
    let type_marker = type_marker.to_i64().ok_or(FalkorDBError::ParsingError)?;

    Ok((type_marker, val))
}

pub(crate) fn parse_type(
    type_marker: i64,
    val: FalkorValue,
    graph_schema: &GraphSchema,
    conn: &mut BorrowedSyncConnection,
) -> Result<FalkorValue> {
    let res = match type_marker {
        1 => FalkorValue::None,
        2 => FalkorValue::FString(val.into_string()?),
        3 => FalkorValue::Int64(val.to_i64().ok_or(FalkorDBError::ParsingError)?),
        4 => FalkorValue::FBool(val.to_bool().ok_or(FalkorDBError::ParsingError)?),
        5 => FalkorValue::F64(val.to_f64().ok_or(FalkorDBError::ParsingError)?),
        6 => FalkorValue::FArray({
            let val = val.into_vec()?;
            let mut parsed_vec = Vec::with_capacity(val.len());
            for item in val {
                let (type_marker, val) = type_val_from_value(item)?;
                parsed_vec.push(parse_type(type_marker, val, graph_schema, conn)?);
            }
            parsed_vec
        }),
        // The following types are sent as an array and require specific parsing functions
        7 => FalkorValue::FEdge(Edge::parse(val, graph_schema, conn)?),
        8 => FalkorValue::FNode(Node::parse(val, graph_schema, conn)?),
        9 => FalkorValue::FPath(Path::parse(val)?),
        10 => FalkorValue::FMap(parse_map(val, graph_schema, conn)?),
        11 => FalkorValue::FPoint(Point::parse(val)?),
        _ => Err(FalkorDBError::ParsingError)?,
    };

    Ok(res)
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
