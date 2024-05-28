/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection,
    value::utils::{parse_type, parse_vec, type_val_from_value},
    EntityType, FalkorDBError, FalkorParsable, FalkorValue, GraphSchema,
};
use anyhow::Result;
use std::collections::HashMap;

#[cfg(feature = "tokio")]
use {
    crate::{
        connection::asynchronous::BorrowedAsyncConnection, value::utils::parse_type_async,
        FalkorParsableAsync,
    },
    std::sync::Arc,
    tokio::sync::Mutex,
};

/// The status of this index
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IndexStatus {
    /// This index is active.
    Active,
    /// This index is still being created.
    Pending,
}

impl TryFrom<&str> for IndexStatus {
    type Error = FalkorDBError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value.to_uppercase().as_str() {
            "OPERATIONAL" => Self::Active,
            "UNDER CONSTRUCTION" => Self::Pending,
            _ => Err(FalkorDBError::IndexStatus)?,
        })
    }
}

impl TryFrom<String> for IndexStatus {
    type Error = FalkorDBError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<&String> for IndexStatus {
    type Error = FalkorDBError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

/// The type of this indexed field
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IndexType {
    /// This index is a range
    Range,
    /// This index is raw vector data
    Vector,
    /// This index is a string
    Fulltext,
}

impl TryFrom<&str> for IndexType {
    type Error = FalkorDBError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value.to_uppercase().as_str() {
            "RANGE" => Self::Range,
            "VECTOR" => Self::Vector,
            "FULLTEXT" => Self::Fulltext,
            _ => Err(FalkorDBError::IndexType)?,
        })
    }
}

impl TryFrom<String> for IndexType {
    type Error = FalkorDBError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<&String> for IndexType {
    type Error = FalkorDBError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

fn parse_types_map(value: FalkorValue) -> Result<HashMap<String, Vec<IndexType>>, FalkorDBError> {
    let value: HashMap<String, FalkorValue> = value.try_into()?;

    let mut out_map = HashMap::with_capacity(value.len());
    for (key, val) in value.into_iter() {
        let val = val.into_vec()?;
        let mut field_types = Vec::with_capacity(val.len());
        for field_type in val {
            field_types.push(IndexType::try_from(field_type.into_string()?)?);
        }

        out_map.insert(key, field_types);
    }

    Ok(out_map)
}

/// Contains all the info regarding an index on the database
#[derive(Clone, Debug, PartialEq)]
pub struct FalkorIndex {
    pub entity_type: EntityType,
    pub status: IndexStatus,
    pub index_label: String,
    pub fields: Vec<String>,
    pub field_types: HashMap<String, Vec<IndexType>>,
    pub language: String,
    pub stopwords: Vec<String>,
    pub info: HashMap<String, FalkorValue>,
}

impl FalkorIndex {
    fn from_raw_values(items: Vec<FalkorValue>) -> Result<Self> {
        let [label, fields, field_types, language, stopwords, entity_type, status, info]: [FalkorValue; 8] = items.try_into().map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        Ok(Self {
            entity_type: EntityType::try_from(entity_type.into_string()?)?,
            status: IndexStatus::try_from(status.into_string()?)?,
            index_label: label.try_into()?,
            fields: parse_vec(fields)?,
            field_types: parse_types_map(field_types)?,
            language: language.try_into()?,
            stopwords: parse_vec(stopwords)?,
            info: info.try_into()?,
        })
    }
}

impl FalkorParsable for FalkorIndex {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &mut GraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> Result<Self> {
        let semi_parsed_items = value
            .into_vec()?
            .into_iter()
            .flat_map(|item| {
                let (type_marker, val) = type_val_from_value(item)?;
                parse_type(type_marker, val, graph_schema, conn)
            })
            .collect::<Vec<_>>();

        Self::from_raw_values(semi_parsed_items)
    }
}

#[cfg(feature = "tokio")]
impl FalkorParsableAsync for FalkorIndex {
    async fn from_falkor_value_async(
        value: FalkorValue,
        graph_schema: &mut GraphSchema,
        conn: Arc<Mutex<BorrowedAsyncConnection>>,
    ) -> Result<Self> {
        let semi_parsed_items = value
            .into_vec()?
            .into_iter()
            .flat_map(|item| {
                let (type_marker, val) = type_val_from_value(item)?;
                parse_type_async(type_marker, val, graph_schema, conn).await
            })
            .collect::<Vec<_>>();

        Self::from_raw_values(semi_parsed_items)
    }
}
