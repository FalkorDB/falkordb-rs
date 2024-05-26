/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::connection::blocking::BorrowedSyncConnection;
use crate::value::utils::{parse_type, type_val_from_value};
use crate::{
    value::parse_vec, EntityType, FalkorDBError, FalkorParsable, FalkorValue, SyncGraphSchema,
};
use anyhow::Result;
use std::collections::HashMap;

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

    fn try_from(value: &str) -> anyhow::Result<Self, Self::Error> {
        Ok(match value.to_uppercase().as_str() {
            "OPERATIONAL" => Self::Active,
            "UNDER CONSTRUCTION" => Self::Pending,
            _ => Err(FalkorDBError::IndexStatus)?,
        })
    }
}

impl TryFrom<String> for IndexStatus {
    type Error = FalkorDBError;

    fn try_from(value: String) -> anyhow::Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<&String> for IndexStatus {
    type Error = FalkorDBError;

    fn try_from(value: &String) -> anyhow::Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

/// The type of this indexed field
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IndexFieldType {
    /// This index is a number
    Range,
    /// This index is raw vector data
    Vector,
    /// This field is a string
    Fulltext,
}

impl TryFrom<&str> for IndexFieldType {
    type Error = FalkorDBError;

    fn try_from(value: &str) -> anyhow::Result<Self, Self::Error> {
        Ok(match value.to_uppercase().as_str() {
            "RANGE" => Self::Range,
            "VECTOR" => Self::Vector,
            "FULLTEXT" => Self::Fulltext,
            _ => Err(FalkorDBError::IndexFieldType)?,
        })
    }
}

impl TryFrom<String> for IndexFieldType {
    type Error = FalkorDBError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<&String> for IndexFieldType {
    type Error = FalkorDBError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

fn parse_types_map(
    value: FalkorValue
) -> Result<HashMap<String, Vec<IndexFieldType>>, FalkorDBError> {
    let value: HashMap<String, FalkorValue> = value.try_into()?;

    let mut out_map = HashMap::with_capacity(value.len());
    for (key, val) in value.into_iter() {
        let val = val.into_vec()?;
        let mut field_types = Vec::with_capacity(val.len());
        for field_type in val {
            field_types.push(IndexFieldType::try_from(field_type.into_string()?)?);
        }

        out_map.insert(key, field_types);
    }

    Ok(out_map)
}

#[derive(Clone, Debug, PartialEq)]
pub struct FalkorIndex {
    pub entity_type: EntityType,
    pub status: IndexStatus,
    pub index_label: String,
    pub fields: Vec<String>,
    pub field_types: HashMap<String, Vec<IndexFieldType>>,
    pub language: String,
    pub stopwords: Vec<String>,
    pub info: HashMap<String, FalkorValue>,
}

impl FalkorParsable for FalkorIndex {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &SyncGraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> Result<Self> {
        let value_vec = value.into_vec()?;
        let mut semi_parsed_items = Vec::with_capacity(value_vec.len());
        for item in value_vec {
            let (type_marker, val) = type_val_from_value(item)?;
            semi_parsed_items.push(parse_type(type_marker, val, graph_schema, conn)?);
        }

        let [label, fields, field_types, language, stopwords, entity_type, status, info]: [FalkorValue; 8] = semi_parsed_items.try_into().map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

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
