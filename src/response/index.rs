/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    value::utils::{parse_type, parse_vec, type_val_from_value},
    EntityType, FalkorDBError, FalkorParsable, FalkorValue, GraphSchema,
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
    /// Whether this index is for a Node or on an Edge
    pub entity_type: EntityType,
    /// Whether this index is active or still under construction
    pub status: IndexStatus,
    /// What is this index's label
    pub index_label: String,
    /// What fields to index by
    pub fields: Vec<String>,
    /// Whether each field is a text field, range, etc.
    pub field_types: HashMap<String, Vec<IndexType>>,
    /// Which language is the text used to index in
    pub language: String,
    /// Words to avoid indexing as they are very common and will just be a waste of resources
    pub stopwords: Vec<String>,
    /// Various other information for querying by the user
    pub info: HashMap<String, FalkorValue>,
}

impl FalkorIndex {
    fn from_raw_values(items: Vec<FalkorValue>) -> Result<Self, FalkorDBError> {
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
    ) -> Result<Self, FalkorDBError> {
        let semi_parsed_items = value
            .into_vec()?
            .into_iter()
            .flat_map(|item| {
                let (type_marker, val) = type_val_from_value(item)?;
                parse_type(type_marker, val, graph_schema)
            })
            .collect::<Vec<_>>();

        Self::from_raw_values(semi_parsed_items)
    }
}
