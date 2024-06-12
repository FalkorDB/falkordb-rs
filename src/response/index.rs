/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::redis_ext::redis_value_as_string;
use crate::{EntityType, FalkorDBError};
use std::collections::HashMap;

/// The status of this index
#[derive(Copy, Clone, Debug, Eq, PartialEq, strum::EnumString, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum IndexStatus {
    /// This index is active.
    #[strum(serialize = "OPERATIONAL")]
    Active,
    /// This index is still being created.
    #[strum(serialize = "UNDER CONSTRUCTION")]
    Pending,
}

/// The type of this indexed field
#[derive(Copy, Clone, Debug, Eq, PartialEq, strum::EnumString, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum IndexType {
    /// This index is a range
    Range,
    /// This index is raw vector data
    Vector,
    /// This index is a string
    Fulltext,
}

fn parse_types_map(value: redis::Value) -> Result<HashMap<String, Vec<IndexType>>, FalkorDBError> {
    Ok(value
        .into_map_iter()
        .map_err(|_| FalkorDBError::ParsingFMap)?
        .into_iter()
        .filter_map(|(key, val)| {
            let key_str = redis_value_as_string(key).ok()?;
            let val_seq = val.into_sequence().ok()?;

            let index_types = val_seq
                .into_iter()
                .filter_map(|index_type| {
                    let index_str = redis_value_as_string(index_type).ok()?;
                    IndexType::try_from(index_str.as_str()).ok()
                })
                .collect::<Vec<_>>();

            Some((key_str, index_types))
        })
        .collect())
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
    pub info: HashMap<String, String>,
}

impl FalkorIndex {
    pub(crate) fn parse(value: redis::Value) -> Result<Self, FalkorDBError> {
        let [label, fields, field_types, language, stopwords, entity_type, status, info] = value
            .into_sequence()
            .map_err(|_| FalkorDBError::ParsingArray)
            .and_then(|as_vec| {
                as_vec.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "Expected exactly 8 elements in index object",
                    )
                })
            })?;

        eprintln!("Got here: {label:?} \n{fields:?} \n{field_types:?} \n{language:?} \n{stopwords:?} \n{entity_type:?} \n{status:?} \n{info:?}");

        Ok(Self {
            entity_type: EntityType::try_from(redis_value_as_string(entity_type)?.as_str())?,
            status: IndexStatus::try_from(redis_value_as_string(status)?.as_str())?,
            index_label: redis_value_as_string(label)?,
            fields: fields
                .into_sequence()
                .map(|fields| fields.into_iter().flat_map(redis_value_as_string).collect())
                .map_err(|_| FalkorDBError::ParsingArray)?,
            field_types: parse_types_map(field_types)?,
            language: redis_value_as_string(language)?,
            stopwords: stopwords
                .into_sequence()
                .map(|stopwords| {
                    stopwords
                        .into_iter()
                        .flat_map(redis_value_as_string)
                        .collect()
                })
                .map_err(|_| FalkorDBError::ParsingArray)?,
            info: info
                .into_map_iter()
                .map(|map_iter| {
                    map_iter
                        .into_iter()
                        .flat_map(|(key, val)| {
                            redis_value_as_string(key)
                                .and_then(|key| redis_value_as_string(val).map(|val| (key, val)))
                        })
                        .collect()
                })
                .map_err(|_| FalkorDBError::ParsingFMap)?,
        })
    }
}
