/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::parser::{parse_falkor_enum, string_vec_from_val};
use crate::{parser::parse_raw_redis_value, EntityType, FalkorDBError, FalkorValue, GraphSchema};
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

fn parse_types_map(
    value: redis::Value,
    graph_schema: &mut GraphSchema,
) -> Result<HashMap<String, Vec<IndexType>>, FalkorDBError> {
    parse_raw_redis_value(value, graph_schema)
        .and_then(|map_val| map_val.into_map())
        .map(|map_val| {
            map_val
                .into_iter()
                .flat_map(|(key, val)| {
                    val.into_vec().map(|as_vec| {
                        (
                            key,
                            as_vec
                                .into_iter()
                                .flat_map(|item| {
                                    item.into_string().and_then(|as_str| {
                                        IndexType::try_from(as_str.as_str()).map_err(Into::into)
                                    })
                                })
                                .collect(),
                        )
                    })
                })
                .collect()
        })
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
    pub(crate) fn parse(
        value: redis::Value,
        graph_schema: &mut GraphSchema,
    ) -> Result<Self, FalkorDBError> {
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

        Ok(Self {
            entity_type: parse_falkor_enum(entity_type, graph_schema)?,
            status: parse_falkor_enum(status, graph_schema)?,
            index_label: parse_raw_redis_value(label, graph_schema)
                .and_then(|parsed_entity_type| parsed_entity_type.into_string())?,
            fields: string_vec_from_val(fields, graph_schema)?,
            field_types: parse_types_map(field_types, graph_schema)?,
            language: parse_raw_redis_value(language, graph_schema)
                .and_then(FalkorValue::into_string)?,
            stopwords: string_vec_from_val(stopwords, graph_schema)?,
            info: parse_raw_redis_value(info, graph_schema)
                .and_then(FalkorValue::into_map)
                .map(|as_map| {
                    as_map
                        .into_iter()
                        .flat_map(|(key, val)| val.into_string().map(|val_str| (key, val_str)))
                        .collect()
                })?,
        })
    }
}
