/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::parser::{ParserTypeMarker, parse_type};
use crate::{
    EntityType, FalkorDBError, FalkorValue, GraphSchema,
    parser::{
        SchemaParsable, parse_falkor_enum, parse_raw_redis_value, redis_value_as_string,
        redis_value_as_typed_string, redis_value_as_vec, type_val_from_value,
    },
};
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

// parse array of strings, both array and strings represent as redis values
fn parse_string_array(
    value: redis::Value,
    graph_schema: &mut GraphSchema,
) -> Result<Vec<String>, FalkorDBError> {
    type_val_from_value(value).and_then(|(type_marker, val)| {
        if type_marker != ParserTypeMarker::Array {
            return Err(FalkorDBError::ParsingArray);
        }
        let vector = parse_type(ParserTypeMarker::Array, val, graph_schema)?.into_vec()?;
        vector
            .into_iter()
            .map(FalkorValue::into_string)
            .collect::<Result<Vec<String>, FalkorDBError>>()
    })
}

fn parse_types_map(value: redis::Value) -> Result<HashMap<String, Vec<IndexType>>, FalkorDBError> {
    type_val_from_value(value).and_then(|(type_marker, val)| {
        if type_marker != ParserTypeMarker::Map {
            return Err(FalkorDBError::ParsingMap);
        }

        let map_iter = val.into_map_iter().map_err(|_| FalkorDBError::ParsingMap)?;

        let result = map_iter
            .into_iter()
            .map(|(key, val)| {
                let key_str = redis_value_as_string(key)?;
                let (val_type_marker, val) = type_val_from_value(val)?;

                if val_type_marker != ParserTypeMarker::Array {
                    return Err(FalkorDBError::ParsingArray);
                }

                let val_vec = redis_value_as_vec(val)?;
                let parsed_values = val_vec
                    .into_iter()
                    .flat_map(parse_falkor_enum::<IndexType>)
                    .collect::<Vec<_>>();

                Ok((key_str, parsed_values))
            })
            .collect::<Result<HashMap<_, _>, FalkorDBError>>()?;

        Ok(result)
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
    pub info: HashMap<String, FalkorValue>,
    /// Various other options relevant for this index
    pub options: HashMap<String, FalkorValue>,
}

impl SchemaParsable for FalkorIndex {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Index", skip_all, level = "info")
    )]
    fn parse(
        value: redis::Value,
        graph_schema: &mut GraphSchema,
    ) -> Result<Self, FalkorDBError> {
        let [
            label,
            fields,
            field_types,
            options,
            language,
            stopwords,
            entity_type,
            status,
            info,
        ] = redis_value_as_vec(value).and_then(|as_vec| {
            as_vec.try_into().map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 9 elements in index object",
                )
            })
        })?;

        Ok(Self {
            entity_type: parse_falkor_enum(entity_type)?,
            status: parse_falkor_enum(status)?,
            index_label: redis_value_as_typed_string(label)?,
            fields: parse_string_array(fields, graph_schema)?,
            field_types: parse_types_map(field_types)?,
            language: redis_value_as_typed_string(language)?,
            stopwords: parse_string_array(stopwords, graph_schema)?,
            info: parse_raw_redis_value(info, graph_schema).and_then(|val| val.into_map())?,
            options: parse_raw_redis_value(options, graph_schema).and_then(|val| val.into_map())?,
        })
    }
}
