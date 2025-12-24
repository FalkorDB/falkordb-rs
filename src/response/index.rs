/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::parser::{parse_type, ParserTypeMarker};
use crate::{
    parser::{
        parse_falkor_enum, parse_raw_redis_value, redis_value_as_string,
        redis_value_as_typed_string, redis_value_as_vec, type_val_from_value, SchemaParsable,
    },
    EntityType, FalkorDBError, FalkorValue, GraphSchema,
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
        let [label, fields, field_types, options, language, stopwords, entity_type, status, info] =
            redis_value_as_vec(value).and_then(|as_vec| {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_status_active() {
        assert_eq!(IndexStatus::Active.to_string(), "OPERATIONAL");
    }

    #[test]
    fn test_index_status_pending() {
        assert_eq!(IndexStatus::Pending.to_string(), "UNDER CONSTRUCTION");
    }

    #[test]
    fn test_index_status_from_string() {
        use std::str::FromStr;
        assert_eq!(
            IndexStatus::from_str("OPERATIONAL").unwrap(),
            IndexStatus::Active
        );
        assert_eq!(
            IndexStatus::from_str("UNDER CONSTRUCTION").unwrap(),
            IndexStatus::Pending
        );
    }

    #[test]
    fn test_index_status_clone() {
        let status = IndexStatus::Active;
        let status_clone = status.clone();
        assert_eq!(status, status_clone);
    }

    #[test]
    fn test_index_status_debug() {
        assert!(format!("{:?}", IndexStatus::Active).contains("Active"));
        assert!(format!("{:?}", IndexStatus::Pending).contains("Pending"));
    }

    #[test]
    fn test_index_type_range() {
        assert_eq!(IndexType::Range.to_string(), "RANGE");
    }

    #[test]
    fn test_index_type_vector() {
        assert_eq!(IndexType::Vector.to_string(), "VECTOR");
    }

    #[test]
    fn test_index_type_fulltext() {
        assert_eq!(IndexType::Fulltext.to_string(), "FULLTEXT");
    }

    #[test]
    fn test_index_type_from_string() {
        use std::str::FromStr;
        assert_eq!(IndexType::from_str("RANGE").unwrap(), IndexType::Range);
        assert_eq!(IndexType::from_str("VECTOR").unwrap(), IndexType::Vector);
        assert_eq!(
            IndexType::from_str("FULLTEXT").unwrap(),
            IndexType::Fulltext
        );
    }

    #[test]
    fn test_index_type_clone() {
        let it = IndexType::Range;
        let it_clone = it.clone();
        assert_eq!(it, it_clone);
    }

    #[test]
    fn test_index_type_debug() {
        assert!(format!("{:?}", IndexType::Range).contains("Range"));
        assert!(format!("{:?}", IndexType::Vector).contains("Vector"));
        assert!(format!("{:?}", IndexType::Fulltext).contains("Fulltext"));
    }

    #[test]
    fn test_falkor_index_clone() {
        let mut field_types = HashMap::new();
        field_types.insert("field1".to_string(), vec![IndexType::Range]);

        let index = FalkorIndex {
            entity_type: EntityType::Node,
            status: IndexStatus::Active,
            index_label: "Person".to_string(),
            fields: vec!["name".to_string()],
            field_types: field_types.clone(),
            language: "english".to_string(),
            stopwords: vec!["the".to_string()],
            info: HashMap::new(),
            options: HashMap::new(),
        };

        let index_clone = index.clone();
        assert_eq!(index, index_clone);
    }

    #[test]
    fn test_falkor_index_debug() {
        let index = FalkorIndex {
            entity_type: EntityType::Node,
            status: IndexStatus::Pending,
            index_label: "User".to_string(),
            fields: vec!["email".to_string()],
            field_types: HashMap::new(),
            language: "english".to_string(),
            stopwords: vec![],
            info: HashMap::new(),
            options: HashMap::new(),
        };

        let debug_str = format!("{:?}", index);
        assert!(debug_str.contains("User"));
        assert!(debug_str.contains("email"));
        assert!(debug_str.contains("english"));
    }

    #[test]
    fn test_falkor_index_fields() {
        let mut field_types = HashMap::new();
        field_types.insert("field1".to_string(), vec![IndexType::Fulltext]);

        let mut info = HashMap::new();
        info.insert(
            "key1".to_string(),
            FalkorValue::String("value1".to_string()),
        );

        let mut options = HashMap::new();
        options.insert("opt1".to_string(), FalkorValue::I64(100));

        let index = FalkorIndex {
            entity_type: EntityType::Edge,
            status: IndexStatus::Active,
            index_label: "KNOWS".to_string(),
            fields: vec!["description".to_string()],
            field_types,
            language: "french".to_string(),
            stopwords: vec!["le".to_string(), "la".to_string()],
            info,
            options,
        };

        assert_eq!(index.entity_type, EntityType::Edge);
        assert_eq!(index.status, IndexStatus::Active);
        assert_eq!(index.index_label, "KNOWS");
        assert_eq!(index.fields.len(), 1);
        assert_eq!(index.language, "french");
        assert_eq!(index.stopwords.len(), 2);
        assert_eq!(index.info.len(), 1);
        assert_eq!(index.options.len(), 1);
    }
}
