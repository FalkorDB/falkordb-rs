/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::ProvidesSyncConnections,
    parser::{parse_type, redis_value_as_int, redis_value_as_string, redis_value_as_vec},
    FalkorDBError, FalkorResult, FalkorValue,
};
use std::{collections::HashMap, sync::Arc};

pub(crate) fn get_refresh_command(schema_type: SchemaType) -> &'static str {
    match schema_type {
        SchemaType::Labels => "DB.LABELS",
        SchemaType::Properties => "DB.PROPERTYKEYS",
        SchemaType::Relationships => "DB.RELATIONSHIPTYPES",
    }
}

// Intermediate type for map parsing
#[derive(Debug)]
pub(crate) struct FKeyTypeVal {
    pub(crate) key: i64,
    pub(crate) type_marker: i64,
    pub(crate) val: redis::Value,
}

impl TryFrom<redis::Value> for FKeyTypeVal {
    type Error = FalkorDBError;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "New KeyTypeValue", skip_all, level = "trace")
    )]
    fn try_from(value: redis::Value) -> FalkorResult<Self> {
        let [key_raw, type_raw, val]: [redis::Value; 3] =
            redis_value_as_vec(value).and_then(|seq| {
                seq.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "Expected exactly 3 elements for key-type-value property",
                    )
                })
            })?;

        redis_value_as_int(key_raw).and_then(|key| {
            redis_value_as_int(type_raw).map(|type_marker| FKeyTypeVal {
                key,
                type_marker,
                val,
            })
        })
    }
}

/// An enum specifying which schema type we are addressing
/// When querying using the compact parser, ids are returned for the various schema entities instead of strings
/// Using this enum we know which of the schema maps to access in order to convert these ids to strings
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SchemaType {
    /// The schema for [`Node`](crate::Node) labels
    Labels,
    /// The schema for [`Node`](crate::Node) or [`Edge`](crate::Edge) properties (attribute keys)
    Properties,
    /// The schema for [`Edge`](crate::Edge) labels
    Relationships,
}

pub(crate) type IdMap = HashMap<i64, String>;

/// A struct containing the various schema maps, allowing conversions between ids and their string representations.
#[derive(Clone)]
pub struct GraphSchema {
    client: Arc<dyn ProvidesSyncConnections>,
    graph_name: String,
    version: i64,
    labels: IdMap,
    properties: IdMap,
    relationships: IdMap,
}

impl GraphSchema {
    pub(crate) fn new<T: ToString>(
        graph_name: T,
        client: Arc<dyn ProvidesSyncConnections>,
    ) -> Self {
        Self {
            client,
            graph_name: graph_name.to_string(),
            version: 0,
            labels: IdMap::new(),
            properties: IdMap::new(),
            relationships: IdMap::new(),
        }
    }

    /// Clears all cached schemas, this will cause a refresh when next attempting to parse a compact query.
    pub fn clear(&mut self) {
        self.version = 0;
        self.labels.clear();
        self.properties.clear();
        self.relationships.clear();
    }

    /// Returns a read-write-locked map, of the relationship ids to their respective string representations.
    /// Minimize locking these to avoid starvation.
    pub fn relationships(&self) -> &IdMap {
        &self.relationships
    }

    /// Returns a read-write-locked map, of the label ids to their respective string representations.
    /// Minimize locking these to avoid starvation.
    pub fn labels(&self) -> &IdMap {
        &self.labels
    }

    /// Returns a read-write-locked map, of the property ids to their respective string representations.
    /// Minimize locking these to avoid starvation.
    pub fn properties(&self) -> &IdMap {
        &self.properties
    }

    #[inline]
    fn get_id_map_by_schema_type(
        &self,
        schema_type: SchemaType,
    ) -> &IdMap {
        match schema_type {
            SchemaType::Labels => &self.labels,
            SchemaType::Properties => &self.properties,
            SchemaType::Relationships => &self.relationships,
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Refresh Schema Type", skip_all, level = "info")
    )]
    fn refresh(
        &mut self,
        schema_type: SchemaType,
    ) -> FalkorResult<()> {
        let id_map = match schema_type {
            SchemaType::Labels => &mut self.labels,
            SchemaType::Properties => &mut self.properties,
            SchemaType::Relationships => &mut self.relationships,
        };

        // This is essentially the call_procedure(), but can be done here without access to the graph(which would cause ownership issues)
        let keys = self
            .client
            .get_connection()
            .and_then(|mut conn| {
                conn.execute_command(
                    Some(self.graph_name.as_str()),
                    "GRAPH.QUERY",
                    None,
                    Some(&[format!("CALL {}()", get_refresh_command(schema_type)).as_str()]),
                )
            })
            .and_then(|res| {
                redis_value_as_vec(res).and_then(|as_vec| {
                    as_vec.into_iter().nth(1).ok_or(FalkorDBError::ParsingArrayToStructElementCount(
            "Expected exactly 3 types for header-resultset-stats response from refresh query"
        ))
                })
            })
            .and_then(redis_value_as_vec)?;

        let new_keys = keys
            .into_iter()
            .enumerate()
            .flat_map(|(idx, item)| {
                FalkorResult::<(i64, String)>::Ok((
                    idx as i64,
                    redis_value_as_vec(item)
                        .and_then(|item_seq| {
                            item_seq.into_iter().next().ok_or_else(|| {
                                FalkorDBError::ParsingError(
                            "Expected new label/property to be the first element in an array"
                                .to_string(),
                        )
                            })
                        })
                        .and_then(redis_value_as_string)?,
                ))
            })
            .collect::<HashMap<i64, String>>();

        *id_map = new_keys;
        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse ID Vec To String Vec", skip_all, level = "debug")
    )]
    pub(crate) fn parse_id_vec(
        &mut self,
        raw_ids: Vec<redis::Value>,
        schema_type: SchemaType,
    ) -> FalkorResult<Vec<String>> {
        let raw_ids_len = raw_ids.len();
        raw_ids
            .into_iter()
            .try_fold(Vec::with_capacity(raw_ids_len), |mut acc, raw_id| {
                let id = redis_value_as_int(raw_id)?;
                let value = match self
                    .get_id_map_by_schema_type(schema_type)
                    .get(&id)
                    .cloned()
                {
                    None => {
                        self.refresh(schema_type)?;
                        self.get_id_map_by_schema_type(schema_type)
                            .get(&id)
                            .cloned()
                            .ok_or(FalkorDBError::MissingSchemaId(schema_type))?
                    }
                    Some(exists) => exists,
                };
                acc.push(value);
                Ok(acc)
            })
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Properties Map", skip_all, level = "debug")
    )]
    pub(crate) fn parse_properties_map(
        &mut self,
        value: redis::Value,
    ) -> FalkorResult<HashMap<String, FalkorValue>> {
        let raw_properties_vec = redis_value_as_vec(value)?;

        let raw_properties_len = raw_properties_vec.len();
        raw_properties_vec.into_iter().try_fold(
            HashMap::with_capacity(raw_properties_len),
            |mut out_map, item| {
                let ktv = FKeyTypeVal::try_from(item)?;
                let key = if let Some(key) = self.properties.get(&ktv.key).cloned() {
                    key
                } else {
                    // Refresh the schema and attempt to retrieve the key again
                    self.refresh(SchemaType::Properties)?;
                    self.properties
                        .get(&ktv.key)
                        .cloned()
                        .ok_or(FalkorDBError::MissingSchemaId(SchemaType::Properties))?
                };

                out_map.insert(key, parse_type(ktv.type_marker, ktv.val, self)?);
                Ok(out_map)
            },
        )
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        client::blocking::create_empty_inner_sync_client, graph::HasGraphSchema,
        test_utils::create_test_client, SyncGraph,
    };
    use std::collections::HashMap;

    pub(crate) fn open_readonly_graph_with_modified_schema() -> SyncGraph {
        let client = create_test_client();
        let mut graph = client.select_graph("imdb");

        {
            let schema = graph.get_graph_schema_mut();
            schema.properties = HashMap::from([
                (0, "age".to_string()),
                (1, "is_boring".to_string()),
                (2, "something_else".to_string()),
                (3, "secs_since_login".to_string()),
            ]);

            schema.labels = HashMap::from([(0, "much".to_string()), (1, "actor".to_string())]);

            schema.relationships = HashMap::from([(0, "very".to_string()), (1, "wow".to_string())]);
        }

        graph
    }

    #[test]
    fn test_label_not_exists() {
        let mut parser =
            GraphSchema::new("graph_name".to_string(), create_empty_inner_sync_client());
        let input_value = redis::Value::Bulk(vec![redis::Value::Bulk(vec![
            redis::Value::Int(1),
            redis::Value::Int(2),
            redis::Value::Status("test".to_string()),
        ])]);

        let result = parser.parse_properties_map(input_value);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_properties_map() {
        let mut parser =
            GraphSchema::new("graph_name".to_string(), create_empty_inner_sync_client());
        parser.properties = HashMap::from([
            (1, "property1".to_string()),
            (2, "property2".to_string()),
            (3, "property3".to_string()),
        ]);

        // Create a FalkorValue to test
        let input_value = redis::Value::Bulk(vec![
            redis::Value::Bulk(vec![
                redis::Value::Int(1),
                redis::Value::Int(2),
                redis::Value::Status("test".to_string()),
            ]),
            redis::Value::Bulk(vec![
                redis::Value::Int(2),
                redis::Value::Int(3),
                redis::Value::Int(42),
            ]),
            redis::Value::Bulk(vec![
                redis::Value::Int(3),
                redis::Value::Int(4),
                redis::Value::Status("true".to_string()),
            ]),
        ]);

        let result = parser.parse_properties_map(input_value);

        let expected_map = HashMap::from([
            (
                "property1".to_string(),
                FalkorValue::String("test".to_string()),
            ),
            ("property2".to_string(), FalkorValue::I64(42)),
            ("property3".to_string(), FalkorValue::Bool(true)),
        ]);
        assert_eq!(result.unwrap(), expected_map);
    }

    #[test]
    fn test_parse_id_vec() {
        let mut parser =
            GraphSchema::new("graph_name".to_string(), create_empty_inner_sync_client());

        parser.labels = HashMap::from([
            (1, "property1".to_string()),
            (2, "property2".to_string()),
            (3, "property3".to_string()),
        ]);

        let labels_ok_res = parser.parse_id_vec(
            vec![
                redis::Value::Int(3),
                redis::Value::Int(1),
                redis::Value::Int(2),
            ],
            SchemaType::Labels,
        );
        assert!(labels_ok_res.is_ok());
        assert_eq!(
            labels_ok_res.unwrap(),
            vec!["property3", "property1", "property2"]
        );

        // Should fail, these are not relationships
        let labels_not_ok_res = parser.parse_id_vec(
            vec![
                redis::Value::Int(3),
                redis::Value::Int(1),
                redis::Value::Int(2),
            ],
            SchemaType::Relationships,
        );
        assert!(labels_not_ok_res.is_err());

        parser.clear();

        parser.relationships = HashMap::from([
            (1, "property4".to_string()),
            (2, "property5".to_string()),
            (3, "property6".to_string()),
        ]);

        let rels_ok_res = parser.parse_id_vec(
            vec![
                redis::Value::Int(3),
                redis::Value::Int(1),
                redis::Value::Int(2),
            ],
            SchemaType::Relationships,
        );
        assert!(rels_ok_res.is_ok());
        assert_eq!(
            rels_ok_res.unwrap(),
            vec![
                "property6".to_string(),
                "property4".to_string(),
                "property5".to_string()
            ]
        )
    }
}
