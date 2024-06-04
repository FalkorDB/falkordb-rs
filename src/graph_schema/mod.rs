/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::blocking::FalkorSyncClientInner, value::utils::parse_type, FalkorDBError, FalkorResult,
    FalkorValue,
};
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) fn get_refresh_command(schema_type: SchemaType) -> &'static str {
    match schema_type {
        SchemaType::Labels => "DB.LABELS",
        SchemaType::Properties => "DB.PROPERTYKEYS",
        SchemaType::Relationships => "DB.RELATIONSHIPTYPES",
    }
}

// Intermediate type for map parsing
pub(crate) struct FKeyTypeVal {
    pub(crate) key: i64,
    pub(crate) type_marker: i64,
    pub(crate) val: FalkorValue,
}

impl TryFrom<FalkorValue> for FKeyTypeVal {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> FalkorResult<Self> {
        let [key_raw, type_raw, val]: [FalkorValue; 3] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        let key = key_raw.to_i64();
        let type_marker = type_raw.to_i64();

        match (key, type_marker) {
            (Some(key), Some(type_marker)) => Ok(FKeyTypeVal {
                key,
                type_marker,
                val,
            }),
            (Some(_), None) => Err(FalkorDBError::ParsingTypeMarkerTypeMismatch)?,
            (None, Some(_)) => Err(FalkorDBError::ParsingKeyIdTypeMismatch)?,
            _ => Err(FalkorDBError::ParsingKTVTypes)?,
        }
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
    client: Arc<FalkorSyncClientInner>,
    graph_name: String,
    version: i64,
    labels: IdMap,
    properties: IdMap,
    relationships: IdMap,
}

impl GraphSchema {
    pub(crate) fn new<T: ToString>(
        graph_name: T,
        client: Arc<FalkorSyncClientInner>,
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
        let [_, keys, _]: [FalkorValue; 3] = self
            .client
            .borrow_connection()?
            .execute_command(
                Some(self.graph_name.as_str()),
                "GRAPH.QUERY",
                None,
                Some(&[format!("CALL {}()", get_refresh_command(schema_type)).as_str()]),
            )?
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        let new_keys = keys
            .into_vec()?
            .into_iter()
            .enumerate()
            .flat_map(|(idx, item)| {
                FalkorResult::<(i64, String)>::Ok((
                    idx as i64,
                    item.into_vec()?
                        .into_iter()
                        .next()
                        .ok_or(FalkorDBError::ParsingError(
                            "Expected new label/property to be the first element in an array"
                                .to_string(),
                        ))
                        .and_then(|item| item.into_string())?,
                ))
            })
            .collect::<HashMap<i64, String>>();

        *id_map = new_keys;
        Ok(())
    }

    pub(crate) fn parse_labels_relationships(
        &mut self,
        raw_ids: Vec<FalkorValue>,
        schema_type: SchemaType,
    ) -> FalkorResult<Vec<String>> {
        let ids_count = raw_ids.len();

        let mut refs_vec = Vec::with_capacity(ids_count);
        let mut success = true;
        for raw_id in &raw_ids {
            let id = raw_id.to_i64().ok_or(FalkorDBError::ParsingI64)?;
            match self.labels.get(&id) {
                None => {
                    success = false;
                    break;
                }
                Some(label) => refs_vec.push(label),
            }
        }

        if success {
            // Clone the strings themselves and return the parsed labels
            return Ok(refs_vec.into_iter().cloned().collect());
        }

        // Refresh and try again
        self.refresh(schema_type)?;

        let mut out_vec = Vec::with_capacity(ids_count);
        for raw_id in raw_ids {
            out_vec.push(
                self.labels
                    .get(&raw_id.to_i64().ok_or(FalkorDBError::ParsingI64)?)
                    .cloned()
                    .ok_or(FalkorDBError::MissingSchemaId(SchemaType::Labels))?,
            );
        }

        Ok(out_vec)
    }

    pub(crate) fn parse_properties_map(
        &mut self,
        value: FalkorValue,
    ) -> FalkorResult<HashMap<String, FalkorValue>> {
        let raw_properties: Vec<_> = value
            .into_vec()?
            .into_iter()
            .flat_map(FKeyTypeVal::try_from)
            .collect();
        let properties_count = raw_properties.len();

        let mut out_vec = Vec::with_capacity(properties_count);
        let mut success = true;
        for fktv in &raw_properties {
            match self.properties().get(&fktv.key).cloned() {
                None => {
                    success = false;
                    break;
                }
                Some(property) => out_vec.push(property),
            }
        }

        if success {
            let mut new_map = HashMap::with_capacity(properties_count);
            for (property, ktv) in out_vec.into_iter().zip(raw_properties) {
                new_map.insert(property, parse_type(ktv.type_marker, ktv.val, self)?);
            }

            return Ok(new_map);
        }

        // Refresh and try again
        self.refresh(SchemaType::Properties)?;

        let mut new_map = HashMap::with_capacity(properties_count);
        for ktv in raw_properties {
            new_map.insert(
                self.properties
                    .get(&ktv.key)
                    .cloned()
                    .ok_or(FalkorDBError::MissingSchemaId(SchemaType::Properties))?,
                parse_type(ktv.type_marker, ktv.val, self)?,
            );
        }

        Ok(new_map)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::{test_utils::create_test_client, SyncGraph};
    use std::collections::HashMap;

    pub(crate) fn open_readonly_graph_with_modified_schema() -> SyncGraph {
        let client = create_test_client();
        let mut graph = client.select_graph("imdb");

        graph.graph_schema.properties = HashMap::from([
            (0, "age".to_string()),
            (1, "is_boring".to_string()),
            (2, "something_else".to_string()),
            (3, "secs_since_login".to_string()),
        ]);

        graph.graph_schema.labels =
            HashMap::from([(0, "much".to_string()), (1, "actor".to_string())]);

        graph.graph_schema.relationships =
            HashMap::from([(0, "very".to_string()), (1, "wow".to_string())]);

        graph
    }
}
