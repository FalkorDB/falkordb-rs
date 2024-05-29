/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection, graph_schema::utils::get_refresh_command,
    FalkorDBError, FalkorResult, FalkorValue,
};
use std::collections::{HashMap, HashSet};
use utils::{get_relevant_hashmap, update_map};

mod utils;

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
#[derive(Clone, Debug, Default)]
pub struct GraphSchema {
    graph_name: String,
    version: i64,
    labels: IdMap,
    properties: IdMap,
    relationships: IdMap,
}

impl GraphSchema {
    pub(crate) fn new<T: ToString>(graph_name: T) -> Self {
        Self {
            graph_name: graph_name.to_string(),
            ..Default::default()
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

    pub(crate) fn verify_id_set(
        &self,
        id_set: &HashSet<i64>,
        schema_type: SchemaType,
    ) -> Option<HashMap<i64, String>> {
        let id_map = match schema_type {
            SchemaType::Labels => &self.labels,
            SchemaType::Properties => &self.properties,
            SchemaType::Relationships => &self.relationships,
        };

        get_relevant_hashmap(id_set, id_map)
    }

    pub(crate) fn refresh(
        &mut self,
        conn: &mut BorrowedSyncConnection,
        schema_type: SchemaType,
        id_hashset: Option<&HashSet<i64>>,
    ) -> FalkorResult<Option<HashMap<i64, String>>> {
        let id_map = match schema_type {
            SchemaType::Labels => &mut self.labels,
            SchemaType::Properties => &mut self.properties,
            SchemaType::Relationships => &mut self.relationships,
        };

        // This is essentially the call_procedure(), but can be done here without access to the graph(which would cause ownership issues)
        let [_, keys, _]: [FalkorValue; 3] = conn
            .send_command(
                Some(self.graph_name.as_str()),
                "GRAPH.QUERY",
                None,
                Some(&[format!("CALL {}()", get_refresh_command(schema_type))]),
            )?
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        update_map(id_map, keys, id_hashset)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{test_utils::create_test_client, SyncGraph};
    use std::collections::HashMap;

    pub(crate) fn open_readonly_graph_with_modified_schema() -> (SyncGraph, BorrowedSyncConnection)
    {
        let client = create_test_client();
        let mut graph = client.select_graph("imdb");
        let conn = client
            .borrow_connection()
            .expect("Could not borrow_connection");

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

        (graph, conn)
    }
}
