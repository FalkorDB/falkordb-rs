/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use super::utils::{get_refresh_command, get_relevant_hashmap, update_map};
use crate::{
    connection::blocking::BorrowedSyncConnection, value::FalkorValue, FalkorDBError, SchemaType,
};
use anyhow::Result;
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicI64, Ordering::SeqCst},
        Arc,
    },
};

pub(crate) type LockableIdMap = Arc<RwLock<HashMap<i64, String>>>;

/// A struct containing the various schema maps, allowing conversions between ids and their string representations.
///
/// # Thread Safety
/// This struct is fully thread safe, it can be cloned and passed within threads without constraints,
/// Its API uses only immutable references
#[derive(Clone, Debug, Default)]
pub struct SyncGraphSchema {
    graph_name: String,
    version: Arc<AtomicI64>,
    labels: LockableIdMap,
    properties: LockableIdMap,
    relationships: LockableIdMap,
}

impl SyncGraphSchema {
    pub(crate) fn new(graph_name: String) -> Self {
        Self {
            graph_name,
            ..Default::default()
        }
    }

    /// Clears all cached schemas, this will cause a refresh when next attempting to parse a compact query.
    pub fn clear(&self) {
        self.version.store(0, SeqCst);
        self.labels.write().clear();
        self.properties.write().clear();
        self.relationships.write().clear();
    }

    /// Returns a read-write-locked map, of the relationship ids to their respective string representations.
    /// Minimize locking these to avoid starvation.
    pub fn relationships(&self) -> LockableIdMap {
        self.relationships.clone()
    }

    /// Returns a read-write-locked map, of the label ids to their respective string representations.
    /// Minimize locking these to avoid starvation.
    pub fn labels(&self) -> LockableIdMap {
        self.labels.clone()
    }

    /// Returns a read-write-locked map, of the property ids to their respective string representations.
    /// Minimize locking these to avoid starvation.
    pub fn properties(&self) -> LockableIdMap {
        self.properties.clone()
    }

    pub(crate) fn verify_id_set(
        &self,
        id_set: &HashSet<i64>,
        schema_type: SchemaType,
    ) -> Option<HashMap<i64, String>> {
        let read_lock = match schema_type {
            SchemaType::Labels => &self.labels,
            SchemaType::Properties => &self.properties,
            SchemaType::Relationships => &self.relationships,
        }
        .read();

        get_relevant_hashmap(id_set, read_lock.deref())
    }

    pub(crate) fn refresh(
        &self,
        schema_type: SchemaType,
        conn: &mut BorrowedSyncConnection,
        id_hashset: Option<&HashSet<i64>>,
    ) -> Result<Option<HashMap<i64, String>>> {
        let command = get_refresh_command(schema_type);
        let map = match schema_type {
            SchemaType::Labels => &self.labels,
            SchemaType::Properties => &self.properties,
            SchemaType::Relationships => &self.relationships,
        };

        let mut write_lock = map.write();

        // This is essentially the call_procedure(), but can be done here without access to the graph(which would cause ownership issues)
        let [_, keys, _]: [FalkorValue; 3] = conn
            .send_command(
                Some(self.graph_name.clone()),
                "GRAPH.QUERY",
                Some(format!("CALL {command}()")),
            )?
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        update_map(write_lock.deref_mut(), keys, id_hashset)
    }
}
