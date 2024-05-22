/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::connection::blocking::BorrowedSyncConnection;
use crate::value::FalkorValue;
use crate::FalkorDBError;
use anyhow::Result;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SchemaType {
    Labels,
    Properties,
    Relationships,
}

pub(crate) type LockableIdMap = Arc<RwLock<HashMap<i64, String>>>;

#[derive(Clone, Debug, Default)]
pub struct GraphSchema {
    graph_name: String,
    version: Arc<AtomicI64>,
    labels: LockableIdMap,
    properties: LockableIdMap,
    relationships: LockableIdMap,
}

impl GraphSchema {
    pub fn new(graph_name: String) -> Self {
        Self {
            graph_name,
            ..Default::default()
        }
    }

    pub fn clear(&mut self) {
        self.version.store(0, SeqCst);
        self.labels.write().clear();
        self.properties.write().clear();
        self.relationships.write().clear();
    }

    pub fn graph_name(&self) -> String {
        self.graph_name.clone()
    }

    pub(crate) fn relationships(&self) -> LockableIdMap {
        self.relationships.clone()
    }

    pub(crate) fn labels(&self) -> LockableIdMap {
        self.labels.clone()
    }

    pub(crate) fn properties(&self) -> LockableIdMap {
        self.properties.clone()
    }

    pub fn verify_id_set(
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

        // Returns the write lock if
        let mut id_hashmap = HashMap::new();
        for id in id_set {
            if let Some(id_val) = read_lock.get(id).cloned() {
                id_hashmap.insert(*id, id_val);
                continue;
            }

            return None;
        }

        Some(id_hashmap)
    }

    pub(crate) fn refresh(
        &self,
        schema_type: SchemaType,
        conn: &mut BorrowedSyncConnection,
        id_hashset: Option<&HashSet<i64>>,
    ) -> Result<Option<HashMap<i64, String>>> {
        let (map, command) = match schema_type {
            SchemaType::Labels => (&self.labels, "DB.LABELS"),
            SchemaType::Properties => (&self.properties, "DB.PROPERTYKEYS"),
            SchemaType::Relationships => (&self.relationships, "DB.RELATIONSHIPTYPES"),
        };

        let mut write_lock = map.write();

        let [_, keys, _]: [FalkorValue; 3] = conn
            .send_command(
                Some(self.graph_name.clone()),
                "GRAPH.QUERY",
                Some(format!("CALL {command}()")),
            )?
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingError)?;
        let keys_vec = keys.into_vec()?;

        let mut new_keys = HashMap::with_capacity(keys_vec.len());
        for (idx, item) in keys_vec.into_iter().enumerate() {
            let key = item
                .into_vec()?
                .into_iter()
                .next()
                .ok_or(FalkorDBError::ParsingError)?
                .into_string()?;
            new_keys.insert(idx as i64, key);
        }

        *write_lock.deref_mut() = new_keys;

        match id_hashset {
            None => Ok(None),
            Some(id_hashset) => {
                let mut relevant_ids = HashMap::with_capacity(id_hashset.len());
                for id in id_hashset {
                    relevant_ids.insert(
                        *id,
                        write_lock
                            .get(id)
                            .cloned()
                            .ok_or(FalkorDBError::ParsingError)?,
                    );
                }

                Ok(Some(relevant_ids))
            }
        }
    }
}
