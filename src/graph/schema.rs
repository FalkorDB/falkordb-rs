/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
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

    pub fn relationships(&self) -> LockableIdMap {
        self.relationships.clone()
    }

    pub fn labels(&self) -> LockableIdMap {
        self.labels.clone()
    }

    pub fn properties(&self) -> LockableIdMap {
        self.properties.clone()
    }

    pub fn verify_id_set(&self, id_set: &HashSet<i64>, schema_type: SchemaType) -> bool {
        match schema_type {
            SchemaType::Labels => {
                let labels = self.labels.read();
                id_set.iter().all(|id| labels.contains_key(id))
            }
            SchemaType::Properties => id_set.iter().all(|id| {
                let properties = self.properties.read();
                properties.contains_key(id)
            }),
            SchemaType::Relationships => id_set.iter().all(|id| {
                let relationships = self.relationships.read();
                relationships.contains_key(id)
            }),
        }
    }
}
