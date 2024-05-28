/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use super::utils::{get_refresh_command, get_relevant_hashmap, update_map};
use crate::{
    connection::asynchronous::BorrowedAsyncConnection, value::FalkorValue, FalkorDBError,
    SchemaType,
};
use anyhow::Result;
use std::{
    collections::{HashMap, HashSet},
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicI64, Ordering::SeqCst},
        Arc,
    },
};
use tokio::sync::RwLock;

pub(crate) type LockableIdMap = Arc<RwLock<HashMap<i64, String>>>;

#[derive(Clone, Debug, Default)]
pub struct AsyncGraphSchema {
    graph_name: String,
    version: Arc<AtomicI64>,
    labels: LockableIdMap,
    properties: LockableIdMap,
    relationships: LockableIdMap,
}

impl AsyncGraphSchema {
    pub fn new(graph_name: String) -> Self {
        Self {
            graph_name,
            ..Default::default()
        }
    }

    pub async fn clear(&mut self) {
        self.version.store(0, SeqCst);
        self.labels.write().await.clear();
        self.properties.write().await.clear();
        self.relationships.write().await.clear();
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

    pub(crate) async fn verify_id_set(
        &self,
        id_set: &HashSet<i64>,
        schema_type: SchemaType,
    ) -> Option<HashMap<i64, String>> {
        let read_lock = match schema_type {
            SchemaType::Labels => &self.labels,
            SchemaType::Properties => &self.properties,
            SchemaType::Relationships => &self.relationships,
        }
        .read()
        .await;

        get_relevant_hashmap(id_set, read_lock.deref())
    }

    pub(crate) async fn refresh(
        &self,
        schema_type: SchemaType,
        conn: &mut BorrowedAsyncConnection,
        id_hashset: Option<&HashSet<i64>>,
    ) -> Result<Option<HashMap<i64, String>>> {
        let command = get_refresh_command(schema_type);
        let map = match schema_type {
            SchemaType::Labels => &self.labels,
            SchemaType::Properties => &self.properties,
            SchemaType::Relationships => &self.relationships,
        };

        let mut write_lock = map.write().await;

        // This is essentially the call_procedure(), but can be done here without access to the graph(which would cause ownership issues)
        let [_, keys, _]: [FalkorValue; 3] = conn
            .send_command(
                Some(self.graph_name.as_str()),
                "GRAPH.QUERY",
                None,
                Some(&[format!("CALL {command}()")]),
            )
            .await?
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        Ok(update_map(write_lock.deref_mut(), keys, id_hashset)?)
    }
}
