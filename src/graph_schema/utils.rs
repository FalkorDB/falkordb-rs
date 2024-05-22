/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorValue, SchemaType};
use anyhow::Result;
use std::collections::{HashMap, HashSet};

pub(crate) fn get_refresh_command(schema_type: SchemaType) -> &'static str {
    match schema_type {
        SchemaType::Labels => "DB.LABELS",
        SchemaType::Properties => "DB.PROPERTYKEYS",
        SchemaType::Relationships => "DB.RELATIONSHIPTYPES",
    }
}

pub(crate) fn update_map(
    map_to_update: &mut HashMap<i64, String>,
    keys: FalkorValue,
    id_hashset: Option<&HashSet<i64>>,
) -> Result<Option<HashMap<i64, String>>> {
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

    *map_to_update = new_keys;

    match id_hashset {
        None => Ok(None),
        Some(id_hashset) => {
            let mut relevant_ids = HashMap::with_capacity(id_hashset.len());
            for id in id_hashset {
                relevant_ids.insert(
                    *id,
                    map_to_update
                        .get(id)
                        .cloned()
                        .ok_or(FalkorDBError::ParsingError)?,
                );
            }

            Ok(Some(relevant_ids))
        }
    }
}

pub(crate) fn get_relevant_hashmap(
    id_set: &HashSet<i64>,
    locked_map: &HashMap<i64, String>,
) -> Option<HashMap<i64, String>> {
    let mut id_hashmap = HashMap::new();
    for id in id_set {
        if let Some(id_val) = locked_map.get(id).cloned() {
            id_hashmap.insert(*id, id_val);
            continue;
        }

        return None;
    }

    Some(id_hashmap)
}
