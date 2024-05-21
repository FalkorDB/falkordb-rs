/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::connection::blocking::BorrowedSyncConnection;
use crate::error::FalkorDBError;
use crate::graph::schema::{GraphSchema, SchemaType};
use crate::value::{parse_type, FalkorValue};
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};

// Intermediate type for map parsing
pub(crate) struct FKeyTypeVal {
    key: i64,
    type_marker: i64,
    val: FalkorValue,
}

impl TryFrom<FalkorValue> for FKeyTypeVal {
    type Error = anyhow::Error;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        let [key_raw, type_raw, val]: [FalkorValue; 3] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingKTVTypes)?;

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

pub(crate) fn get_schema_subset(
    ids: &HashSet<i64>,
    schema: &HashMap<i64, String>,
) -> Option<HashMap<i64, String>> {
    let mut hashmap = HashMap::new();
    for id in ids {
        hashmap.insert(*id, schema.get(id).cloned()?);
    }

    Some(hashmap)
}

// TODO: This does NOT support nested attributes yet
pub(crate) fn parse_map_with_schema(
    value: FalkorValue,
    graph_schema: &GraphSchema,
    conn: &mut BorrowedSyncConnection,
    schema_type: SchemaType,
) -> anyhow::Result<HashMap<String, FalkorValue>> {
    let val_vec = value.into_vec()?;
    let (mut id_hashset, mut map_vec) = (
        HashSet::with_capacity(val_vec.len()),
        Vec::with_capacity(val_vec.len()),
    );

    for item in val_vec {
        let mut fktv = FKeyTypeVal::try_from(item)?;
        id_hashset.insert(fktv.key);
        map_vec.push(fktv);
    }

    let locked_id_to_string_map = match schema_type {
        SchemaType::Labels => graph_schema.labels(),
        SchemaType::Properties => graph_schema.properties(),
        SchemaType::Relationships => graph_schema.relationships(),
    };

    let relevant_ids_map = {
        let read_lock = locked_id_to_string_map.read();
        get_schema_subset(&id_hashset, read_lock.deref())
    };

    if let Some(relevant_ids_map) = relevant_ids_map {
        let mut new_map = HashMap::with_capacity(map_vec.len());
        for fktv in map_vec {
            new_map.insert(
                relevant_ids_map
                    .get(&fktv.key)
                    .cloned()
                    .ok_or(FalkorDBError::ParsingError)?,
                parse_type(fktv.type_marker, fktv.val, graph_schema, conn)?,
            );
        }

        return Ok(new_map);
    }

    // If we reached here, schema validation failed and we need to refresh our schema
    let command = match schema_type {
        SchemaType::Labels => "DB.LABELS",
        SchemaType::Properties => "DB.PROPERTYKEYS",
        SchemaType::Relationships => "DB.RELATIONSHIPTYPES",
    };
    let [_, keys, _]: [FalkorValue; 3] = conn
        .send_command(
            Some(graph_schema.graph_name()),
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

    let id_map = match schema_type {
        SchemaType::Labels => graph_schema.labels(),
        SchemaType::Properties => graph_schema.properties(),
        SchemaType::Relationships => graph_schema.relationships(),
    };

    let mut lock = id_map.write();
    *(lock.deref_mut()) = new_keys;

    let mut new_map = HashMap::with_capacity(map_vec.len());
    for fktv in map_vec {
        new_map.insert(
            lock.deref()
                .get(&fktv.key)
                .cloned()
                .ok_or(FalkorDBError::ParsingError)?,
            parse_type(fktv.type_marker, fktv.val, graph_schema, conn)?,
        );
    }

    Ok(new_map)
}

pub(crate) fn parse_map(
    value: FalkorValue,
    graph_schema: &GraphSchema,
    conn: &mut BorrowedSyncConnection,
) -> anyhow::Result<HashMap<String, FalkorValue>> {
    let val_vec = value.into_vec()?;

    let mut new_map = HashMap::with_capacity(val_vec.len());
    for val in val_vec {
        let fktv = FKeyTypeVal::try_from(val)?;
        new_map.insert(
            fktv.key.to_string(),
            parse_type(fktv.type_marker, fktv.val, graph_schema, conn)?,
        );
    }

    Ok(new_map)
}
