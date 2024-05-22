/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection, FalkorParsable, FalkorValue, SyncGraphSchema,
};
use std::collections::HashMap;

/// TODO: I...honestly don't know what this it
pub struct Constraint {
    _type: String,
    label: String,
    properties: HashMap<String, FalkorValue>,
    entity_type: FalkorValue,
    status: String,
}

impl FalkorParsable for Constraint {
    fn from_falkor_value(
        _value: FalkorValue,
        _graph_schema: &SyncGraphSchema,
        _conn: &mut BorrowedSyncConnection,
    ) -> anyhow::Result<Self> {
        todo!()
    }
}
