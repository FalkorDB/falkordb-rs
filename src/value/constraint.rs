/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use super::FalkorValue;
use std::collections::HashMap;

pub struct Constraint {
    _type: String,
    pub label: String,
    properties: HashMap<String, FalkorValue>,
    entity_type: FalkorValue,
    status: String,
}
