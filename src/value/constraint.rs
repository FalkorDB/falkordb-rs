/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use super::FalkorValue;

pub struct Constraint {
    _type: String,
    label: String,
    properties: FalkorValue,
    entity_type: FalkorValue,
    status: String,
}
