/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

pub(crate) mod blocking;
mod utils;

#[cfg(feature = "tokio")]
pub(crate) mod asynchronous;

/// An enum specifying which schema type we are addressing
/// When querying using the compact parser, ids are returned for the various schema entities instead of strings
/// Using this enum we know which of the schema maps to access in order to convert these ids to strings
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SchemaType {
    Labels,
    Properties,
    Relationships,
}
