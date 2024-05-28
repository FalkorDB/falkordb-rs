/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

pub mod utils;

use crate::{connection::blocking::BorrowedSyncConnection, FalkorValue, GraphSchema};
use anyhow::Result;

/// This trait allows implementing a parser from the table-style result sent by the database, to any other struct
pub trait FalkorParsable: Sized {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &mut GraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> Result<Self>;
}
