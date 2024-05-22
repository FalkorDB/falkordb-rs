/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::connection::blocking::BorrowedSyncConnection;
use crate::graph::schema::GraphSchema;
use crate::value::FalkorValue;
use anyhow::Result;

pub trait FalkorParsable: Sized {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &GraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> Result<Self>;
}
