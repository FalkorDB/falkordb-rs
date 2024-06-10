/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

pub mod utils;

use crate::{client::ProvidesSyncConnections, FalkorResult, FalkorValue, GraphSchema};

/// This trait allows implementing a parser from the table-style result sent by the database, to any other struct
pub trait FalkorParsable: Sized {
    /// Parse the following value, using the graph schem owned by the graph object, and the connection used to make the request
    fn from_falkor_value<C: ProvidesSyncConnections>(
        value: FalkorValue,
        graph_schema: &mut GraphSchema<C>,
    ) -> FalkorResult<Self>;
}
