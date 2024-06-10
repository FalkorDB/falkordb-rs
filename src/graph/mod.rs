/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{client::ProvidesSyncConnections, GraphSchema};

pub(crate) mod blocking;
pub(crate) mod query_builder;
pub(crate) mod utils;

#[cfg(feature = "tokio")]
pub(crate) mod asynchronous;

pub(crate) trait HasGraphSchema<C: ProvidesSyncConnections> {
    fn get_graph_schema_mut(&mut self) -> &mut GraphSchema<C>;
}
