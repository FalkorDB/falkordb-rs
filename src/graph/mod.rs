/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::client::ProvidesSyncConnections;
use crate::GraphSchema;

pub(crate) mod blocking;
pub(crate) mod query_builder;

#[cfg(feature = "tokio")]
pub(crate) mod asynchronous;

pub trait HasGraphSchema<C: ProvidesSyncConnections> {
    fn get_graph_schema(&self) -> &GraphSchema<C>;

    fn get_graph_schema_mut(&mut self) -> &mut GraphSchema<C>;
}
