/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

#![allow(private_interfaces)]
#![allow(private_bounds)]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![doc = include_str!("../README.md")]

#[cfg(not(feature = "redis"))]
compile_error!("The `redis` feature must be enabled.");

mod client;
mod connection;
mod connection_info;
mod error;
mod graph;
mod graph_schema;
mod parser;
mod response;
mod value;

#[cfg(feature = "redis")]
mod redis_ext;

/// A [`Result`] which only returns [`FalkorDBError`] as its E type
pub type FalkorResult<T> = Result<T, FalkorDBError>;

pub use client::{blocking::FalkorSyncClient, builder::FalkorClientBuilder};
pub use connection_info::FalkorConnectionInfo;
pub use error::FalkorDBError;
pub use graph::{
    blocking::SyncGraph,
    query_builder::{ProcedureQueryBuilder, QueryBuilder},
};
pub use graph_schema::{GraphSchema, SchemaType};
pub use parser::FalkorParsable;
pub use response::{
    constraint::{Constraint, ConstraintStatus, ConstraintType},
    execution_plan::ExecutionPlan,
    index::{FalkorIndex, IndexStatus, IndexType},
    lazy_result_set::LazyResultSet,
    slowlog_entry::SlowlogEntry,
    FalkorResponse,
};
pub use value::{
    config::ConfigValue,
    graph_entities::{Edge, EntityType, Node},
    path::Path,
    point::Point,
    FalkorValue,
};

#[cfg(feature = "tokio")]
pub use client::asynchronous::FalkorAsyncClient;
#[cfg(feature = "tokio")]
pub use graph::asynchronous::AsyncGraph;

#[cfg(test)]
pub(crate) mod test_utils {
    use super::*;

    pub(crate) struct TestSyncGraphHandle {
        pub(crate) inner: SyncGraph,
    }

    impl Drop for TestSyncGraphHandle {
        fn drop(&mut self) {
            self.inner.delete().ok();
        }
    }

    pub(crate) fn create_test_client() -> FalkorSyncClient {
        FalkorClientBuilder::new()
            .build()
            .expect("Could not create client")
    }

    pub(crate) fn open_test_graph(graph_name: &str) -> TestSyncGraphHandle {
        let client = create_test_client();

        client.select_graph(graph_name).delete().ok();

        TestSyncGraphHandle {
            inner: client
                .copy_graph("imdb", graph_name)
                .expect("Could not copy graph for test"),
        }
    }
}
