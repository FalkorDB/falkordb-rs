/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

#![allow(private_interfaces)]
#![allow(private_bounds)]
#![allow(clippy::multiple_crate_versions)]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![doc = include_str!("../README.md")]

mod client;
mod connection;
mod connection_info;
mod error;
mod graph;
mod graph_schema;
mod parser;
mod response;
mod value;

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
pub use response::{
    constraint::{Constraint, ConstraintStatus, ConstraintType},
    execution_plan::ExecutionPlan,
    index::{FalkorIndex, IndexStatus, IndexType},
    lazy_result_set::LazyResultSet,
    slowlog_entry::SlowlogEntry,
    QueryResult,
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

    pub struct TestSyncGraphHandle {
        pub(crate) inner: SyncGraph,
    }

    impl Drop for TestSyncGraphHandle {
        fn drop(&mut self) {
            self.inner.delete().ok();
        }
    }

    #[cfg(feature = "tokio")]
    pub(crate) struct TestAsyncGraphHandle {
        pub(crate) inner: AsyncGraph,
    }

    #[cfg(feature = "tokio")]
    impl Drop for TestAsyncGraphHandle {
        fn drop(&mut self) {
            tokio::task::block_in_place(|| {
                // Avoid copying the schema each time
                let mut graph_handle =
                    AsyncGraph::new(self.inner.get_client().clone(), self.inner.graph_name());
                tokio::runtime::Handle::current().block_on(async move {
                    graph_handle.delete().await.ok();
                })
            })
        }
    }

    pub fn create_test_client() -> FalkorSyncClient {
        FalkorClientBuilder::new()
            .build()
            .expect("Could not create client")
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn create_async_test_client() -> FalkorAsyncClient {
        FalkorClientBuilder::new_async()
            .build()
            .await
            .expect("Could not create client")
    }

    pub fn open_empty_test_graph(graph_name: &str) -> TestSyncGraphHandle {
        let client = create_test_client();

        TestSyncGraphHandle {
            inner: client.select_graph(graph_name),
        }
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn open_empty_async_test_graph(graph_name: &str) -> TestAsyncGraphHandle {
        let client = create_async_test_client().await;

        TestAsyncGraphHandle {
            inner: client.select_graph(graph_name),
        }
    }
}
