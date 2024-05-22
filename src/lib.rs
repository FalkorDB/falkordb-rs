/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

#[cfg(not(feature = "redis"))]
compile_error!("The `redis` feature must be enabled.");

mod client;
mod connection;
mod connection_info;
mod error;
mod graph;
mod graph_schema;
mod parser;
mod value;

#[cfg(feature = "redis")]
mod redis_ext;

pub use client::{blocking::SyncFalkorClient, builder::FalkorDBClientBuilder};
pub use connection_info::FalkorConnectionInfo;
pub use error::FalkorDBError;
pub use graph::blocking::SyncGraph;
pub use graph_schema::{blocking::SyncGraphSchema, SchemaType};
pub use parser::FalkorParsable;
pub use value::{
    config::ConfigValue,
    constraint::Constraint,
    execution_plan::ExecutionPlan,
    graph_entities::{Edge, Node},
    path::Path,
    point::Point,
    query_result::QueryResult,
    slowlog_entry::SlowlogEntry,
    FalkorValue,
};

#[cfg(feature = "tokio")]
pub use {
    client::asynchronous::AsyncFalkorClient, connection::asynchronous::FalkorAsyncConnection,
    graph::asynchronous::AsyncGraph, graph_schema::asynchronous::AsyncGraphSchema,
    parser::FalkorAsyncParseable,
};
