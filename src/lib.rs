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
pub(crate) mod parser;
mod value;

#[cfg(feature = "redis")]
mod redis_ext;

pub use client::builder::FalkorDBClientBuilder;
pub use error::FalkorDBError;
