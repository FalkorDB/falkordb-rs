/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

#![recursion_limit = "256"]
#![allow(private_interfaces)]
#![allow(private_bounds)]
#![allow(mismatched_lifetime_syntaxes)]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
//! The official Rust client for [FalkorDB](https://www.falkordb.com/) — a fast, low-latency
//! graph database. One ergonomic API across a blocking client and an async (`tokio`) client, with
//! typed results, parameter binding, batching, an embedded server, TLS, automatic retries, and
//! OpenTelemetry-aligned tracing and metrics.
//!
//! ## Highlights
//!
//! - **Sync and async** — a blocking client and a `tokio` async client share the same ergonomic API.
//! - **Header-aware, typed results** — read columns by name or index, or map rows straight into your own `serde` types.
//! - **Safe parameters** — bind Rust values as Cypher literals; no hand-quoting, no injection.
//! - **Batching and pipelining** — send many queries in a single round-trip.
//! - **Async streaming** — result sets are `Stream`s that compose with the full `futures` toolbox.
//! - **Resilient** — opt-in `RetryPolicy` with bounded backoff for transient failures; writes are never retried.
//! - **Observable** — OpenTelemetry-aligned `tracing` spans and `metrics` counters/histograms, privacy-safe by default.
//! - **Replica-aware** — opt in to routing read-only queries to replicas behind Redis Sentinel.
//! - **Embedded server** — spin up a self-contained FalkorDB for tests and prototyping, with an
//!   optional build-time bundle mode that runs fully offline.
//!
//! ## Table of contents
//!
//! - [Highlights](#highlights)
//! - [Quickstart](#quickstart)
//! - [Cargo feature flags](#cargo-feature-flags)
//! - [Guide](#guide)
//!   - [Queries and results](#queries-and-results)
//!   - [Async](#async)
//!   - [Execution patterns](#execution-patterns)
//!   - [Connections and networking](#connections-and-networking)
//!   - [Resilience and observability](#resilience-and-observability)
//!   - [Embedded server](#embedded-server)
//! - [Examples](#examples)
//! - [API documentation](#api-documentation)
//! - [Migration guides](#migration-guides)
//! - [Contributing](#contributing)
//! - [Community and license](#community-and-license)
//!
//! ## Quickstart
//!
//! ### Install
//!
//! Install it with [`cargo add`](https://doc.rust-lang.org/cargo/commands/cargo-add.html):
//!
//! ```bash
//! cargo add falkordb
//! ```
//!
//! ### Run a FalkorDB server
//!
//! Docker:
//!
//! ```sh
//! docker run --rm -p 6379:6379 falkordb/falkordb
//! ```
//!
//! ### Your first query
//!
//! ```no_run
//! use falkordb::{FalkorClientBuilder, FalkorConnectionInfo};
//!
//! // Connect to FalkorDB
//! let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()
//!             .expect("Invalid connection info");
//!
//! let client = FalkorClientBuilder::new()
//!            .with_connection_info(connection_info)
//!            .build()
//!            .expect("Failed to build client");
//!
//! // Select the social graph
//! let mut graph = client.select_graph("social");
//!
//! // Create 100 nodes and return a handful
//! let mut nodes = graph.query("UNWIND range(1, 100) AS i CREATE (n { v:1 }) RETURN n LIMIT 10")
//!             .with_timeout(5000)
//!             .execute()
//!             .expect("Failed executing query");
//!
//! // Each item is a `FalkorResult<Row>`; read columns by index or name.
//! while let Some(row) = nodes.data.next() {
//!    let row = row.expect("row failed to parse");
//!    println!("{:?}", row.get_at(0));
//! }
//! ```
//!
//! ## Cargo feature flags
//!
//! All features are **off by default** — enable only what you need:
//!
//! | Feature | Enables |
//! |---|---|
//! | `tokio` | The async client and API on the `tokio` runtime (multi-threaded scheduler). |
//! | `serde` | Map query results into your own `serde::Deserialize` types. |
//! | `tracing` | OpenTelemetry-aligned `tracing` spans with a privacy-safe query fingerprint. |
//! | `metrics` | Counters and histograms via the `metrics` facade (install any exporter). |
//! | `embedded` | Run a self-contained embedded FalkorDB server (module downloaded at runtime). |
//! | `embedded-bundle` | Embed the module at build time so the embedded server runs fully offline. |
//! | `rustls` / `native-tls` | TLS for the sync client, via `rustls` or `native-tls`. |
//! | `tokio-rustls` / `tokio-native-tls` | TLS for the async client. |
//!
//! ```bash
//! cargo add falkordb --features tokio,serde
//! ```
//!
//! ## Guide
//!
//! Each capability below has a short explanation, a minimal snippet, and a link to a complete,
//! runnable example. The async client mirrors the sync API — `await` the terminals.
//!
//! ### Queries and results
//!
//! #### Header-aware result rows
//!
//! `QueryResult::data` iterates the result set as `FalkorResult<Row>`. Each `Row` pairs the query
//! header (the column aliases) with that row's values, so you read columns by **name or index** and a
//! row that fails to parse surfaces as an `Err` instead of being silently swallowed:
//!
//! ```no_run
//! use falkordb::{FalkorClientBuilder, FalkorConnectionInfo};
//!
//! let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()
//!     .expect("Invalid connection info");
//! let client = FalkorClientBuilder::new()
//!     .with_connection_info(connection_info)
//!     .build()
//!     .expect("Failed to build client");
//! let mut graph = client.select_graph("imdb");
//!
//! let mut result = graph
//!     .query("MATCH (m:Movie) RETURN m.title AS title, m.year AS year")
//!     .execute()
//!     .expect("Failed executing query");
//!
//! for row in result.data.by_ref() {
//!     let row = row.expect("row failed to parse");
//!     // Read a column by alias and convert it in one step (strictly, via `FromFalkorValue`).
//!     let title: String = row.try_get("title").expect("title column");
//!     let year: i64 = row.try_get("year").expect("year column");
//!     println!("{title} ({year})");
//! }
//! ```
//!
//! `Row` offers borrowing accessors (`get`, `get_at`, `get_all`), typed accessors
//! (`try_get::<T>`, `try_get_at::<T>`), and consuming conversions (`into_values`, `into_map`). Typed
//! access is **strict** — no silent lossy casts — via the [`FromFalkorValue`] conversion trait. Because
//! `collect` short-circuits on the first `Err`, a whole result set can be gathered with
//! `result.data.collect::<falkordb::FalkorResult<Vec<_>>>()`.
//!
//! FalkorDB rejects a query whose result columns are not uniquely named, so rows from a query always
//! have distinct columns; if a `Row` ever does hold duplicates, the access paths are still defined
//! (`get`/`try_get` return the first match, `get_all` returns every match, `into_map` keeps the last).
//! To opt back into the pre-0.7 behavior (bare `Vec<FalkorValue>` rows, parse errors collapsed to
//! `FalkorValue::Unparseable`), call `result.data.into_values_lossy()`. A runnable version lives in
//! [`examples/rows.rs`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/rows.rs). Upgrading from 0.6? See the
//! [0.7 migration guide](https://github.com/FalkorDB/falkordb-rs/blob/main/docs/migrating-to-0.7.md).
//!
//! #### Type-safe query parameters
//!
//! Pass Rust values straight into a query — the client encodes them as Cypher literals and escapes
//! them for you, so you never hand-quote strings or risk Cypher injection:
//!
//! ```ignore
//! let res = graph
//!     .query("MATCH (m:Movie {title: $title}) WHERE m.year IN $years RETURN m")
//!     .with_param("title", "The Matrix")
//!     .with_param("years", [1999, 2003])
//!     .execute()?;
//! ```
//!
//! Add several at once from an array, `Vec`, or map with `with_params` (the values share a single
//! type; use chained `with_param` calls, as above, for a mix of types):
//!
//! ```ignore
//! .with_params([("min_year", 1990), ("max_year", 2000)])
//! ```
//!
//! Supported value types include integers, floats, boolean values, strings, `Option` (encoded as
//! `null`), arrays/`Vec`, and string-keyed `HashMap`/`BTreeMap` (nested freely). Points and vectors
//! cannot be bound directly (a FalkorDB limitation) — pass the components and construct them in the
//! query:
//!
//! ```ignore
//! use std::collections::BTreeMap;
//! let coords = BTreeMap::from([("latitude", 32.07), ("longitude", 34.79)]);
//! graph.query("RETURN point($p)").with_param("p", coords).execute()?;
//! ```
//!
//! If you really need a raw Cypher expression, `with_raw_param("key", "…")` is the explicit escape
//! hatch — no escaping is applied to the value (the parameter name is still validated).
//!
//! Temporal values returned by queries — `datetime`, `date`, `time`/`localtime` and `duration` —
//! decode into the typed `DateTime`, `Date`, `Time` and `Duration` values. Each exposes its scalar as a
//! typed `Seconds` (`value.seconds()`), and `DateTime`/`Duration` support a small type-safe algebra
//! (`DateTime - DateTime` → `Duration`, `DateTime ± Duration` → `DateTime`, plus `Duration`
//! add/subtract/negate) with overflow-checked `checked_*` variants. They are read from results but
//! cannot be bound back as parameters — build them in the query with the matching Cypher function (e.g.
//! `date($s)`). A runnable version lives in [`examples/temporal.rs`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/temporal.rs).
//!
//! #### Typed result mapping with serde
//!
//! Enable the optional `serde` feature to map query results straight into your own types instead of hand-matching every
//! `FalkorValue` variant:
//!
//! ```bash
//! cargo add falkordb --features serde
//! ```
//!
//! Derive `serde::Deserialize` on your type and call `FalkorValue::deserialize_into` (or the free function
//! `falkordb::from_falkor_value`) on a returned value. A node is deserialized from its properties, and scalars, `Option`,
//! sequences and maps map onto the matching Rust types:
//!
//! ```ignore
//! use falkordb::{FalkorClientBuilder, FalkorConnectionInfo};
//! use serde::Deserialize;
//! #[derive(Debug, Deserialize)]
//! struct Movie {
//!     title: String,
//!     year: i64,
//!     rating: Option<f64>,
//! }
//! let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()
//!     .expect("Invalid connection info");
//! let client = FalkorClientBuilder::new()
//!     .with_connection_info(connection_info)
//!     .build()
//!     .expect("Failed to build client");
//! let mut graph = client.select_graph("imdb");
//! let mut result = graph.query("MATCH (m:Movie) RETURN m").execute()
//!     .expect("Failed executing query");
//! for row in result.data.by_ref() {
//!     let row = row.expect("row failed to parse");
//!     if let Some(node) = row.into_iter().next() {
//!         let movie: Movie = node.deserialize_into().expect("Failed to map node");
//!         println!("{} ({})", movie.title, movie.year);
//!     }
//! }
//! ```
//!
//! A runnable version lives in [`examples/typed_mapping.rs`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/typed_mapping.rs).
//!
//! To map a whole result set in one shot, call `query_as::<T>()` before `execute()`. Each row is
//! deserialized into a `T`, and the result's `data` becomes an iterator of `FalkorResult<T>`, so it
//! collects directly into a `Vec`:
//!
//! ```ignore
//! let movies: Vec<Movie> = graph
//!     .query("MATCH (m:Movie) RETURN m")
//!     .query_as::<Movie>()
//!     .execute()
//!     .expect("Failed executing query")
//!     .data
//!     .collect::<Result<_, _>>()
//!     .expect("Failed mapping rows");
//! ```
//!
//! A single-column row (such as `RETURN m`) is deserialized from that one column's value, so a node
//! maps from its properties and `RETURN count(m)` maps a scalar. A multi-column row (such as
//! `RETURN m.title AS title, m.year AS year`) maps each column alias onto the matching struct field,
//! or yields the values in order for a tuple. The query `header` and `stats` remain available on the
//! returned result.
//!
//! ### Async
//!
//! #### tokio support
//!
//! This client supports nonblocking API using the [`tokio`](https://tokio.rs/) runtime.
//! It can be enabled like so:
//!
//! ```bash
//! cargo add falkordb --features tokio
//! ```
//!
//! Currently, this API requires running within a [
//! `multi_threaded tokio scheduler`](https://docs.rs/tokio/latest/tokio/runtime/index.html#multi-thread-scheduler), and
//! does not support the `current_thread` one, but this will probably be supported in the future.
//!
//! The API uses an almost identical API, but the various functions need to be awaited:
//!
//! ```ignore
//! use falkordb::{FalkorClientBuilder, FalkorConnectionInfo};
//! use futures::StreamExt; // brings `.next().await` onto the result stream
//!
//! // Connect to FalkorDB
//! let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()
//!             .expect("Invalid connection info");
//!
//! let client = FalkorClientBuilder::new_async()
//!             .with_connection_info(connection_info)
//!             .build()
//!             .await
//!             .expect("Failed to build client");
//!
//! // Select the social graph
//! let mut graph = client.select_graph("social");
//!
//! // Create 100 nodes and return a handful
//! let mut nodes = graph.query("UNWIND range(1, 100) AS i CREATE (n { v:1 }) RETURN n LIMIT 10")
//!             .with_timeout(5000)
//!             .execute()
//!             .await
//!             .expect("Failed executing query");
//!
//! // `nodes.data` is a `Stream<Item = FalkorResult<Row>>`; pull rows with `.next().await`:
//! while let Some(row) = nodes.data.next().await {
//!      let row = row.expect("row failed to parse");
//!      println!("{:?}", row.get_at(0));
//! }
//! ```
//!
//! The result set (`nodes.data`) is an owned, `Send + 'static` `Stream`, so it can be moved into a
//! spawned task and driven with the full `StreamExt` / `TryStreamExt` toolbox. The graph
//! handle itself is `Send + Clone`: cloning is cheap and **shares one schema cache**, so to use a graph
//! from several concurrent tasks you just clone it — no `Arc<Mutex<_>>` wrapping required.
//!
//! #### Async streaming
//!
//! Because results are a `Stream`, the standard combinators just work. Import the extension traits
//! (`use futures::{StreamExt, TryStreamExt};`) and:
//!
//! ```ignore
//! use futures::{StreamExt, TryStreamExt};
//! // Collect a typed stream in one line (errors short-circuit):
//! let years: Vec<i64> = graph
//!     .query("MATCH (m:Movie) RETURN m.year AS year ORDER BY year")
//!     .execute()
//!     .await?
//!     .data
//!     .map(|row| row?.try_get::<i64>("year"))
//!     .try_collect()
//!     .await?;
//! // Move a result stream into its own task (it is `Send + 'static`):
//! let mut stream = graph.query("MATCH (n) RETURN n").execute().await?.data;
//! let count = tokio::spawn(async move {
//!     let mut n = 0usize;
//!     while let Some(row) = stream.next().await {
//!         row?;
//!         n += 1;
//!     }
//!     Ok::<_, falkordb::FalkorDBError>(n)
//! })
//! .await
//! .unwrap()?;
//! // Fan out a follow-up query per row with bounded concurrency, over cloned handles:
//! let enriched: Vec<i64> = graph
//!     .query("MATCH (m:Movie) RETURN m.year AS year")
//!     .execute()
//!     .await?
//!     .data
//!     .map(|row| {
//!         let mut g = graph.clone(); // cheap; shares the schema cache
//!         async move {
//!             let year: i64 = row?.try_get("year")?;
//!             let mut r = g.query(format!("RETURN {year} + 1 AS next")).execute().await?;
//!             r.data.try_next().await?.expect("a row").try_get::<i64>("next")
//!         }
//!     })
//!     .buffer_unordered(8)
//!     .try_collect()
//!     .await?;
//! ```
//!
//! A runnable version lives in [`examples/async_stream.rs`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/async_stream.rs).
//!
//! #### Connection strategy and multiplexing
//!
//! The asynchronous client chooses how it manages its underlying Redis connections via a
//! `ConnectionStrategy`:
//!
//! - **`Multiplexed`** (the async default): a small number of shared, cloneable,
//!   auto-reconnecting connections. Many concurrent commands are pipelined over each socket,
//!   so a single connection can carry many in-flight requests at once. This avoids the
//!   borrow/return bottleneck and is the most efficient option for highly concurrent
//!   workloads.
//! - **`Pooled`**: a fixed pool of independent connections, each used by exactly one command
//!   at a time (borrow/return). This gives strict per-command isolation and a natural cap on
//!   in-flight commands. It is the only strategy for the synchronous client.
//!
//! Select or tune the strategy on the builder:
//!
//! ```no_run
//! use falkordb::{ConnectionStrategy, FalkorClientBuilder};
//! use std::num::NonZeroU8;
//!
//! # async fn doc() {
//! // Spread commands across 4 shared multiplexed sockets (the default uses 8).
//! let client = FalkorClientBuilder::new_async()
//!     .with_connection_strategy(ConnectionStrategy::Multiplexed {
//!         connections: NonZeroU8::new(4).unwrap(),
//!     })
//!     // Optional backpressure: cap concurrently in-flight commands per socket.
//!     .with_max_inflight(std::num::NonZeroUsize::new(256).unwrap())
//!     .build()
//!     .await
//!     .expect("Failed to build client");
//!
//! assert_eq!(client.connection_pool_size(), 4);
//! # }
//! ```
//!
//! Notes and caveats:
//!
//! - **Behavior change:** the async default is now multiplexed (previously an exclusive
//!   borrow-pool). The API is source-compatible; `with_num_connections` now sets the number
//!   of underlying connections/sockets for the active strategy, and `connection_pool_size()`
//!   reports that count.
//! - **Backpressure:** multiplexed mode does not bound the number of outstanding requests
//!   unless you set `with_max_inflight(n)` (where `n` is a `NonZeroUsize`; ignored by the
//!   pooled strategy, whose pool size already caps in-flight commands).
//! - **Sentinel:** a multiplexed connection built from a Sentinel-resolved node would not
//!   re-resolve the master/replica on failover, so for Sentinel deployments the client
//!   transparently falls back to the pooled strategy (which re-resolves on reconnect).
//!   `connection_strategy()` returns this *effective* strategy.
//!
//! A runnable example is provided in [`examples/multiplexed_async.rs`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/multiplexed_async.rs).
//!
//! ### Execution patterns
//!
//! #### Batch and pipelined execution
//!
//! Normally each query is one network round-trip. `graph.batch()` queues several queries and sends them
//! over a single Redis pipeline in **one round-trip**, returning one result per query **in submission
//! order**. Queue queries with `query` (a `GRAPH.QUERY`) / `ro_query` (a `GRAPH.RO_QUERY`) and set
//! per-query parameters on the returned handle:
//!
//! ```ignore
//! let mut batch = graph.batch();
//! for movie in &movies {
//!     batch.query("CREATE (:Movie {title: $t})").with_param("t", movie);
//! }
//! batch.ro_query("MATCH (m:Movie) RETURN count(m) AS n");
//!
//! let results = batch.execute()?; // Vec<BatchItemResult>, one per query, in order
//! for (i, item) in results.into_iter().enumerate() {
//!     match item {
//!         Ok(result) => { /* result.data: Vec<Row>, result.header, result.stats */ }
//!         Err(err) => eprintln!("query {i} failed: {err}"),
//!     }
//! }
//! ```
//!
//! On the async client it is identical but for the `await`:
//!
//! ```ignore
//! let mut batch = graph.batch();
//! // … queue queries …
//! let results = batch.execute().await?;
//! ```
//!
//! Key points:
//!
//! - **Per-item errors.** A failing query (bad Cypher, or a parameter that can't be encoded) becomes
//!   that slot's `Err`; the other queries are unaffected. The **outer** `Result` only fails if the whole
//!   batch could not be completed — and if that happens *after* the pipeline was sent, the server may
//!   have run some or all queries (the state is unknown), which matters for writes.
//! - **Not a transaction.** A pipeline is not `MULTI`/`EXEC`: every queued query is executed, so a
//!   failure in one does **not** roll back or stop the others.
//! - **Results are eager.** Each query's rows are parsed up front into a `Vec<Row>` (the same `Row` as
//!   elsewhere), since many result sets coexist in one batch.
//! - **Owned queries.** To build queries ahead of time, construct `BatchQuery::write(..)` /
//!   `BatchQuery::read(..)`, attach params/timeout, and `batch.push(query)`.
//!
//! A runnable version lives in [`examples/batch.rs`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/batch.rs).
//!
//! #### Waiting for background operations
//!
//! Some FalkorDB operations finish **after** the command that starts them returns: when you create or
//! drop an index or constraint, the request returns immediately while the index is populated (or the
//! constraint is enforced) on a background worker thread, and `GRAPH.COPY` can fail transiently while
//! the server is unable to `fork`. The eager methods
//! (`create_index`, `create_unique_constraint`, `copy_graph`, …) stay fire-and-forget, but every
//! one of them now has an additive `*_op` builder that adds explicit, opt-in waiting while keeping
//! full backward compatibility.
//!
//! Each builder offers `.execute()` (non-blocking, identical to the eager method) and `.wait()` /
//! `.wait_with(WaitOptions)` terminals. For index and constraint builders, `.wait()` blocks until the
//! operation has actually taken effect (the index/constraint becomes operational or is dropped),
//! returning [`FalkorDBError::Timeout`] if it does not happen in time. For the copy builder, `GRAPH.COPY`
//! is already blocking on the server, so `.wait()` simply retries transient `could not fork` failures
//! with backoff; it does **not** verify the copied contents (that remains the caller's responsibility).
//!
//! ```no_run
//! use falkordb::{EntityType, FalkorClientBuilder, FalkorConnectionInfo, IndexType, WaitOptions};
//! use std::time::Duration;
//!
//! let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()
//!             .expect("Invalid connection info");
//! let client = FalkorClientBuilder::new()
//!            .with_connection_info(connection_info)
//!            .build()
//!            .expect("Failed to build client");
//! let mut graph = client.select_graph("social");
//!
//! // Fire-and-forget, exactly like `create_index` (returns as soon as the server accepts it):
//! graph.create_index_op(IndexType::Range, EntityType::Node, "Person", &["age"], None)
//!      .execute()
//!      .expect("Failed to request index creation");
//!
//! // Block until the index is actually operational (default 30s readiness timeout):
//! graph.create_index_op(IndexType::Range, EntityType::Node, "Person", &["name"], None)
//!      .wait()
//!      .expect("Index did not become operational");
//!
//! // A unique constraint reports a *distinct* error if existing data violates it:
//! match graph.create_unique_constraint_op(EntityType::Node, "Person", &["email"])
//!            .wait_with(WaitOptions::with_timeout(Duration::from_secs(10)))
//! {
//!     Ok(()) => println!("constraint is enforced"),
//!     Err(falkordb::FalkorDBError::ConstraintFailed { .. }) => println!("data violates the constraint"),
//!     Err(other) => panic!("unexpected error: {other}"),
//! }
//!
//! // Copy a graph, retrying transient `could not fork` failures:
//! let _copy = client.copy_graph_op("social", "social_backup")
//!                   .wait()
//!                   .expect("Failed to copy graph");
//! ```
//!
//! The same builders exist on the async client — just `await` the terminals. See
//! [`examples/waiting_ops.rs`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/waiting_ops.rs) for a complete, runnable example.
//!
//! For vector indexes, the typed helpers `create_node_vector_index` / `create_edge_vector_index` take a
//! `dimension` and a `VectorSimilarity` (`Euclidean` or `Cosine`) and generate the correct
//! `OPTIONS { dimension: N, similarityFunction: '…' }` clause for you. Like the other index operations
//! they are fire-and-forget, and they have matching `create_node_vector_index_op` /
//! `create_edge_vector_index_op` builders that integrate with the waiting ergonomics above —
//! `.wait()` blocks until the vector index is operational (and `.execute()` is the non-blocking
//! equivalent). A runnable version lives in
//! [`examples/vector_index.rs`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/vector_index.rs).
//!
//! ### Connections and networking
//!
//! #### TLS support
//!
//! This client is currently built upon the [`redis`](https://docs.rs/redis/latest/redis/) crate, and therefore supports TLS
//! using
//! its implementation, which uses either [`rustls`](https://docs.rs/rustls/latest/rustls/) or [
//! `native_tls`](https://docs.rs/native-tls/latest/native_tls/).
//! This is not enabled by default, and the user just opt-in by enabling the respective features: `"rustls"`/`"native-tls"` (
//! when using tokio: `"tokio-rustls"`/`"tokio-native-tls"`).
//!
//! For Rustls:
//!
//! ```bash
//! cargo add falkordb --features rustls
//! ```
//!
//! ```bash
//! cargo add falkordb --features tokio-rustls
//! ```
//!
//! For Native TLS:
//!
//! ```bash
//! cargo add falkordb --features native-tls
//! ```
//!
//! ```bash
//! cargo add falkordb --features tokio-native-tls
//! ```
//!
//! A runnable example is provided in [`examples/tls.rs`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/tls.rs).
//!
//! #### TCP keepalive
//!
//! Long-lived clients behind NATs, stateful firewalls, or idle-timeout-enforcing
//! proxies can silently lose their TCP sessions. The builder exposes TCP-level
//! socket settings to prevent this:
//!
//! ```no_run
//! use falkordb::FalkorClientBuilder;
//! use std::time::Duration;
//!
//! // Convenience: just enable keepalive with a 30-second idle timeout
//! let client = FalkorClientBuilder::new()
//!     .with_tcp_keepalive(Duration::from_secs(30))
//!     .build()
//!     .expect("Failed to build client");
//!
//! // Or full control via redis::io::tcp::TcpSettings
//! let settings = redis::io::tcp::TcpSettings::default()
//!     .set_nodelay(true)
//!     .set_keepalive(
//!         redis::io::tcp::socket2::TcpKeepalive::new()
//!             .with_time(Duration::from_secs(60)),
//!     );
//! let client = FalkorClientBuilder::new()
//!     .with_tcp_settings(settings)
//!     .build()
//!     .expect("Failed to build client");
//! ```
//!
//! > **Note:** TCP settings apply to direct Redis TCP connections only.
//! > Unix-domain socket / embedded connections and the Sentinel connection path are
//! > not affected.
//!
//! #### Read-only queries and replica routing
//!
//! Read-only queries (`ro_query` and `call_procedure_ro`) send `GRAPH.RO_QUERY`, which the server
//! refuses to let write. *Where* such a query runs is a separate, **opt-in** choice expressed with
//! [`ReadPreference`]. Because a FalkorDB replica applies writes only **after** the primary, a read
//! served from a replica can be slightly **stale**, so the default ([`ReadPreference::Primary`])
//! keeps every read on the primary — you never observe replication lag unless you ask for it.
//!
//! Opt into replicas either per client or per query:
//!
//! - **Per client** — [`with_read_preference`](FalkorClientBuilder::with_read_preference) sets the
//!   default for every read-only query.
//! - **Per query** — [`prefer_replica`](QueryBuilder::prefer_replica) opts a single query in, and
//!   [`primary_only`](QueryBuilder::primary_only) forces one back onto the primary (read-your-writes).
//!   The per-query choice overrides the client default.
//!
//! Replica routing requires a Redis Sentinel deployment that exposes readable replicas; when none is
//! available (for example a single node), [`ReadPreference::PreferReplica`] transparently falls back
//! to the primary, so the same code runs everywhere. Writes always go to the primary — asking for a
//! replica on a writable `query`/`call_procedure`/batch fails with
//! [`FalkorDBError::ReadPreferenceNotReadOnly`].
//!
//! > **Connection pool sizing:** When readable replicas are present the client opens
//! > a second pool of up to `num_connections` additional connections (one per slot)
//! > alongside the primary pool, regardless of the read preference. Size your pool limits and
//! > file-descriptor limits accordingly.
//!
//! ```no_run
//! use falkordb::{FalkorClientBuilder, ReadPreference};
//!
//! let client = FalkorClientBuilder::new()
//!     // A Sentinel endpoint, e.g. falkor://127.0.0.1:26379
//!     .with_connection_info("falkor://127.0.0.1:26379".try_into().expect("Invalid connection info"))
//!     // Prefer replicas for this client's read-only queries (accepts slightly stale reads).
//!     .with_read_preference(ReadPreference::PreferReplica)
//!     .build()
//!     .expect("Failed to build client");
//!
//! // Capability (a replica pool exists) vs policy (the default routing).
//! if client.replica_reads_available() {
//!     println!("Replica connections are available");
//! }
//! println!("Default read preference: {:?}", client.read_preference());
//!
//! let mut graph = client.select_graph("imdb");
//!
//! // Writes go to the primary.
//! graph.query("CREATE (:Actor {name: 'Tom Hanks'})").execute().expect("Failed to write");
//!
//! // Follows the client default (a replica when available, else the primary).
//! let mut nodes = graph.ro_query("MATCH (a:Actor) RETURN a.name").execute().expect("Failed to read");
//!
//! // Force the freshest data from the primary for a single read, overriding the default.
//! let mut fresh = graph.ro_query("MATCH (a:Actor) RETURN a.name").primary_only().execute().expect("Failed to read");
//! ```
//!
//! Against a single node (or any deployment without readable replicas),
//! [`replica_reads_available`](FalkorSyncClient::replica_reads_available) returns `false` and reads
//! use the primary. See [`examples/readonly_replica.rs`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/readonly_replica.rs)
//! for a complete working example.
//!
//! ### Resilience and observability
//!
//! #### Automatic retries
//!
//! A client can opt in to a `RetryPolicy` that automatically re-issues *eligible* operations on
//! *transient* connection failures, with bounded backoff. It is **disabled by default**, so a client
//! built without one behaves exactly as before (every operation is attempted once):
//!
//! ```no_run
//! use falkordb::{Backoff, FalkorClientBuilder, RetryPolicy};
//! use std::time::Duration;
//!
//! # fn doc() -> Result<(), Box<dyn std::error::Error>> {
//! let client = FalkorClientBuilder::new()
//!     .with_retry_policy(
//!         RetryPolicy::read_only()                         // retry read-only ops only
//!             .max_attempts(4)                             // 1 initial try + up to 3 retries
//!             .backoff(Backoff::exponential(Duration::from_millis(50))
//!                 .max_delay(Duration::from_secs(1))),     // 50ms, 100ms, 200ms, … capped at 1s
//!     )
//!     .build()?;
//! # let _ = client;
//! # Ok(())
//! # }
//! ```
//!
//! The same `with_retry_policy(..)` is available on the async builder
//! (`FalkorClientBuilder::new_async()`).
//!
//! **Write safety.** The only scope available today, `RetryScope::ReadOnly`, retries **read-only /
//! idempotent** operations only (`ro_query`, `explain`, `list_indices`, `list_constraints`, read-only
//! procedure calls). **Writes are never retried**, so enabling a policy can never duplicate a write.
//! Classification is by the API you call, never by inspecting Cypher — `query()` is treated as a write
//! even when it only reads, so use `ro_query()` for retryable reads.
//!
//! Only transient connection errors are retried (a dropped/unavailable connection, or a Sentinel
//! resolution failure); deterministic errors (syntax, constraint violations, parse/type errors,
//! wait-operation timeouts) are returned immediately. Retries compose with the client's existing
//! connection healing: each attempt re-borrows a connection, so a recovered connection is picked up on
//! the next try.
//!
//! **Scope.** Retry currently wraps query and procedure *execution* — `ro_query`, `query`, `explain`,
//! `profile`, `call_procedure`/`call_procedure_ro`, and `list_indices`/`list_constraints` (only the
//! read-only ones are eligible). Direct client/admin calls (`list_graphs`, configuration getters/setters,
//! `slowlog`, server `INFO`) and the internal schema-cache refresh that can run while a result is parsed
//! are **not** wrapped yet, so a transient failure there still surfaces even with a policy enabled.
//! Broadening the coverage is a planned follow-up.
//!
//! See [`examples/retry.rs`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/retry.rs) for a complete, runnable example.
//!
//! #### Tracing
//!
//! This crate fully supports instrumentation using the [`tracing`](https://docs.rs/tracing/latest/tracing/) crate, to use
//! it, simply, enable the `tracing` feature:
//!
//! ```bash
//! cargo add falkordb --features tracing
//! ```
//!
//! Note that different functions use different filtration levels, to avoid spamming your tests, be sure to enable the
//! correct level as you desire it.
//!
//! When the `tracing` feature is enabled, the query- and procedure-execution spans are enriched with
//! structured, low-cardinality fields you can slice and filter on (named after the
//! [OpenTelemetry database conventions](https://opentelemetry.io/docs/specs/semconv/database/) so they
//! map cleanly when exported via `tracing-opentelemetry`):
//!
//! | Field | Example | Meaning |
//! |---|---|---|
//! | `db.system.name` | `falkordb` | constant |
//! | `db.namespace` | `social` | the graph name |
//! | `db.operation.name` | `GRAPH.RO_QUERY` / `db.idx.fulltext.queryNodes` | the command or procedure |
//! | `db.falkordb.read_only` | `true` | whether the operation is read-only |
//! | `db.falkordb.strategy` | `multiplexed` | the active connection strategy |
//! | `db.query.fingerprint` | `a1b2c3d4e5f60718` | a privacy-safe hash of the query *shape* |
//! | `error.type` | `connection_down` | a bounded error kind, recorded on failure |
//! | `db.response.returned_rows` | `42` | rows the server returned (on the outer `execute` span) |
//! | `db.falkordb.server_time_ms` | `1.18` | the server's internal execution time, when reported |
//!
//! **Privacy by default.** The raw query text and parameter values are **never** recorded by default —
//! only the `db.query.fingerprint`, which is a hash of the query with all literals (strings, numbers,
//! `true`/`false`/`null`) redacted, so two queries that differ only in their values share a fingerprint
//! and no value ever enters a span. If you need the raw Cypher for debugging in a trusted environment,
//! opt in explicitly:
//!
//! ```no_run
//! use falkordb::FalkorClientBuilder;
//!
//! # fn doc() -> Result<(), Box<dyn std::error::Error>> {
//! let client = FalkorClientBuilder::new()
//!     .with_query_logging(true) // records `db.query.text`; off by default
//!     .build()?;
//! # let _ = client;
//! # Ok(())
//! # }
//! ```
//!
//! Parameter values supplied via `with_param` are never recorded even when query logging is enabled
//! (they live in the query preamble, not the query text).
//!
//! > **Note:** the async query/procedure futures are deeply nested (retry + instrumentation). If you
//! > `tokio::spawn` them with the `tracing` feature enabled and hit a `recursion limit` /
//! > `overflow evaluating ... Send` error, add `#![recursion_limit = "256"]` to your crate root — the
//! > standard fix for deep `async` + `tracing` stacks.
//!
//! See [`examples/observability.rs`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/observability.rs) for a complete, runnable example.
//!
//! #### Metrics
//!
//! Enable the `metrics` feature to emit counters and histograms through the
//! [`metrics`](https://docs.rs/metrics/latest/metrics/) facade, so your application can install any
//! exporter (Prometheus, OpenTelemetry, …):
//!
//! ```bash
//! cargo add falkordb --features metrics
//! ```
//!
//! Each query and procedure execution records:
//!
//! | Metric | Type | Labels |
//! |---|---|---|
//! | `falkordb_queries_total` | counter | `command`, `operation` (`read`/`write`), `strategy` |
//! | `falkordb_query_duration_seconds` | histogram | `command`, `operation` |
//! | `falkordb_query_errors_total` | counter | `command`, `error_kind` |
//! | `falkordb_retries_total` | counter | `operation`, `error_kind` |
//! | `falkordb_connections_in_flight` | gauge | `route` (`primary`/`replica`) |
//! | `falkordb_connection_pool_wait_seconds` | histogram | `route` (pooled strategy only) |
//!
//! All labels are **bounded, low-cardinality** values: `command` is an allowlist of known commands
//! (unknown ⇒ `other`), `operation`/`strategy`/`error_kind` are small fixed sets. The graph name, query
//! text, and query fingerprint are **never** used as labels (they are unbounded and would explode metric
//! cardinality) — those belong on `tracing` spans, not metrics. Like `tracing`, recording is a no-op
//! until you install a recorder; for example, with `metrics-exporter-prometheus`:
//!
//! ```ignore
//! let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
//! builder.install().expect("failed to install Prometheus recorder");
//! // ... use the client; metrics are now exported on the configured endpoint.
//! ```
//!
//! #### Actionable error hints
//!
//! `FalkorDBError::mitigation_hint()` turns common, recognizable failures into a short, actionable
//! remediation tip — handy for logs and AI tooling. It is purely additive: the raw error and its
//! `Display`/`Debug` output are unchanged, hints are fixed `&'static str`s (so they never echo text from
//! the underlying message), and unrecognized errors return `None`.
//!
//! ```no_run
//! use falkordb::FalkorDBError;
//!
//! let err = FalkorDBError::ConnectionDown;
//! if let Some(hint) = err.mitigation_hint() {
//!     println!("hint: {hint}");
//! }
//! ```
//!
//! ### Embedded server
//!
//! This client supports running an embedded FalkorDB server, which is useful for:
//! - Testing without external dependencies
//! - Embedded applications
//! - Quick prototyping and development
//!
//! To use the embedded feature, enable it:
//!
//! ```bash
//! cargo add falkordb --features embedded
//! ```
//!
//! #### Choosing a module-provisioning mode
//!
//! The `redis-server` binary is **never** downloaded — it must be installed on the host (see
//! Requirements). Only the FalkorDB `falkordb.so` **module** is provisioned, and there are two
//! features for that:
//!
//! - **`embedded`** — *runtime download*. The module is downloaded on first start (and cached), so
//!   the running process **needs network access** the first time. Best for development.
//! - **`embedded-bundle`** — *build-time embed, offline at runtime*. A `build.rs` fetches the module
//!   for the build target at **compile time** and embeds it in your binary, so the running process
//!   needs **no network at all**. Best for network-isolated deployments. Enable it instead of
//!   `embedded`:
//!
//!   ```bash
//!   cargo add falkordb --features embedded-bundle
//!   ```
//!
//!   Control the bundled module at build time with environment variables:
//!   `FALKORDB_EMBEDDED_MODULE_VERSION` (release tag; defaults to the pinned version),
//!   `FALKORDB_EMBEDDED_MODULE_PLATFORM` (override the asset for distro-specific Linux targets such
//!   as `rhel9-x64`), and `FALKORDB_EMBEDDED_MODULE_PATH` to embed a **local** `.so` instead of
//!   downloading (fully offline builds, or unsupported platforms). A non-default version must be
//!   accompanied by `FALKORDB_EMBEDDED_MODULE_SHA256` — unchecked downloaded native code is never
//!   embedded. The downloading build uses the host `curl` (set `FALKORDB_EMBEDDED_MODULE_PATH` on
//!   build hosts without `curl` or network access); the `embedded-bundle` runtime itself carries no
//!   HTTP/hashing dependencies.
//!
//!   > **License:** `embedded-bundle` embeds the SSPL-licensed FalkorDB module into your binary, so
//!   > you are responsible for complying with its license when you distribute that binary.
//!
//! #### Requirements
//!
//! - `redis-server` (**version 8.0 or newer**) must be installed and available in PATH (or you can
//!   specify a custom path). It is **not** downloaded automatically — install it from your package
//!   manager (e.g. `brew install redis`, `apt-get install redis-server`).
//! - The `falkordb.so` module is provisioned automatically: downloaded at runtime with `embedded`
//!   (when `auto_download` is enabled, the default) or embedded at build time with
//!   `embedded-bundle`. You can also point `falkordb_module_path` at an existing module, or disable
//!   `auto_download` to use only explicit/system-installed binaries.
//! - On macOS the module requires OpenMP: `brew install libomp`.
//!
//! Supported platforms: Linux x86_64/aarch64 (glibc and musl/Alpine, plus
//! RHEL 8/9 and Amazon Linux 2023 on x86_64) and macOS aarch64 (Apple Silicon).
//!
//! #### Self-contained vs. already-installed
//!
//! ```no_run
//! use falkordb::EmbeddedConfig;
//! use std::path::PathBuf;
//!
//! // Self-contained (default): download + cache the module if it is missing.
//! let _auto = EmbeddedConfig::default();
//!
//! // Offline: use only binaries already on the machine (no network access).
//! let _offline = EmbeddedConfig {
//!     auto_download: false,
//!     falkordb_module_path: Some(PathBuf::from("/usr/lib/redis/modules/falkordb.so")),
//!     ..Default::default()
//! };
//! ```
//!
//! The cache directory defaults to `~/.cache/falkordb-rs` (Linux) or
//! `~/Library/Caches/falkordb-rs` (macOS) and can be overridden with the
//! `cache_dir` field or the `FALKORDB_RS_CACHE_DIR` environment variable.
//!
//! #### Usage Example
//!
//! ```no_run
//! use falkordb::{EmbeddedConfig, FalkorClientBuilder, FalkorConnectionInfo};
//!
//! // Create an embedded configuration with defaults
//! let embedded_config = EmbeddedConfig::default();
//!
//! // Or customize the configuration:
//! // let embedded_config = EmbeddedConfig {
//! //     redis_server_path: Some(PathBuf::from("/path/to/redis-server")),
//! //     falkordb_module_path: Some(PathBuf::from("/path/to/falkordb.so")),
//! //     db_dir: Some(PathBuf::from("/tmp/my_falkordb")),
//! //     falkordb_version: None, // pin a different release, e.g. Some("v4.18.10".into())
//! //     cache_dir: None,        // override the download cache location
//! //     ..Default::default()
//! // };
//!
//! // Build a client with embedded FalkorDB
//! let client = FalkorClientBuilder::new()
//!     .with_connection_info(FalkorConnectionInfo::Embedded(embedded_config))
//!     .build()
//!     .expect("Failed to build client");
//!
//! // Use the client normally
//! let mut graph = client.select_graph("social");
//! graph.query("CREATE (:Person {name: 'Alice', age: 30})").execute().expect("Failed to execute query");
//!
//! // The embedded server will be automatically shut down when the client is dropped
//! ```
//!
//! The embedded server:
//! - Spawns a `redis-server` process with the FalkorDB module loaded
//! - Uses Unix socket for communication (no network port)
//! - Automatically cleans up when the client is dropped
//! - Can be configured with custom paths, database directory, and socket location
//!
//! ## Examples
//!
//! Every example is a runnable file under [`examples/`](https://github.com/FalkorDB/falkordb-rs/tree/main/examples) and is compiled in CI.
//! Run one with `cargo run` plus the flags shown:
//!
//! | Example | Shows | Run with |
//! |---|---|---|
//! | [`basic_usage`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/basic_usage.rs) | A minimal connect, query, and iterate flow | `--example basic_usage` |
//! | [`rows`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/rows.rs) | Header-aware rows: read columns by name or index with strict typed access | `--example rows` |
//! | [`typed_params`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/typed_params.rs) | Type-safe, injection-proof query parameters | `--example typed_params` |
//! | [`typed_mapping`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/typed_mapping.rs) | Map query results into your own `serde` types | `--features serde --example typed_mapping` |
//! | [`temporal`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/temporal.rs) | Decode temporal values and use the type-safe `DateTime`/`Duration` algebra | `--example temporal` |
//! | [`vector_index`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/vector_index.rs) | Create vector indexes with the typed helpers and `VectorSimilarity` | `--example vector_index` |
//! | [`batch`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/batch.rs) | Batch / pipelined execution: many queries in one round-trip | `--example batch` |
//! | [`waiting_ops`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/waiting_ops.rs) | Wait for background index / constraint / copy operations to take effect | `--example waiting_ops` |
//! | [`udf_usage`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/udf_usage.rs) | Load a user-defined-function (UDF) library | `--example udf_usage` |
//! | [`async_api`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/async_api.rs) | The async (`tokio`) client end to end | `--features tokio --example async_api` |
//! | [`async_stream`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/async_stream.rs) | Async streaming with `futures` combinators | `--features tokio --example async_stream` |
//! | [`multiplexed_async`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/multiplexed_async.rs) | The multiplexed async connection strategy | `--features tokio --example multiplexed_async` |
//! | [`readonly_replica`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/readonly_replica.rs) | Route read-only queries to replica nodes | `--example readonly_replica` |
//! | [`retry`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/retry.rs) | The opt-in retry policy for transient failures | `--example retry` |
//! | [`tls`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/tls.rs) | Connect to FalkorDB over TLS | `--features rustls --example tls` |
//! | [`observability`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/observability.rs) | `tracing` span enrichment and the query fingerprint | `--features tracing --example observability` |
//! | [`embedded_usage`](https://github.com/FalkorDB/falkordb-rs/blob/main/examples/embedded_usage.rs) | Run an embedded FalkorDB server | `--features embedded --example embedded_usage` |
//!
//! ## API documentation
//!
//! The complete API reference is published on [docs.rs](https://docs.rs/falkordb/latest/falkordb/).
//!
//! ## Migration guides
//!
//! - [Migrating to 0.7](https://github.com/FalkorDB/falkordb-rs/blob/main/docs/migrating-to-0.7.md)
//! - [Migrating to 0.8](https://github.com/FalkorDB/falkordb-rs/blob/main/docs/migrating-to-0.8.md)
//! - [Migrating to 0.10](https://github.com/FalkorDB/falkordb-rs/blob/main/docs/migrating-to-0.10.md)
//!
//! ## Contributing
//!
//! Development setup, the full `just` recipe reference, and how to run the tests and benchmarks live in
//! [`CONTRIBUTING.md`](https://github.com/FalkorDB/falkordb-rs/blob/main/CONTRIBUTING.md).
//!
//! ## Community and license
//!
//! - [GitHub Discussions](https://github.com/orgs/FalkorDB/discussions)
//! - [Discord](https://discord.com/invite/6M4QwDXn2w)
//! - [FalkorDB Cloud](https://app.falkordb.cloud)
//!
//! Licensed under the [MIT License](https://github.com/FalkorDB/falkordb-rs/blob/main/LICENSE).

mod client;
mod connection;
mod connection_info;
#[cfg(feature = "embedded-core")]
mod embedded;
mod error;
mod graph;
mod graph_schema;
#[cfg(any(feature = "tracing", feature = "metrics"))]
mod observability;
mod parser;
mod response;
mod retry;
mod value;

/// A [`Result`] which only returns [`FalkorDBError`] as its E type
pub type FalkorResult<T> = Result<T, FalkorDBError>;

pub use client::{
    blocking::FalkorSyncClient, builder::FalkorClientBuilder, ConnectionStrategy, ReadPreference,
};
pub use connection_info::FalkorConnectionInfo;
pub use error::FalkorDBError;
pub use graph::{
    batch::{BatchBuilder, BatchItemResult, BatchQuery, BatchResult},
    blocking::SyncGraph,
    ops::{ConstraintOpBuilder, CopyGraphBuilder, IndexOpBuilder, WaitOperation, WaitOptions},
    query_builder::{ProcedureQueryBuilder, QueryBuilder},
    VectorSimilarity,
};
pub use graph_schema::{GraphSchema, SchemaType};
pub use response::{
    constraint::{Constraint, ConstraintStatus, ConstraintType},
    execution_plan::ExecutionPlan,
    index::{FalkorIndex, IndexStatus, IndexType},
    lazy_result_set::LazyResultSet,
    row::Row,
    slowlog_entry::SlowlogEntry,
    QueryResult,
};
pub use retry::{Backoff, RetryPolicy, RetryScope};
pub use value::{
    config::ConfigValue,
    graph_entities::{Edge, EntityType, Node},
    path::Path,
    point::Point,
    temporal::{Date, DateTime, Duration, Seconds, Time},
    to_cypher_param, FalkorParams, FalkorValue, FromFalkorValue, IntoFalkorParam, IntoFalkorParams,
    RawParam,
};

#[cfg(feature = "tokio")]
pub use response::row_stream::RowStream;
#[cfg(feature = "serde")]
pub use response::typed_result_set::TypedLazyResultSet;
#[cfg(all(feature = "serde", feature = "tokio"))]
pub use response::typed_row_stream::TypedRowStream;
#[cfg(feature = "serde")]
pub use value::{from_falkor_row, from_falkor_value, FalkorValueDeserializer};

#[cfg(feature = "tokio")]
pub use client::asynchronous::FalkorAsyncClient;
#[cfg(feature = "tokio")]
pub use graph::asynchronous::AsyncGraph;
#[cfg(feature = "tokio")]
pub use graph::ops::{AsyncConstraintOpBuilder, AsyncCopyGraphBuilder, AsyncIndexOpBuilder};

#[cfg(feature = "embedded-core")]
pub use embedded::{EmbeddedConfig, EmbeddedServer};

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

    pub(crate) fn create_test_client() -> FalkorSyncClient {
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

    /// Name of the shared, read-only graph fixture loaded by
    /// `resources/populate_graph.py`.
    pub(crate) const IMDB_FIXTURE_GRAPH: &str = "imdb";

    /// Shown when the shared `imdb` fixture is missing, so that a fixture problem is never
    /// mistaken for a code regression introduced by a change.
    const IMDB_FIXTURE_HINT: &str = "the shared `imdb` test graph is empty or missing. Run \
        `resources/populate_graph.py` against the test server first (CI does this in the \
        coverage job's \"Populate test graph\" step). This indicates a missing test fixture, \
        NOT a code regression.";

    /// Number of `actor` nodes in the shared `imdb` fixture.
    ///
    /// A query or parsing failure here is a real connectivity/code problem (not a missing
    /// fixture), so it panics with the underlying error rather than being collapsed into a
    /// zero count. Only an actual count of `0` indicates an empty/missing fixture, which the
    /// caller turns into the [`IMDB_FIXTURE_HINT`] message.
    fn imdb_actor_count(graph: &mut SyncGraph) -> i64 {
        let mut result = graph
            .ro_query("MATCH (a:actor) RETURN count(a)")
            .execute()
            .expect("failed to query the imdb actor count (connection or query regression)");
        result
            .data
            .next()
            .expect("imdb actor count query returned no rows")
            .expect("imdb actor count row failed to parse")
            .try_get_at::<i64>(0)
            .expect("imdb actor count column was not an i64")
    }

    /// Create a sync client and assert the shared `imdb` fixture is populated, so every
    /// fixture-dependent test fails fast with an actionable message (rather than a cryptic
    /// assertion or a vacuous pass on an empty graph) when the fixture is missing.
    pub(crate) fn imdb_test_client() -> FalkorSyncClient {
        let client = create_test_client();
        let mut graph = client.select_graph(IMDB_FIXTURE_GRAPH);
        assert!(imdb_actor_count(&mut graph) > 0, "{IMDB_FIXTURE_HINT}");
        client
    }

    /// Async counterpart of [`imdb_test_client`].
    #[cfg(feature = "tokio")]
    pub(crate) async fn imdb_async_test_client() -> FalkorAsyncClient {
        use futures::StreamExt;
        let client = create_async_test_client().await;
        let mut graph = client.select_graph(IMDB_FIXTURE_GRAPH);
        let mut result = graph
            .ro_query("MATCH (a:actor) RETURN count(a)")
            .execute()
            .await
            .expect("failed to query the imdb actor count (connection or query regression)");
        let count = result
            .data
            .next()
            .await
            .expect("imdb actor count query returned no rows")
            .expect("imdb actor count row failed to parse")
            .try_get_at::<i64>(0)
            .expect("imdb actor count column was not an i64");
        assert!(count > 0, "{IMDB_FIXTURE_HINT}");
        client
    }

    pub(crate) fn open_empty_test_graph(graph_name: &str) -> TestSyncGraphHandle {
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

    const RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
    /// `GRAPH.COPY` is performed by a background fork on the server, which can take
    /// noticeably longer than index/constraint readiness under heavy load, so its
    /// re-issue loop is given a longer window than [`RETRY_TIMEOUT`].
    pub(crate) const COPY_RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// FalkorDB builds indices and validates constraints asynchronously, so a freshly
    /// created index/constraint may not be reported by the matching `list_*` call that
    /// immediately follows creation, especially while the server is under load. Retry
    /// `op` until `done` is satisfied or the timeout elapses, returning the last value.
    pub(crate) fn retry_until<T>(
        op: impl FnMut() -> T,
        done: impl Fn(&T) -> bool,
    ) -> T {
        retry_until_with_timeout(RETRY_TIMEOUT, op, done)
    }

    /// [`retry_until`] with an explicit overall timeout, for operations that need a
    /// different retry window than the shared default (e.g. the `GRAPH.COPY` re-issue
    /// loop, which uses [`COPY_RETRY_TIMEOUT`]).
    pub(crate) fn retry_until_with_timeout<T>(
        timeout: std::time::Duration,
        mut op: impl FnMut() -> T,
        done: impl Fn(&T) -> bool,
    ) -> T {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let value = op();
            if done(&value) || std::time::Instant::now() >= deadline {
                return value;
            }
            std::thread::sleep(RETRY_INTERVAL);
        }
    }

    /// Async counterpart of [`retry_until`]. Written with an explicit
    /// `FnMut(&mut AsyncGraph) -> Pin<Box<dyn Future>>` bound (rather than an async
    /// closure / `AsyncFnMut`) so it compiles on all stable toolchains.
    #[cfg(feature = "tokio")]
    pub(crate) async fn retry_until_async<T>(
        graph: &mut AsyncGraph,
        mut op: impl for<'a> FnMut(
            &'a mut AsyncGraph,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + 'a>>,
        done: impl Fn(&T) -> bool,
    ) -> T {
        let deadline = std::time::Instant::now() + RETRY_TIMEOUT;
        loop {
            let value = op(graph).await;
            if done(&value) || std::time::Instant::now() >= deadline {
                return value;
            }
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
    }

    /// Async counterpart of [`retry_until`] for self-contained operations that do
    /// not borrow a long-lived `&mut AsyncGraph` (so the future can capture
    /// everything it needs, e.g. a shared `&FalkorAsyncClient`). Used for server
    /// operations performed by a background fork (e.g. `GRAPH.COPY`) that must be
    /// re-issued until they take effect.
    #[cfg(feature = "tokio")]
    pub(crate) async fn retry_until_async_fn<T, Fut>(
        op: impl FnMut() -> Fut,
        done: impl Fn(&T) -> bool,
    ) -> T
    where
        Fut: std::future::Future<Output = T>,
    {
        retry_until_async_fn_with_timeout(RETRY_TIMEOUT, op, done).await
    }

    /// [`retry_until_async_fn`] with an explicit overall timeout, for operations that
    /// need a different retry window than the shared default (e.g. the `GRAPH.COPY`
    /// re-issue loop, which uses [`COPY_RETRY_TIMEOUT`]).
    #[cfg(feature = "tokio")]
    pub(crate) async fn retry_until_async_fn_with_timeout<T, Fut>(
        timeout: std::time::Duration,
        mut op: impl FnMut() -> Fut,
        done: impl Fn(&T) -> bool,
    ) -> T
    where
        Fut: std::future::Future<Output = T>,
    {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let value = op().await;
            if done(&value) || std::time::Instant::now() >= deadline {
                return value;
            }
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
    }

    /// Classifies a copy-retry attempt: pass an `Ok` value through, return the default (e.g. an
    /// empty `Vec`, so the caller's predicate treats it as a miss and re-issues) on a transient
    /// [`FalkorDBError::ConnectionDown`] — the client has already swapped in a fresh connection — and
    /// panic immediately on any other error so a genuine regression fails fast with context instead
    /// of being masked until the retry timeout.
    pub(crate) fn default_on_connection_down<T: Default>(
        context: &str,
        result: FalkorResult<T>,
    ) -> T {
        match result {
            Ok(value) => value,
            Err(FalkorDBError::ConnectionDown) => T::default(),
            Err(err) => panic!("{context}: {err}"),
        }
    }

    /// Awaits a `copy_graph(...)`/`copy_graph_op(...).wait()` future and reads the copied graph's
    /// actors. Shared by the async copy tests so the (necessarily `?`-based) async error plumbing
    /// lives in one place.
    #[cfg(feature = "tokio")]
    pub(crate) async fn read_copied_actors(
        copy: impl std::future::Future<Output = FalkorResult<AsyncGraph>>
    ) -> FalkorResult<Vec<FalkorResult<Row>>> {
        use futures::StreamExt as _;
        let mut graph = copy.await?;
        Ok(graph
            .query("MATCH (a:actor) RETURN a")
            .execute()
            .await?
            .data
            .collect::<Vec<_>>()
            .await)
    }
}

#[cfg(test)]
mod retry_tests {
    use super::test_utils::retry_until;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn retry_until_returns_immediately_when_already_done() {
        let calls = AtomicUsize::new(0);
        let value = retry_until(|| calls.fetch_add(1, Ordering::Relaxed) + 1, |v| *v == 1);
        assert_eq!(value, 1);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn retry_until_polls_until_condition_is_met() {
        let calls = AtomicUsize::new(0);
        let value = retry_until(|| calls.fetch_add(1, Ordering::Relaxed) + 1, |v| *v == 3);
        assert_eq!(value, 3);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread")]
    async fn retry_until_async_fn_polls_until_condition_is_met() {
        use super::test_utils::retry_until_async_fn;
        let calls = AtomicUsize::new(0);
        let value = retry_until_async_fn(
            || async { calls.fetch_add(1, Ordering::Relaxed) + 1 },
            |v| *v == 3,
        )
        .await;
        assert_eq!(value, 3);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn default_on_connection_down_passes_ok_through() {
        use super::test_utils::default_on_connection_down;
        let value = default_on_connection_down("ctx", Ok::<_, crate::FalkorDBError>(vec![1, 2, 3]));
        assert_eq!(value, vec![1, 2, 3]);
    }

    #[test]
    fn default_on_connection_down_returns_default_on_connection_down() {
        use super::test_utils::default_on_connection_down;
        let value: Vec<i32> =
            default_on_connection_down("ctx", Err(crate::FalkorDBError::ConnectionDown));
        assert!(value.is_empty());
    }

    #[test]
    #[should_panic(expected = "could not copy")]
    fn default_on_connection_down_panics_on_other_error() {
        use super::test_utils::default_on_connection_down;
        let _: Vec<i32> =
            default_on_connection_down("could not copy", Err(crate::FalkorDBError::ParsingArray));
    }
}
