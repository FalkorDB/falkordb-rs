[![Release](https://img.shields.io/github/release/falkordb/falkordb-rs.svg)](https://github.com/falkordb/falkordb-rs/releases/latest)
[![crates.io](https://img.shields.io/crates/dr/falkordb)](https://crates.io/crates/falkordb)
[![license](https://img.shields.io/crates/l/falkordb)](https://github.com/FalkorDB/falkordb-rs?tab=License-1-ov-file)\
[![GitHub Issues or Pull Requests](https://img.shields.io/github/issues/falkordb/falkordb-rs)](https://github.com/FalkorDB/falkordb-rs/issues)
[![Pipeline](https://img.shields.io/github/actions/workflow/status/falkordb/falkordb-rs/main.yml)](https://github.com/FalkorDB/falkordb-rs)
[![Codecov](https://codecov.io/gh/falkordb/falkordb-rs/branch/main/graph/badge.svg)](https://codecov.io/gh/falkordb/falkordb-rs)
[![Docs](https://img.shields.io/docsrs/falkordb)](https://docs.rs/falkordb/latest/falkordb/)\
[![Forum](https://img.shields.io/badge/Forum-falkordb-blue)](https://github.com/orgs/FalkorDB/discussions)
[![Discord](https://img.shields.io/discord/1146782921294884966?style=flat-square)](https://discord.com/invite/6M4QwDXn2w)

# falkordb-rs

[![Try Free](https://img.shields.io/badge/Try%20Free-FalkorDB%20Cloud-FF8101?labelColor=FDE900&style=for-the-badge&link=https://app.falkordb.cloud)](https://app.falkordb.cloud)

### FalkorDB Rust client

## Usage

### Installation

Just add it to your `Cargo.toml`, like so:

```toml
falkordb = { version = "0.3.0" }
```

### Run FalkorDB instance

Docker:

```sh
docker run --rm -p 6379:6379 falkordb/falkordb
```

### Code Example

```rust,no_run
use falkordb::{FalkorClientBuilder, FalkorConnectionInfo};

// Connect to FalkorDB
let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()
            .expect("Invalid connection info");

let client = FalkorClientBuilder::new()
           .with_connection_info(connection_info)
           .build()
           .expect("Failed to build client");

// Select the social graph
let mut graph = client.select_graph("social");

// Create 100 nodes and return a handful
let mut nodes = graph.query("UNWIND range(0, 100) AS i CREATE (n { v:1 }) RETURN n LIMIT 10")
            .with_timeout(5000)
            .execute()
            .expect("Failed executing query");

// Can also be collected, like any other iterator
while let Some(node) = nodes.data.next() {
   println ! ("{:?}", node);
}
```

## Features

### Waiting for background operations

Some FalkorDB operations finish **after** the command that starts them returns: when you create or
drop an index or constraint, the request returns immediately while the index is populated (or the
constraint is enforced) on a background worker thread, and `GRAPH.COPY` can fail transiently while
the server is unable to `fork`. The eager methods
(`create_index`, `create_unique_constraint`, `copy_graph`, …) stay fire-and-forget, but every
one of them now has an additive `*_op` builder that adds explicit, opt-in waiting while keeping
full backward compatibility.

Each builder offers `.execute()` (non-blocking, identical to the eager method) and `.wait()` /
`.wait_with(WaitOptions)` terminals. For index and constraint builders, `.wait()` blocks until the
operation has actually taken effect (the index/constraint becomes operational or is dropped),
returning [`FalkorDBError::Timeout`] if it does not happen in time. For the copy builder, `GRAPH.COPY`
is already blocking on the server, so `.wait()` simply retries transient `could not fork` failures
with backoff; it does **not** verify the copied contents (that remains the caller's responsibility).

```rust,no_run
use falkordb::{EntityType, FalkorClientBuilder, FalkorConnectionInfo, IndexType, WaitOptions};
use std::time::Duration;

let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()
            .expect("Invalid connection info");
let client = FalkorClientBuilder::new()
           .with_connection_info(connection_info)
           .build()
           .expect("Failed to build client");
let mut graph = client.select_graph("social");

// Fire-and-forget, exactly like `create_index` (returns as soon as the server accepts it):
graph.create_index_op(IndexType::Range, EntityType::Node, "Person", &["age"], None)
     .execute()
     .expect("Failed to request index creation");

// Block until the index is actually operational (default 30s readiness timeout):
graph.create_index_op(IndexType::Range, EntityType::Node, "Person", &["name"], None)
     .wait()
     .expect("Index did not become operational");

// A unique constraint reports a *distinct* error if existing data violates it:
match graph.create_unique_constraint_op(EntityType::Node, "Person", &["email"])
           .wait_with(WaitOptions::with_timeout(Duration::from_secs(10)))
{
    Ok(()) => println!("constraint is enforced"),
    Err(falkordb::FalkorDBError::ConstraintFailed { .. }) => println!("data violates the constraint"),
    Err(other) => panic!("unexpected error: {other}"),
}

// Copy a graph, retrying transient `could not fork` failures:
let _copy = client.copy_graph_op("social", "social_backup")
                  .wait()
                  .expect("Failed to copy graph");
```

The same builders exist on the async client/graph; just `await` the terminals:

```rust,ignore
use falkordb::{EntityType, FalkorClientBuilder, FalkorConnectionInfo, IndexType};

let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()
            .expect("Invalid connection info");
let client = FalkorClientBuilder::new_async()
           .with_connection_info(connection_info)
           .build()
           .await
           .expect("Failed to build client");
let mut graph = client.select_graph("social");

graph.create_index_op(IndexType::Range, EntityType::Node, "Person", &["name"], None)
     .wait()
     .await
     .expect("Index did not become operational");
```

[`FalkorDBError::Timeout`]: https://docs.rs/falkordb/latest/falkordb/enum.FalkorDBError.html


### `tokio` support

This client supports nonblocking API using the [`tokio`](https://tokio.rs/) runtime.
It can be enabled like so:

```toml
falkordb = { version = "0.3.0", features = ["tokio"] }
```

Currently, this API requires running within a [
`multi_threaded tokio scheduler`](https://docs.rs/tokio/latest/tokio/runtime/index.html#multi-thread-scheduler), and
does not support the `current_thread` one, but this will probably be supported in the future.

The API uses an almost identical API, but the various functions need to be awaited:

```rust,ignore
use falkordb::{FalkorClientBuilder, FalkorConnectionInfo};

// Connect to FalkorDB
let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()
            .expect("Invalid connection info");

let client = FalkorClientBuilder::new_async()
            .with_connection_info(connection_info)
            .build()
            .await
            .expect("Failed to build client");

// Select the social graph
let mut graph = client.select_graph("social");

// Create 100 nodes and return a handful
let mut nodes = graph.query("UNWIND range(0, 100) AS i CREATE (n { v:1 }) RETURN n LIMIT 10")
            .with_timeout(5000)
            .execute()
            .await
            .expect("Failed executing query");

// Graph operations are asynchronous, but parsing is still concurrent:
while let Some(node) = nodes.data.next() {
     println ! ("{:?}", node);
}
```

Note that thread safety is still up to the user to ensure, I.e. an `AsyncGraph` cannot simply be sent to a task spawned
by tokio and expected to be used later,
it must be wrapped in an Arc<Mutex<_>> or something similar.

### Connection Strategy and Multiplexing

The asynchronous client chooses how it manages its underlying Redis connections via a
`ConnectionStrategy`:

- **`Multiplexed`** (the async default): a small number of shared, cloneable,
  auto-reconnecting connections. Many concurrent commands are pipelined over each socket,
  so a single connection can carry many in-flight requests at once. This avoids the
  borrow/return bottleneck and is the most efficient option for highly concurrent
  workloads.
- **`Pooled`**: a fixed pool of independent connections, each used by exactly one command
  at a time (borrow/return). This gives strict per-command isolation and a natural cap on
  in-flight commands. It is the only strategy for the synchronous client.

Select or tune the strategy on the builder:

```rust,no_run
use falkordb::{ConnectionStrategy, FalkorClientBuilder};
use std::num::NonZeroU8;

# async fn doc() {
// Spread commands across 4 shared multiplexed sockets (the default uses 8).
let client = FalkorClientBuilder::new_async()
    .with_connection_strategy(ConnectionStrategy::Multiplexed {
        connections: NonZeroU8::new(4).unwrap(),
    })
    // Optional backpressure: cap concurrently in-flight commands per socket.
    .with_max_inflight(std::num::NonZeroUsize::new(256).unwrap())
    .build()
    .await
    .expect("Failed to build client");

assert_eq!(client.connection_pool_size(), 4);
# }
```

Notes and caveats:

- **Behavior change:** the async default is now multiplexed (previously an exclusive
  borrow-pool). The API is source-compatible; `with_num_connections` now sets the number
  of underlying connections/sockets for the active strategy, and `connection_pool_size()`
  reports that count.
- **Backpressure:** multiplexed mode does not bound the number of outstanding requests
  unless you set `with_max_inflight(n)` (where `n` is a `NonZeroUsize`; ignored by the
  pooled strategy, whose pool size already caps in-flight commands).
- **Sentinel:** a multiplexed connection built from a Sentinel-resolved node would not
  re-resolve the master/replica on failover, so for Sentinel deployments the client
  transparently falls back to the pooled strategy (which re-resolves on reconnect).
  `connection_strategy()` returns this *effective* strategy.

A runnable example is provided in [`examples/multiplexed_async.rs`](examples/multiplexed_async.rs).

### SSL/TLS Support

This client is currently built upon the [`redis`](https://docs.rs/redis/latest/redis/) crate, and therefore supports TLS
using
its implementation, which uses either [`rustls`](https://docs.rs/rustls/latest/rustls/) or [
`native_tls`](https://docs.rs/native-tls/latest/native_tls/).
This is not enabled by default, and the user just opt-in by enabling the respective features: `"rustls"`/`"native-tls"` (
when using tokio: `"tokio-rustls"`/`"tokio-native-tls"`).

For Rustls:

```toml
falkordb = { version = "0.3.0", features = ["rustls"] }
```

```toml
falkordb = { version = "0.3.0", features = ["tokio-rustls"] }
```

For Native TLS:

```toml
falkordb = { version = "0.3.0", features = ["native-tls"] }
```

```toml
falkordb = { version = "0.3.0", features = ["tokio-native-tls"] }
```

### TCP Keepalive

Long-lived clients behind NATs, stateful firewalls, or idle-timeout-enforcing
proxies can silently lose their TCP sessions. The builder exposes TCP-level
socket settings to prevent this:

```rust,no_run
use falkordb::FalkorClientBuilder;
use std::time::Duration;

// Convenience: just enable keepalive with a 30-second idle timeout
let client = FalkorClientBuilder::new()
    .with_tcp_keepalive(Duration::from_secs(30))
    .build()
    .expect("Failed to build client");

// Or full control via redis::io::tcp::TcpSettings
let settings = redis::io::tcp::TcpSettings::default()
    .set_nodelay(true)
    .set_keepalive(
        redis::io::tcp::socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(60)),
    );
let client = FalkorClientBuilder::new()
    .with_tcp_settings(settings)
    .build()
    .expect("Failed to build client");
```

> **Note:** TCP settings apply to direct Redis TCP connections only.
> Unix-domain socket / embedded connections and the Sentinel connection path are
> not affected.

### Read-only Queries and Replica Routing

Read-only queries (`ro_query` and `call_procedure_ro`) can be served from
replica nodes, taking read load off the primary. When the client connects to a
Redis Sentinel deployment that exposes readable replicas, it automatically
builds a dedicated read-only connection pool that routes those queries to a
replica. Writes always go to the primary.

> **Connection pool sizing:** When readable replicas are present the client opens
> a second pool of up to `num_connections` additional connections (one per slot)
> alongside the primary pool. Size your pool limits and file-descriptor limits
> accordingly.

```rust,no_run
use falkordb::FalkorClientBuilder;

let client = FalkorClientBuilder::new()
    // A Sentinel endpoint, e.g. falkor://127.0.0.1:26379
    .with_connection_info("falkor://127.0.0.1:26379".try_into().expect("Invalid connection info"))
    .build()
    .expect("Failed to build client");

// `true` only when readable replicas are available.
if client.reads_from_replicas() {
    println!("Read-only queries are routed to replicas");
}

let mut graph = client.select_graph("imdb");

// Writes go to the primary.
graph.query("CREATE (:Actor {name: 'Tom Hanks'})").execute().expect("Failed to write");

// Read-only queries are served from a replica when one is available.
let mut nodes = graph.ro_query("MATCH (a:Actor) RETURN a.name").execute().expect("Failed to read");
```

This behavior is fully backward compatible: against a single node (or any
deployment without readable replicas), `ro_query` / `call_procedure_ro`
transparently fall back to the primary connection, and `reads_from_replicas()`
returns `false`. See [`examples/readonly_replica.rs`](examples/readonly_replica.rs)
for a complete working example.

### Tracing

This crate fully supports instrumentation using the [`tracing`](https://docs.rs/tracing/latest/tracing/) crate, to use
it, simply, enable the `tracing` feature:

```toml
falkordb = { version = "0.3.0", features = ["tracing"] }
```

Note that different functions use different filtration levels, to avoid spamming your tests, be sure to enable the
correct level as you desire it.

### Typed result mapping (serde)

Enable the optional `serde` feature to map query results straight into your own types instead of hand-matching every
`FalkorValue` variant:

```toml
falkordb = { version = "0.3.0", features = ["serde"] }
```

Derive `serde::Deserialize` on your type and call `FalkorValue::deserialize_into` (or the free function
`falkordb::from_falkor_value`) on a returned value. A node is deserialized from its properties, and scalars, `Option`,
sequences and maps map onto the matching Rust types:

```rust,ignore
use falkordb::{FalkorClientBuilder, FalkorConnectionInfo};
use serde::Deserialize;
#[derive(Debug, Deserialize)]
struct Movie {
    title: String,
    year: i64,
    rating: Option<f64>,
}
let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()
    .expect("Invalid connection info");
let client = FalkorClientBuilder::new()
    .with_connection_info(connection_info)
    .build()
    .expect("Failed to build client");
let mut graph = client.select_graph("imdb");
let mut result = graph.query("MATCH (m:Movie) RETURN m").execute()
    .expect("Failed executing query");
for row in result.data.by_ref() {
    if let Some(node) = row.into_iter().next() {
        let movie: Movie = node.deserialize_into().expect("Failed to map node");
        println!("{} ({})", movie.title, movie.year);
    }
}
```

A runnable version lives in [`examples/typed_mapping.rs`](examples/typed_mapping.rs).

### Embedded FalkorDB Server

This client supports running an embedded FalkorDB server, which is useful for:
- Testing without external dependencies
- Embedded applications
- Quick prototyping and development

To use the embedded feature, enable it in your `Cargo.toml`:

```toml
falkordb = { version = "0.3.0", features = ["embedded"] }
```

#### Requirements

- `redis-server` must be installed and available in PATH (or you can specify a custom path)
- `falkordb.so` module must be installed (or you can specify a custom path)

You can install these from:
- [Redis](https://github.com/redis/redis)
- [FalkorDB](https://github.com/falkordb/falkordb)

#### Usage Example

```rust,no_run
use falkordb::{EmbeddedConfig, FalkorClientBuilder, FalkorConnectionInfo};

// Create an embedded configuration with defaults
let embedded_config = EmbeddedConfig::default();

// Or customize the configuration:
// let embedded_config = EmbeddedConfig {
//     redis_server_path: Some(PathBuf::from("/path/to/redis-server")),
//     falkordb_module_path: Some(PathBuf::from("/path/to/falkordb.so")),
//     db_dir: Some(PathBuf::from("/tmp/my_falkordb")),
//     ..Default::default()
// };

// Build a client with embedded FalkorDB
let client = FalkorClientBuilder::new()
    .with_connection_info(FalkorConnectionInfo::Embedded(embedded_config))
    .build()
    .expect("Failed to build client");

// Use the client normally
let mut graph = client.select_graph("social");
graph.query("CREATE (:Person {name: 'Alice', age: 30})").execute().expect("Failed to execute query");

// The embedded server will be automatically shut down when the client is dropped
```

The embedded server:
- Spawns a `redis-server` process with the FalkorDB module loaded
- Uses Unix socket for communication (no network port)
- Automatically cleans up when the client is dropped
- Can be configured with custom paths, database directory, and socket location

## Development

This repository ships a [`just`](https://github.com/casey/just) file that automates the
whole development cycle — formatting, linting, building, docs, tests, coverage,
benchmarks, the dependency audit and a Dockerized FalkorDB server. It is the recommended
entry point for day-to-day work and mirrors the commands the CI gates run.

Install the runner once with `cargo install just` (or `brew install just`), then list
every available recipe:

```bash
just            # or: just --list
```

### Common recipes

```bash
# Fast inner loop (no server needed): format, lint and build.
just check

# Run every required CI gate locally (no server needed):
# fmt-check, clippy, build, doc, deny.
just ci

# Post-task gate: every CI gate PLUS strict clippy over all targets/features
# (examples, tests, benches). Run this before declaring work done.
just done

# Format / lint / docs individually.
just fmt
just clippy
just doc

# Full validation including the server-backed test suite (manages Docker for you):
# spins up FalkorDB, populates the fixture, runs the suite, tears it down.
just verify
```

### Server-backed recipes

Tests, coverage and benchmarks need a reachable FalkorDB instance. The `db-*` recipes
manage one via Docker, and the `*-local` wrappers do it for you automatically:

```bash
# Manage a FalkorDB container yourself.
just db-up          # start a server (and wait until it is ready)
just db-populate    # load the IMDB fixture graph the lib tests use
just db-down        # stop and remove the container

# Or let a single recipe manage the container lifecycle end-to-end.
just test-local       # start DB, populate, run the full suite, tear down
just coverage-local   # same, but produce Codecov JSON
just bench-local      # start DB, run all benchmarks, tear down
```

Targeted recipes are available too, e.g. `just test-parity`, `just test-embedded`,
`just test-one <filter>`, `just bench-one '<criterion-id>'`, and `just coverage-html`.

The host, port, Docker image and feature set can be overridden on the command line, for
example `just port=6380 test` or `just image=falkordb/falkordb:latest db-up`.

### Reproducing CI locally

The GitHub Actions workflows invoke these same recipes, so a failing CI job can be
reproduced with a single command:

| CI job | Recipe |
| --- | --- |
| `check-fmt` | `just fmt-check` |
| `check-clippy` | `just clippy` |
| `check-build` | `just build` |
| `check-doc` | `just doc` |
| `check-deny` | `just deny` |
| `integration-tests` | `just integration` and `just integration --all-features` |
| `integration-tests-tokio` | `just integration --features tokio` |
| `coverage` | `just coverage` |

Run `just ci` to execute every required no-server gate at once, or `just verify` to also
run the server-backed suite. The integration and coverage recipes need a reachable
FalkorDB instance (use `just db-up` first, or the `*-local` wrappers).

## Testing

### Running Tests

This project includes both unit tests and integration tests.

#### Unit Tests

Unit tests don't require a running FalkorDB instance:

```bash
# Run all unit tests
cargo test --lib

# Run unit tests with embedded feature
cargo test --lib --features embedded
```

#### Integration Tests

Integration tests require a running FalkorDB instance. The easiest way to run them is using Docker:

```bash
# Using the provided script (requires Docker)
./run_integration_tests.sh

# Or manually start FalkorDB and run tests
docker run -d --name falkordb-test -p 6379:6379 falkordb/falkordb:latest
cargo test --test integration_tests

# With async support
cargo test --test integration_tests --features tokio

# Clean up
docker stop falkordb-test && docker rm falkordb-test
```

#### CI Integration Tests

Integration tests are automatically run in GitHub Actions using Docker services. See `.github/workflows/integration-tests.yml` for the CI configuration.

### Benchmarks

The crate ships a [criterion](https://docs.rs/criterion) benchmark,
`benches/async_strategies.rs`, that compares the two async connection strategies
(`Pooled` vs `Multiplexed`) across a range of connection counts (1, 8, 32) and
concurrency levels (1, 8, 64, 256). Benchmarks are developer/PR-time tools and are **not**
part of the required CI gates.

They require a running FalkorDB instance and the `tokio` feature:

```bash
# Start a server (configure with FALKORDB_HOST / FALKORDB_PORT; defaults to 127.0.0.1:6379)
docker run -d --name falkordb-bench -p 6379:6379 falkordb/falkordb:latest

# Run the full benchmark suite
cargo bench --features tokio --bench async_strategies

# Run a single case (criterion accepts a filter on the benchmark id)
cargo bench --features tokio --bench async_strategies -- 'async_read_throughput/multiplexed_8/8'

# Clean up
docker stop falkordb-bench && docker rm falkordb-bench
```

When no server is reachable the benchmark prints a notice and skips its work, so it stays
runnable in serverless CI.

#### Interpreting the results

Each case reports the wall-clock time to complete a batch of `concurrency` read queries,
and the corresponding throughput (`Kelem/s`). criterion writes a full HTML report to
`target/criterion/report/index.html`.

What to expect:

- **At concurrency = 1** the two strategies are close: a single in-flight command cannot
  benefit from multiplexing, so the per-request latency dominates.
- **As concurrency rises (64, 256)** the `multiplexed` strategy should pull ahead of
  `pooled` at the same connection count, because many commands are pipelined over each
  shared socket instead of waiting for an exclusive connection from the pool. The gap is
  largest at low connection counts (e.g. `multiplexed_1` vs `pooled_1`), where the pool
  becomes a hard bottleneck while a single multiplexed socket keeps absorbing work.
- **Higher connection counts narrow the gap**: a large pool (e.g. `pooled_32`) hides much
  of its borrow/return cost, approaching multiplexed throughput at the expense of holding
  more sockets open.

Absolute numbers depend heavily on your hardware, the server, and network latency, so
treat them as relative comparisons between strategies on the *same* machine rather than
portable figures.

#### Memory and CPU usage

`benches/async_strategies.rs` measures wall-clock throughput. A second, non-criterion
harness, `benches/resource_usage.rs`, measures **peak memory (RSS)** and **CPU time**
(user/system) per strategy. Because peak RSS is a process-wide high-water mark that cannot
be reset between iterations, the harness runs each strategy in its own subprocess and
prints a table:

```bash
docker run -d --name falkordb-bench -p 6379:6379 falkordb/falkordb:latest
cargo bench --features tokio --bench resource_usage
docker stop falkordb-bench && docker rm falkordb-bench
```

Example output (numbers are illustrative — they vary by machine and server):

```text
strategy         peak_rss_MiB  cpu_user_ms   cpu_sys_ms      wall_ms    queries/sec
pooled:1               ...
multiplexed:1          ...
...
```

What to expect:

- **Memory:** at the *same* connection count, peak RSS is comparable — both strategies hold
  that many sockets. The real saving is that `multiplexed` sustains high concurrency with
  *far fewer* connections (e.g. `multiplexed:1` vs a large `pooled:N`), and each connection
  carries its own read/write buffers, so cutting connection count cuts RSS.
- **CPU:** `multiplexed` removes the borrow/return machinery — the `mpsc` channel, the
  `Mutex`, and the per-command task spawn the pool uses to return a connection — so it
  generally spends less CPU per request and produces less transient allocation churn.

When no server is reachable the harness prints a notice and exits cleanly, so it stays
runnable in serverless CI.

