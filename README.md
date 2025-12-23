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
falkordb = { version = "0.1.10" }
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

### `tokio` support

This client supports nonblocking API using the [`tokio`](https://tokio.rs/) runtime.
It can be enabled like so:

```toml
falkordb = { version = "0.1.10", features = ["tokio"] }
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

### SSL/TLS Support

This client is currently built upon the [`redis`](https://docs.rs/redis/latest/redis/) crate, and therefore supports TLS
using
its implementation, which uses either [`rustls`](https://docs.rs/rustls/latest/rustls/) or [
`native_tls`](https://docs.rs/native-tls/latest/native_tls/).
This is not enabled by default, and the user just opt-in by enabling the respective features: `"rustls"`/`"native-tls"` (
when using tokio: `"tokio-rustls"`/`"tokio-native-tls"`).

For Rustls:

```toml
falkordb = { version = "0.1.10", features = ["rustls"] }
```

```toml
falkordb = { version = "0.1.10", features = ["tokio-rustls"] }
```

For Native TLS:

```toml
falkordb = { version = "0.1.10", features = ["native-tls"] }
```

```toml
falkordb = { version = "0.1.10", features = ["tokio-native-tls"] }
```

### Tracing

This crate fully supports instrumentation using the [`tracing`](https://docs.rs/tracing/latest/tracing/) crate, to use
it, simply, enable the `tracing` feature:

```toml
falkordb = { version = "0.1.10", features = ["tracing"] }
```

Note that different functions use different filtration levels, to avoid spamming your tests, be sure to enable the
correct level as you desire it.

### Embedded FalkorDB Server

This client supports running an embedded FalkorDB server, which is useful for:
- Testing without external dependencies
- Embedded applications
- Quick prototyping and development

To use the embedded feature, enable it in your `Cargo.toml`:

```toml
falkordb = { version = "0.1.10", features = ["embedded"] }
```

#### Requirements

- `redis-server` must be installed and available in PATH (or you can specify a custom path)
- `falkordb.so` module must be installed (or you can specify a custom path)

You can install these from:
- Redis: https://github.com/redis/redis
- FalkorDB: https://github.com/falkordb/falkordb

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
