[![license](https://img.shields.io/crates/l/falkordb)](https://github.com/FalkorDB/falkordb-rs?tab=License-1-ov-file)
[![Release](https://img.shields.io/github/release/falkordb/falkordb-rs.svg)](https://github.com/falkordb/falkordb-rs/releases/latest)
[![GitHub Issues or Pull Requests](https://img.shields.io/github/issues/falkordb/falkordb-rs)](https://github.com/FalkorDB/falkordb-rs/issues)\
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
falkordb = { version = "*" }
```

### Run FalkorDB instance

Docker:

```sh
docker run --rm -p 6379:6379 falkordb/falkordb
```

### Code Example

```rust
use falkordb::{FalkorClientBuilder, FalkorConnectionInfo};

// Connect to FalkorDB
let connection_info: FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()
    .expect("Invalid connection info");

let client = FalkorClientBuilder::new()
    .with_connection_info(connection_info)
    .build().expect("Failed to build client");

// Select the social graph
let mut graph = client.select_graph("social");

// Create 100 nodes and return a handful
let nodes = graph.query("UNWIND range(0, 100) AS i CREATE (n { v:1 }) RETURN n LIMIT 10")
    .with_timeout(5000).execute().expect("Failed executing query");

for n in nodes.data {
    println!("{:?}", n[0]);
}
```

## Features

### SSL/TLS Support

This client is currently built upon the [`redis`](https://docs.rs/redis/latest/redis/) crate, and therefor supports TLS using
its implementation, which uses either [`rustls`](https://docs.rs/rustls/latest/rustls/) or [`native_tls`](https://docs.rs/native-tls/latest/native_tls/).
This is not enabled by default, and the user ust opt-in by enabling the respective features: `"rustls"`/`"native-tls"`.\

For Rustls:
```toml
falkordb = { version = "*", features = ["rustls"] }
```

For NativeTLS:
```toml
falkordb = { version = "*", features = ["native-tls"] }
```

### Tracing

This crate fully supports instrumentation using the [`tracing`](https://docs.rs/tracing/latest/tracing/) crate, to use it, simply, enable the `tracing` feature:
```toml
falkordb = { version = "*", features = ["tracing"] }
```

Note that different functions use different filtration levels, to avoid spamming your tests, be sure to enable the correct level as you desire it.