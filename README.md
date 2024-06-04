[![license](https://img.shields.io/github/license/falkordb/falkordb-rs.svg)](https://github.com/falkordb/falkordb-rs)
[![Release](https://img.shields.io/github/release/falkordb/falkordb-rs.svg)](https://github.com/falkordb/falkordb-rs/releases/latest)
[![Codecov](https://codecov.io/gh/falkordb/falkordb-rs/branch/main/graph/badge.svg)](https://codecov.io/gh/falkordb/falkordb-rs)
[![Docs](https://img.shields.io/docsrs/:crate)](https://docs.rs/crate/falkordb/0.1.0)\
[![Forum](https://img.shields.io/badge/Forum-falkordb-blue)](https://github.com/orgs/FalkorDB/discussions)
[![Discord](https://img.shields.io/discord/1146782921294884966?style=flat-square)](https://discord.gg/ErBEqN9E)

# falkordb-client-rs

[![Try Free](https://img.shields.io/badge/Try%20Free-FalkorDB%20Cloud-FF8101?labelColor=FDE900&style=for-the-badge&link=https://app.falkordb.cloud)](https://app.falkordb.cloud)

FalkorDB Rust client

## Usage

### Installation

Just add it to your `Cargo.toml`, like so

```toml
falkordb = { version = "0.1.0" }
```

### Run FalkorDB instance

Docker:

```sh
docker run --rm -p 6379:6379 falkordb/falkordb
```

Or use our [sandbox](https://cloud.falkordb.com/sandbox)

### Code Example

```rust
use falkordb::FalkorClientBuilder;

// Connect to FalkorDB
let client = FalkorClientBuilder::new().with_connection_info("falkor://127.0.0.1:6379".try_into().unwrap()).build().unwrap();

// Select the social graph
let mut graph = client.select_graph("social");

// Create 100 nodes and return a handful
let nodes = graph.query("UNWIND range(0, 100) AS i CREATE (n { v:1 }) RETURN n LIMIT 10").with_timeout(5000).perform().unwrap().data;
for n in nodes {
println!("{:?}", n[0]);
}
```
