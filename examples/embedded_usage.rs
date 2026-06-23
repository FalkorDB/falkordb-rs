/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Example demonstrating the embedded FalkorDB server feature.
//!
//! This example shows how to use FalkorDB with an embedded Redis server
//! that has the FalkorDB module loaded. This is useful for:
//! - Testing without external dependencies
//! - Embedded applications
//! - Quick prototyping
//!
//! # Self-Contained Mode
//!
//! The embedded server can auto-provision the FalkorDB **module** via the
//! `auto_download` option (enabled by default). When enabled, a missing
//! `falkordb.so` is downloaded from the official FalkorDB releases, verified
//! against a pinned SHA-256 checksum and cached locally, so the embedded server
//! "just works" without a pre-installed module.
//!
//! The `redis-server` binary is **not** downloaded — it is located on the host
//! via `PATH` or `redis_server_path`. Install it from your package manager
//! (e.g. `brew install redis`, `apt-get install redis-server`).
//!
//! # Using an already-installed FalkorDB
//!
//! To run fully offline against binaries already on the machine, either set
//! `falkordb_module_path` explicitly or disable `auto_download` (common system
//! locations are still searched). See `already_installed_config()` below.
//!
//! # Build-time bundle (offline at runtime)
//!
//! For network-isolated deployments, enable the `embedded-bundle` feature instead
//! of `embedded`. A `build.rs` fetches the module for the build target at compile
//! time and embeds it in the binary, so the running process needs **no network**.
//! The same `EmbeddedConfig` / API shown here works unchanged; only the module
//! source differs. Run it with:
//! ```bash
//! cargo run --example embedded_usage --features embedded-bundle
//! ```
//!
//! # Platform-Specific Requirements
//!
//! ## macOS
//! The FalkorDB module requires OpenMP (libomp) on macOS. If not already installed via
//! Homebrew, install it with: `brew install libomp`
//!
//! # License Note
//!
//! The FalkorDB module binary (downloaded at runtime, or embedded at build time
//! with `embedded-bundle`) is licensed under the Server Side Public License
//! (SSPL) v1. See https://www.falkordb.com/ for licensing details.
//!
//! # Running this example
//! ```bash
//! cargo run --example embedded_usage --features embedded
//! ```

use falkordb::{EmbeddedConfig, FalkorClientBuilder, FalkorConnectionInfo, FalkorResult};

/// Build a config that uses binaries already installed on the machine, without
/// any network access: redis-server is taken from `PATH` and the FalkorDB
/// module from the given path (or common system locations when left as `None`).
#[allow(dead_code)]
fn already_installed_config() -> EmbeddedConfig {
    EmbeddedConfig {
        // Disable downloading: resolve only explicit/system binaries.
        auto_download: false,
        // Optionally pin an installed module; `None` searches system locations.
        falkordb_module_path: None,
        ..Default::default()
    }
}

fn main() -> FalkorResult<()> {
    println!("Starting embedded FalkorDB example...");

    // Default config: download + cache the FalkorDB module if missing, and find
    // redis-server on PATH. Swap in `already_installed_config()` to run offline
    // against an existing install instead.
    let embedded_config = EmbeddedConfig::default();

    // You can also customize the configuration:
    // let embedded_config = EmbeddedConfig {
    //     redis_server_path: Some(PathBuf::from("/path/to/redis-server")),
    //     falkordb_module_path: Some(PathBuf::from("/path/to/falkordb.so")),
    //     db_dir: Some(PathBuf::from("/tmp/my_falkordb")),
    //     db_filename: "mydb.rdb".to_string(),
    //     socket_path: Some(PathBuf::from("/tmp/falkordb.sock")),
    //     start_timeout: Duration::from_secs(10),
    //     auto_download: true,
    //     falkordb_version: None, // or Some("v4.18.10".to_string())
    //     cache_dir: None,        // or Some(PathBuf::from("/tmp/fk-cache"))
    // };

    println!("Creating client with embedded FalkorDB server...");

    // Build a client with embedded FalkorDB
    let client = FalkorClientBuilder::new()
        .with_connection_info(FalkorConnectionInfo::Embedded(embedded_config))
        .build()?;

    println!("Connected to embedded FalkorDB server!");

    // Now use the client normally
    let mut graph = client.select_graph("social");

    // Create some nodes
    println!("Creating nodes...");
    graph
        .query("CREATE (:Person {name: 'Alice', age: 30})")
        .execute()?;
    graph
        .query("CREATE (:Person {name: 'Bob', age: 25})")
        .execute()?;
    graph
        .query("CREATE (:Person {name: 'Charlie', age: 35})")
        .execute()?;

    // Create relationships
    println!("Creating relationships...");
    graph
        .query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
        )
        .execute()?;
    graph
        .query(
            "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)",
        )
        .execute()?;

    // Query the graph
    println!("\nQuerying the graph:");
    let result = graph
        .query("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age")
        .execute()?;

    println!("Found {} results:", result.data.len());
    for row in result.data {
        println!("  {:?}", row);
    }

    // Query with relationships
    println!("\nQuerying relationships:");
    let result = graph
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .execute()?;

    println!("Found {} relationships:", result.data.len());
    for row in result.data {
        println!("  {:?}", row);
    }

    // Clean up
    println!("\nDeleting graph...");
    graph.delete()?;

    println!("Example completed successfully!");
    println!("The embedded server will be automatically shut down when the client is dropped.");

    Ok(())
}
