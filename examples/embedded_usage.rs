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
//! # Requirements
//! - redis-server must be installed and available in PATH (or specify path in EmbeddedConfig)
//! - falkordb.so module must be installed (or specify path in EmbeddedConfig)
//!
//! # Running this example
//! ```bash
//! cargo run --example embedded_usage --features embedded
//! ```

use falkordb::{EmbeddedConfig, FalkorClientBuilder, FalkorConnectionInfo, FalkorResult};

fn main() -> FalkorResult<()> {
    println!("Starting embedded FalkorDB example...");
    
    // Create an embedded configuration
    // This will use defaults: redis-server from PATH, falkordb.so from common locations
    let embedded_config = EmbeddedConfig::default();
    
    // You can also customize the configuration:
    // let embedded_config = EmbeddedConfig {
    //     redis_server_path: Some(PathBuf::from("/path/to/redis-server")),
    //     falkordb_module_path: Some(PathBuf::from("/path/to/falkordb.so")),
    //     db_dir: Some(PathBuf::from("/tmp/my_falkordb")),
    //     db_filename: "mydb.rdb".to_string(),
    //     socket_path: Some(PathBuf::from("/tmp/falkordb.sock")),
    //     start_timeout: Duration::from_secs(10),
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
    graph.query("CREATE (:Person {name: 'Alice', age: 30})").execute()?;
    graph.query("CREATE (:Person {name: 'Bob', age: 25})").execute()?;
    graph.query("CREATE (:Person {name: 'Charlie', age: 35})").execute()?;
    
    // Create relationships
    println!("Creating relationships...");
    graph.query("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)").execute()?;
    graph.query("MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)").execute()?;
    
    // Query the graph
    println!("\nQuerying the graph:");
    let result = graph.query("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age").execute()?;
    
    println!("Found {} results:", result.data.len());
    for row in result.data {
        println!("  {:?}", row);
    }
    
    // Query with relationships
    println!("\nQuerying relationships:");
    let result = graph.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name").execute()?;
    
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
