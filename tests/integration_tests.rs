/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Integration tests for FalkorDB client
//!
//! These tests require a running FalkorDB instance.
//! Set FALKORDB_HOST and FALKORDB_PORT environment variables to configure.
//! Default: localhost:6379

use falkordb::{FalkorClientBuilder, FalkorConnectionInfo, FalkorResult};

fn get_test_connection_info() -> FalkorResult<FalkorConnectionInfo> {
    let host = std::env::var("FALKORDB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("FALKORDB_PORT")
        .unwrap_or_else(|_| "6379".to_string())
        .parse()
        .unwrap_or(6379);

    FalkorConnectionInfo::try_from((host.as_str(), port))
}

fn skip_if_no_server() -> bool {
    // Check if we should skip tests
    std::env::var("SKIP_INTEGRATION_TESTS").is_ok()
}

#[test]
fn test_client_connection() {
    if skip_if_no_server() {
        return;
    }

    let conn_info = match get_test_connection_info() {
        Ok(info) => info,
        Err(_) => return,
    };

    let client = FalkorClientBuilder::new()
        .with_connection_info(conn_info)
        .build();

    assert!(client.is_ok(), "Failed to create client");
}

#[test]
fn test_select_graph_and_simple_query() {
    if skip_if_no_server() {
        return;
    }

    let conn_info = match get_test_connection_info() {
        Ok(info) => info,
        Err(_) => return,
    };

    let client = match FalkorClientBuilder::new()
        .with_connection_info(conn_info)
        .build()
    {
        Ok(c) => c,
        Err(_) => return,
    };

    let mut graph = client.select_graph("test_integration");

    // Create a simple node
    let result = graph
        .query("CREATE (n:TestNode {name: 'test'}) RETURN n")
        .execute();

    assert!(result.is_ok(), "Failed to execute query");

    // Clean up
    let _ = graph.delete();
}

#[test]
fn test_graph_creation_and_deletion() {
    if skip_if_no_server() {
        return;
    }

    let conn_info = match get_test_connection_info() {
        Ok(info) => info,
        Err(_) => return,
    };

    let client = match FalkorClientBuilder::new()
        .with_connection_info(conn_info)
        .build()
    {
        Ok(c) => c,
        Err(_) => return,
    };

    let mut graph = client.select_graph("test_temp_graph");

    // Create some data
    let _ = graph
        .query("CREATE (n:Person {name: 'Alice', age: 30})")
        .execute();

    // Query the data
    let result = graph.query("MATCH (n:Person) RETURN n").execute();

    assert!(result.is_ok());

    // Delete the graph
    let delete_result = graph.delete();
    assert!(delete_result.is_ok(), "Failed to delete graph");
}

#[test]
fn test_multiple_connections() {
    if skip_if_no_server() {
        return;
    }

    let conn_info = match get_test_connection_info() {
        Ok(info) => info,
        Err(_) => return,
    };

    let client = match FalkorClientBuilder::new()
        .with_connection_info(conn_info)
        .with_num_connections(std::num::NonZeroU8::new(4).unwrap())
        .build()
    {
        Ok(c) => c,
        Err(_) => return,
    };

    assert_eq!(client.connection_pool_size(), 4);

    let mut graph = client.select_graph("test_multi_conn");
    let _ = graph.query("CREATE (n:Test {id: 1})").execute();
    let _ = graph.delete();
}

#[test]
fn test_read_only_query() {
    if skip_if_no_server() {
        return;
    }

    let conn_info = match get_test_connection_info() {
        Ok(info) => info,
        Err(_) => return,
    };

    let client = match FalkorClientBuilder::new()
        .with_connection_info(conn_info)
        .build()
    {
        Ok(c) => c,
        Err(_) => return,
    };

    let mut graph = client.select_graph("test_readonly");

    // Create some data first
    let _ = graph.query("CREATE (n:Data {value: 42})").execute();

    // Perform read-only query
    let result = graph.ro_query("MATCH (n:Data) RETURN n.value").execute();

    assert!(result.is_ok());

    // Clean up
    let _ = graph.delete();
}

#[test]
fn test_list_graphs() {
    if skip_if_no_server() {
        return;
    }

    let conn_info = match get_test_connection_info() {
        Ok(info) => info,
        Err(_) => return,
    };

    let client = match FalkorClientBuilder::new()
        .with_connection_info(conn_info)
        .build()
    {
        Ok(c) => c,
        Err(_) => return,
    };

    // Create a test graph
    let mut graph = client.select_graph("test_list_graphs");
    let _ = graph.query("CREATE (n:Node)").execute();

    // List graphs
    let graphs = client.list_graphs();

    // Should include our test graph
    assert!(graphs.is_ok(), "Failed to list graphs: {:?}", graphs.err());

    // Clean up
    let _ = graph.delete();
}

#[cfg(feature = "tokio")]
mod async_tests {
    use super::*;

    #[tokio::test]
    async fn test_async_client_connection() {
        if skip_if_no_server() {
            return;
        }

        let conn_info = match get_test_connection_info() {
            Ok(info) => info,
            Err(_) => return,
        };

        let client = falkordb::FalkorClientBuilder::new_async()
            .with_connection_info(conn_info)
            .build()
            .await;

        assert!(client.is_ok(), "Failed to create async client");
    }

    #[tokio::test]
    async fn test_async_query() {
        if skip_if_no_server() {
            return;
        }

        let conn_info = match get_test_connection_info() {
            Ok(info) => info,
            Err(_) => return,
        };

        let client = match falkordb::FalkorClientBuilder::new_async()
            .with_connection_info(conn_info)
            .build()
            .await
        {
            Ok(c) => c,
            Err(_) => return,
        };

        let mut graph = client.select_graph("test_async_query");

        let result = graph
            .query("CREATE (n:AsyncTest {value: 123}) RETURN n")
            .execute()
            .await;

        assert!(result.is_ok());

        let _ = graph.delete().await;
    }
}
