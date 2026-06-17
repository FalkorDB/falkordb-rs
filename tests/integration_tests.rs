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
fn test_reads_from_replicas_single_node() {
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

    // The test endpoint can be pointed at a Sentinel deployment via env vars, in
    // which case `reads_from_replicas()` may legitimately be true. Only assert the
    // single-node expectation (no replica routing) when the endpoint is known to be
    // non-Sentinel; set FALKORDB_SENTINEL to skip that strict assertion.
    let is_sentinel = std::env::var("FALKORDB_SENTINEL")
        .map(|v| !v.is_empty() && v != "0" && v.to_lowercase() != "false")
        .unwrap_or(false);
    if !is_sentinel {
        assert!(!client.reads_from_replicas());
    }

    let mut graph = client.select_graph("test_reads_from_replicas_single_node");
    let _ = graph.query("CREATE (n:Data {value: 7})").execute();

    // Read-only queries must succeed regardless of whether reads are routed to a
    // replica (Sentinel) or served by the primary (single-node).
    let result = graph.ro_query("MATCH (n:Data) RETURN n.value").execute();
    assert!(result.is_ok());

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

mod typed_params {
    use super::{get_test_connection_info, skip_if_no_server};
    use falkordb::{FalkorClientBuilder, FalkorValue};
    use std::collections::BTreeMap;

    fn graph_for(name: &str) -> Option<falkordb::SyncGraph> {
        if skip_if_no_server() {
            return None;
        }
        let conn_info = get_test_connection_info().ok()?;
        let client = FalkorClientBuilder::new()
            .with_connection_info(conn_info)
            .build()
            .ok()?;
        let mut graph = client.select_graph(name);
        let _ = graph.delete();
        Some(graph)
    }

    #[test]
    fn test_with_param_scalars_round_trip() {
        let Some(mut graph) = graph_for("test_params_scalars") else {
            return;
        };
        let mut result = graph
            .query("RETURN $s, $i, $f, $b, $n")
            .with_param("s", "it's a \"test\"")
            .with_param("i", 42i64)
            .with_param("f", 2.5f64)
            .with_param("b", true)
            .with_param("n", Option::<i64>::None)
            .execute()
            .expect("query should succeed");
        let row = result.data.next().expect("expected a row");
        assert_eq!(
            row,
            vec![
                FalkorValue::String("it's a \"test\"".to_string()),
                FalkorValue::I64(42),
                FalkorValue::F64(2.5),
                FalkorValue::Bool(true),
                FalkorValue::None,
            ]
        );
        let _ = graph.delete();
    }

    #[test]
    fn test_with_param_injection_is_inert() {
        let Some(mut graph) = graph_for("test_params_injection") else {
            return;
        };
        let payload = "'; MATCH (n) DETACH DELETE n //";
        graph
            .query("CREATE (:Tag {name: $name})")
            .with_param("name", payload)
            .execute()
            .expect("create should succeed");

        // The payload must be stored verbatim, and nothing should have been deleted/executed.
        let mut result = graph
            .query("MATCH (t:Tag) RETURN t.name")
            .execute()
            .expect("read should succeed");
        let row = result.data.next().expect("the node must still exist");
        assert_eq!(
            row.into_iter().next(),
            Some(FalkorValue::String(payload.to_string()))
        );
        let _ = graph.delete();
    }

    #[test]
    fn test_with_param_list_in_clause() {
        let Some(mut graph) = graph_for("test_params_list") else {
            return;
        };
        graph
            .query("UNWIND [1, 2, 3, 4] AS v CREATE (:N {v: v})")
            .execute()
            .expect("create should succeed");
        let mut result = graph
            .query("MATCH (n:N) WHERE n.v IN $vals RETURN count(n)")
            .with_param("vals", [2, 4])
            .execute()
            .expect("query should succeed");
        let count = result
            .data
            .next()
            .and_then(|row| row.into_iter().next())
            .and_then(|v| v.to_i64());
        assert_eq!(count, Some(2));
        let _ = graph.delete();
    }

    #[test]
    fn test_with_param_map_and_point_workaround() {
        let Some(mut graph) = graph_for("test_params_point") else {
            return;
        };
        let coords = BTreeMap::from([("latitude", 32.07), ("longitude", 34.79)]);
        let mut result = graph
            .query("RETURN point($p)")
            .with_param("p", coords)
            .execute()
            .expect("point($p) workaround should succeed");
        let value = result
            .data
            .next()
            .and_then(|row| row.into_iter().next())
            .expect("expected a point");
        assert!(matches!(value, FalkorValue::Point(_)));
        let _ = graph.delete();
    }

    #[test]
    fn test_invalid_param_name_errors_before_network() {
        let Some(mut graph) = graph_for("test_params_bad_name") else {
            return;
        };
        let result = graph
            .query("RETURN $x")
            .with_param("bad name", 1i64)
            .execute();
        assert!(
            matches!(result, Err(falkordb::FalkorDBError::ParamEncoding { .. })),
            "an invalid parameter name must fail with ParamEncoding"
        );
        let _ = graph.delete();
    }

    #[test]
    fn test_with_params_collection() {
        let Some(mut graph) = graph_for("test_params_collection") else {
            return;
        };
        let mut result = graph
            .query("RETURN $a + $b")
            .with_params([("a", 10i64), ("b", 32i64)])
            .execute()
            .expect("query should succeed");
        let value = result
            .data
            .next()
            .and_then(|row| row.into_iter().next())
            .and_then(|v| v.to_i64());
        assert_eq!(value, Some(42));
        let _ = graph.delete();
    }

    #[test]
    fn test_with_raw_param() {
        let Some(mut graph) = graph_for("test_params_raw") else {
            return;
        };
        let mut result = graph
            .query("RETURN $v")
            .with_raw_param("v", "[1, 2, 3]")
            .execute()
            .expect("query should succeed");
        let row = result.data.next().expect("expected a row");
        assert_eq!(
            row.into_iter().next(),
            Some(FalkorValue::Array(vec![
                FalkorValue::I64(1),
                FalkorValue::I64(2),
                FalkorValue::I64(3),
            ]))
        );
        let _ = graph.delete();
    }

    #[test]
    fn test_try_with_param() {
        let Some(mut graph) = graph_for("test_params_try") else {
            return;
        };
        // A non-finite float fails eagerly, before building the query.
        assert!(graph
            .query("RETURN $x")
            .try_with_param("x", f64::NAN)
            .is_err());

        // A valid value succeeds and executes.
        let mut result = graph
            .query("RETURN $x")
            .try_with_param("x", 7i64)
            .expect("valid param should be accepted")
            .execute()
            .expect("query should succeed");
        let value = result
            .data
            .next()
            .and_then(|row| row.into_iter().next())
            .and_then(|v| v.to_i64());
        assert_eq!(value, Some(7));
        let _ = graph.delete();
    }

    #[test]
    fn test_with_param_last_wins_and_clears_error() {
        let Some(mut graph) = graph_for("test_params_lastwins") else {
            return;
        };
        // The last value for a key wins, and a later valid value clears an earlier encoding
        // error for the same key.
        let mut result = graph
            .query("RETURN $x")
            .with_param("x", f64::NAN) // would error
            .with_param("x", 7i64) // overrides → valid
            .execute()
            .expect("the override should clear the earlier error");
        let value = result
            .data
            .next()
            .and_then(|row| row.into_iter().next())
            .and_then(|v| v.to_i64());
        assert_eq!(value, Some(7));
        let _ = graph.delete();
    }

    #[test]
    fn test_with_params_invalid_value_fails_at_execute() {
        let Some(mut graph) = graph_for("test_params_invalid_collection") else {
            return;
        };
        let result = graph
            .query("RETURN $x")
            .with_params([("x", f64::NAN)])
            .execute();
        assert!(matches!(
            result,
            Err(falkordb::FalkorDBError::ParamEncoding { .. })
        ));
        let _ = graph.delete();
    }
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

    #[cfg(feature = "serde")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_query_as() {
        use serde::Deserialize;

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

        #[derive(Debug, Deserialize, PartialEq)]
        struct Movie {
            title: String,
            year: i64,
        }

        let mut graph = client.select_graph("test_async_query_as");
        let _ = graph.delete().await;

        graph
            .query("CREATE (:Movie {title: 'Heat', year: 1995})")
            .execute()
            .await
            .expect("create should succeed");

        let movies: Vec<Movie> = graph
            .query("MATCH (m:Movie) RETURN m")
            .query_as::<Movie>()
            .execute()
            .await
            .expect("query should succeed")
            .data
            .collect::<Result<_, _>>()
            .expect("row mapping should succeed");

        assert_eq!(
            movies,
            vec![Movie {
                title: "Heat".to_string(),
                year: 1995,
            }]
        );

        let _ = graph.delete().await;
    }
}

#[cfg(feature = "serde")]
mod serde_typed_mapping {
    use super::{get_test_connection_info, skip_if_no_server};
    use falkordb::FalkorClientBuilder;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Movie {
        title: String,
        year: i64,
        #[serde(default)]
        rating: Option<f64>,
    }

    #[test]
    fn test_deserialize_node_into_struct() {
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

        let mut graph = client.select_graph("test_serde_typed_mapping");
        let _ = graph.delete();

        graph
            .query("CREATE (:Movie {title: 'Heat', year: 1995, rating: 8.3})")
            .execute()
            .expect("create should succeed");

        let mut result = graph
            .query("MATCH (m:Movie) RETURN m")
            .execute()
            .expect("query should succeed");

        let row = result.data.next().expect("expected a row");
        let node = row.into_iter().next().expect("expected a node column");
        let movie: Movie = node.deserialize_into().expect("deserialize should succeed");

        assert_eq!(
            movie,
            Movie {
                title: "Heat".to_string(),
                year: 1995,
                rating: Some(8.3),
            }
        );

        let _ = graph.delete();
    }

    #[test]
    fn test_deserialize_scalar_column() {
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

        let mut graph = client.select_graph("test_serde_scalar_column");
        let _ = graph.delete();

        let mut result = graph
            .query("RETURN 42")
            .execute()
            .expect("query should succeed");

        let row = result.data.next().expect("expected a row");
        let value = row.into_iter().next().expect("expected a column");
        let number: i64 = value
            .deserialize_into()
            .expect("deserialize should succeed");
        assert_eq!(number, 42);

        let _ = graph.delete();
    }

    #[test]
    fn test_query_as_node_rows() {
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

        let mut graph = client.select_graph("test_serde_query_as_nodes");
        let _ = graph.delete();

        graph
            .query(
                "CREATE (:Movie {title: 'Heat', year: 1995, rating: 8.3}), \
                 (:Movie {title: 'Casino', year: 1995})",
            )
            .execute()
            .expect("create should succeed");

        let mut movies: Vec<Movie> = graph
            .query("MATCH (m:Movie) RETURN m")
            .query_as::<Movie>()
            .execute()
            .expect("query should succeed")
            .data
            .collect::<Result<_, _>>()
            .expect("row mapping should succeed");

        movies.sort_by(|a, b| a.title.cmp(&b.title));
        assert_eq!(
            movies,
            vec![
                Movie {
                    title: "Casino".to_string(),
                    year: 1995,
                    rating: None,
                },
                Movie {
                    title: "Heat".to_string(),
                    year: 1995,
                    rating: Some(8.3),
                },
            ]
        );

        let _ = graph.delete();
    }

    #[test]
    fn test_query_as_multi_column_aliased() {
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

        let mut graph = client.select_graph("test_serde_query_as_aliased");
        let _ = graph.delete();

        graph
            .query("CREATE (:Movie {title: 'Heat', year: 1995})")
            .execute()
            .expect("create should succeed");

        let rows: Vec<Movie> = graph
            .query("MATCH (m:Movie) RETURN m.title AS title, m.year AS year")
            .query_as::<Movie>()
            .execute()
            .expect("query should succeed")
            .data
            .collect::<Result<_, _>>()
            .expect("row mapping should succeed");

        assert_eq!(
            rows,
            vec![Movie {
                title: "Heat".to_string(),
                year: 1995,
                rating: None,
            }]
        );

        let _ = graph.delete();
    }

    #[test]
    fn test_query_as_scalar_rows() {
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

        let mut graph = client.select_graph("test_serde_query_as_scalar");
        let _ = graph.delete();

        graph
            .query("CREATE (:Movie {title: 'Heat', year: 1995})")
            .execute()
            .expect("create should succeed");

        let counts: Vec<i64> = graph
            .query("MATCH (m:Movie) RETURN count(m)")
            .query_as::<i64>()
            .execute()
            .expect("query should succeed")
            .data
            .collect::<Result<_, _>>()
            .expect("row mapping should succeed");

        assert_eq!(counts, vec![1]);

        let _ = graph.delete();
    }
}
