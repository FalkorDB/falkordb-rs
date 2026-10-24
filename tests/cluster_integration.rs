/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */
#![recursion_limit = "256"]

//! Integration tests against a Redis Cluster running FalkorDB.
//!
//! They only run when `FALKORDB_CLUSTER_URL` points at any node of the cluster (for example
//! `falkor://127.0.0.1:7001`); otherwise every test returns immediately. `just cluster-up` starts
//! a local three-node cluster and `just test-cluster` runs this suite against it.
//!
//! Each test touches many graphs: graph names hash to different slots, so a client that cannot
//! follow the cluster topology fails with `MOVED` errors on the ones owned by other nodes.

use falkordb::{FalkorClientBuilder, FalkorConnectionInfo, FalkorDBError};

const GRAPHS: usize = 16;

/// What the server answers a `GRAPH.COPY` whose two names hash to different slots.
fn cross_slot() -> FalkorDBError {
    FalkorDBError::RedisError("CROSSSLOT Keys in request don't hash to the same slot".to_string())
}

fn cluster_info() -> Option<FalkorConnectionInfo> {
    let url = std::env::var("FALKORDB_CLUSTER_URL").ok()?;
    Some(FalkorConnectionInfo::try_from(url.as_str()).expect("valid FALKORDB_CLUSTER_URL"))
}

fn graph_name(
    prefix: &str,
    i: usize,
) -> String {
    format!("{prefix}_{i}")
}

/// Select `name`, first deleting whatever an interrupted earlier run may have left in it.
fn fresh_graph(
    client: &falkordb::FalkorSyncClient,
    name: &str,
) -> falkordb::SyncGraph {
    let mut graph = client.select_graph(name);
    // Usually there is nothing to delete, which the server reports as an error.
    let _ = graph.delete();
    graph
}

#[test]
fn sync_client_routes_commands_across_cluster_nodes() {
    let Some(info) = cluster_info() else { return };
    let client = FalkorClientBuilder::new()
        .with_connection_info(info)
        .build()
        .expect("cluster client");

    assert_eq!(
        client
            .redis_info(Some("server"))
            .expect("info")
            .get("redis_mode")
            .map(String::as_str),
        Some("cluster"),
        "FALKORDB_CLUSTER_URL must point at a cluster node"
    );

    for i in 0..GRAPHS {
        let mut graph = fresh_graph(&client, &graph_name("rs_cluster_sync", i));
        graph
            .query("CREATE (:N {v: $v})")
            .with_param("v", i as i64)
            .execute()
            .expect("write routed to the owning node");
        let count = graph
            .ro_query("MATCH (n:N) RETURN count(n)")
            .execute()
            .expect("read routed to the owning node")
            .data
            .next()
            .expect("a row")
            .expect("row parses")
            .try_get_at::<i64>(0)
            .expect("count");
        assert_eq!(count, 1);
        graph.delete().expect("delete");
    }
}

#[test]
fn sync_batch_is_routed_to_the_owning_node() {
    let Some(info) = cluster_info() else { return };
    let client = FalkorClientBuilder::new()
        .with_connection_info(info)
        .build()
        .expect("cluster client");

    for i in 0..GRAPHS {
        let mut graph = fresh_graph(&client, &graph_name("rs_cluster_batch", i));
        let mut batch = graph.batch();
        batch.query("CREATE (:N)");
        batch.query("CREATE (:N)");
        batch.ro_query("MATCH (n:N) RETURN count(n) AS c");
        let results = batch.execute().expect("batch dispatched");
        let count: i64 = results[2].as_ref().expect("count ok").data[0]
            .try_get("c")
            .expect("count");
        assert_eq!(count, 2);
        graph.delete().expect("delete");
    }
}

#[test]
fn sync_batch_reports_each_query_outcome() {
    let Some(info) = cluster_info() else { return };
    let client = FalkorClientBuilder::new()
        .with_connection_info(info)
        .build()
        .expect("cluster client");

    // A failing query leaves the results of the others intact, as on a single node.
    for i in 0..GRAPHS {
        let mut graph = fresh_graph(&client, &graph_name("rs_cluster_mixed", i));
        let mut batch = graph.batch();
        batch.query("CREATE (:N)");
        batch.query("RETURN 1/0");
        batch.ro_query("MATCH (n:N) RETURN count(n) AS c");
        let results = batch.execute().expect("batch dispatched");
        assert!(results[0].is_ok());
        assert!(results[1]
            .as_ref()
            .is_err_and(|err| err.to_string().contains("Division by zero")));
        let count: i64 = results[2].as_ref().expect("count ok").data[0]
            .try_get("c")
            .expect("count");
        assert_eq!(count, 1);
        graph.delete().expect("delete");
    }
}

#[test]
fn sync_list_graphs_spans_every_primary() {
    let Some(info) = cluster_info() else { return };
    let client = FalkorClientBuilder::new()
        .with_connection_info(info)
        .build()
        .expect("cluster client");

    let mut graphs: Vec<_> = (0..GRAPHS)
        .map(|i| fresh_graph(&client, &graph_name("rs_cluster_list", i)))
        .collect();
    for graph in &mut graphs {
        graph.query("CREATE (:N)").execute().expect("create");
    }

    // A keyless command would otherwise reach one random node and list only its graphs, so
    // check every call, not just one.
    for _ in 0..3 {
        let listed = client.list_graphs().expect("list");
        for i in 0..GRAPHS {
            assert!(
                listed.contains(&graph_name("rs_cluster_list", i)),
                "graph {i} missing from the cluster-wide listing"
            );
        }
    }
    for graph in &mut graphs {
        graph.delete().expect("delete");
    }
}

#[test]
fn sync_copy_graph_needs_a_shared_hash_tag() {
    let Some(info) = cluster_info() else { return };
    let client = FalkorClientBuilder::new()
        .with_connection_info(info)
        .build()
        .expect("cluster client");

    // The `{rs_copy}` hash tag puts both graphs in one slot, owned by one node.
    let mut source = fresh_graph(&client, "{rs_copy}:source");
    let _ = fresh_graph(&client, "{rs_copy}:copy");
    source
        .query("CREATE (:N {v: 7})")
        .execute()
        .expect("create");
    let mut copy = client
        .copy_graph("{rs_copy}:source", "{rs_copy}:copy")
        .expect("copy within one slot");
    let copied = copy
        .ro_query("MATCH (n:N) RETURN n.v")
        .execute()
        .expect("read the copy")
        .data
        .next()
        .expect("a row")
        .expect("row parses")
        .try_get_at::<i64>(0)
        .expect("v");
    assert_eq!(copied, 7);

    // An untagged name hashes to another slot: the server refuses, and the hint says why.
    let Err(err) = client.copy_graph("{rs_copy}:source", "rs_copy_untagged") else {
        panic!("a copy across slots must fail");
    };
    assert_eq!(err, cross_slot());
    assert!(err
        .mitigation_hint()
        .is_some_and(|hint| hint.contains("hash tag")));
    assert!(!client
        .list_graphs()
        .expect("list")
        .contains(&"rs_copy_untagged".to_string()));

    source.delete().expect("delete source");
    copy.delete().expect("delete copy");
}

#[test]
fn sync_config_and_udf_commands_reach_the_cluster() {
    let Some(info) = cluster_info() else { return };
    let client = FalkorClientBuilder::new()
        .with_connection_info(info)
        .build()
        .expect("cluster client");

    let original = client
        .config_get("RESULTSET_SIZE")
        .expect("config get")
        .remove("RESULTSET_SIZE")
        .expect("RESULTSET_SIZE is reported");
    client
        .config_set("RESULTSET_SIZE", 1234)
        .expect("config set on every primary");
    assert_eq!(
        client.config_get("RESULTSET_SIZE").expect("config get")["RESULTSET_SIZE"],
        falkordb::ConfigValue::Int64(1234)
    );
    client
        .config_set("RESULTSET_SIZE", original)
        .expect("restore config");

    // The only test in this suite that touches the UDF registry, on a cluster the suite has to
    // itself, so the flush below cannot remove another test's libraries.
    let script = "function f(a, b) { return a + b; }\nfalkor.register('f', f);\n";
    client
        .udf_load("rs_cluster_lib", script, true)
        .expect("udf load on every primary");
    assert!(client.udf_list(None, false).is_ok());
    client
        .udf_delete("rs_cluster_lib")
        .expect("udf delete on every primary");
    client.udf_flush().expect("udf flush on every primary");
}

#[cfg(feature = "tokio")]
mod asynchronous {
    use super::*;
    use falkordb::ConnectionStrategy;
    use futures::StreamExt;
    use std::num::NonZeroU8;

    /// Async counterpart of [`fresh_graph`](super::fresh_graph).
    async fn fresh_graph(
        client: &falkordb::FalkorAsyncClient,
        name: &str,
    ) -> falkordb::AsyncGraph {
        let mut graph = client.select_graph(name);
        let _ = graph.delete().await;
        graph
    }

    async fn exercise(
        prefix: &str,
        strategy: ConnectionStrategy,
    ) {
        let Some(info) = cluster_info() else { return };
        let client = FalkorClientBuilder::new_async()
            .with_connection_info(info)
            .with_connection_strategy(strategy)
            .build()
            .await
            .expect("cluster client");

        for i in 0..GRAPHS {
            let mut graph = fresh_graph(&client, &graph_name(prefix, i)).await;
            graph
                .query("CREATE (:N {v: $v})")
                .with_param("v", i as i64)
                .execute()
                .await
                .expect("write routed to the owning node");
            let count = graph
                .ro_query("MATCH (n:N) RETURN count(n)")
                .execute()
                .await
                .expect("read routed to the owning node")
                .data
                .next()
                .await
                .expect("a row")
                .expect("row parses")
                .try_get_at::<i64>(0)
                .expect("count");
            assert_eq!(count, 1);

            let mut batch = graph.batch();
            batch.query("CREATE (:N)");
            batch.ro_query("MATCH (n:N) RETURN count(n) AS c");
            let results = batch.execute().await.expect("batch dispatched");
            let count: i64 = results[1].as_ref().expect("count ok").data[0]
                .try_get("c")
                .expect("count");
            assert_eq!(count, 2);
            graph.delete().await.expect("delete");
        }

        assert!(client.list_graphs().await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_graphs_spans_every_primary() {
        let Some(info) = cluster_info() else { return };
        let client = FalkorClientBuilder::new_async()
            .with_connection_info(info)
            .build()
            .await
            .expect("cluster client");

        let mut graphs = Vec::new();
        for i in 0..GRAPHS {
            graphs.push(fresh_graph(&client, &graph_name("rs_cluster_list_async", i)).await);
        }
        for graph in &mut graphs {
            graph.query("CREATE (:N)").execute().await.expect("create");
        }
        for _ in 0..3 {
            let listed = client.list_graphs().await.expect("list");
            for i in 0..GRAPHS {
                assert!(
                    listed.contains(&graph_name("rs_cluster_list_async", i)),
                    "graph {i} missing from the cluster-wide listing"
                );
            }
        }
        for graph in &mut graphs {
            graph.delete().await.expect("delete");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn copy_graph_needs_a_shared_hash_tag() {
        let Some(info) = cluster_info() else { return };
        let client = FalkorClientBuilder::new_async()
            .with_connection_info(info)
            .build()
            .await
            .expect("cluster client");

        let mut source = fresh_graph(&client, "{rs_copy_async}:source").await;
        let _ = fresh_graph(&client, "{rs_copy_async}:copy").await;
        source
            .query("CREATE (:N {v: 7})")
            .execute()
            .await
            .expect("create");
        let mut copy = client
            .copy_graph("{rs_copy_async}:source", "{rs_copy_async}:copy")
            .await
            .expect("copy within one slot");
        let copied = copy
            .ro_query("MATCH (n:N) RETURN n.v")
            .execute()
            .await
            .expect("read the copy")
            .data
            .next()
            .await
            .expect("a row")
            .expect("row parses")
            .try_get_at::<i64>(0)
            .expect("v");
        assert_eq!(copied, 7);

        let Err(err) = client
            .copy_graph("{rs_copy_async}:source", "rs_copy_async_untagged")
            .await
        else {
            panic!("a copy across slots must fail");
        };
        assert_eq!(err, cross_slot());
        assert!(err
            .mitigation_hint()
            .is_some_and(|hint| hint.contains("hash tag")));

        source.delete().await.expect("delete source");
        copy.delete().await.expect("delete copy");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn batch_reports_each_query_outcome() {
        let Some(info) = cluster_info() else { return };
        let client = FalkorClientBuilder::new_async()
            .with_connection_info(info)
            .build()
            .await
            .expect("cluster client");

        for i in 0..GRAPHS {
            let mut graph = fresh_graph(&client, &graph_name("rs_cluster_mixed_async", i)).await;
            let mut batch = graph.batch();
            batch.query("CREATE (:N)");
            batch.query("RETURN 1/0");
            batch.ro_query("MATCH (n:N) RETURN count(n) AS c");
            let results = batch.execute().await.expect("batch dispatched");
            assert!(results[0].is_ok());
            assert!(results[1]
                .as_ref()
                .is_err_and(|err| err.to_string().contains("Division by zero")));
            let count: i64 = results[2].as_ref().expect("count ok").data[0]
                .try_get("c")
                .expect("count");
            assert_eq!(count, 1);
            graph.delete().await.expect("delete");
        }
    }

    /// Concurrent queries for graphs on every node still complete when `with_max_inflight` caps
    /// each node connection at one command. That the cap holds is checked against a fake node in
    /// the unit test `test_cluster_concurrency_limit_caps_in_flight_queries_per_node`, which can
    /// see every query arrive.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_queries_complete_under_max_inflight() {
        let Some(info) = cluster_info() else { return };
        let client = FalkorClientBuilder::new_async()
            .with_connection_info(info)
            .with_connection_strategy(ConnectionStrategy::Multiplexed {
                connections: NonZeroU8::new(1).unwrap(),
            })
            .with_max_inflight(std::num::NonZeroUsize::new(1).unwrap())
            .build()
            .await
            .expect("cluster client");

        // Concurrent queries for graphs on every node queue on their node's connection.
        let queries = (0..GRAPHS).map(|i| {
            let client = &client;
            async move {
                let mut graph = fresh_graph(client, &graph_name("rs_cluster_inflight", i)).await;
                graph.query("CREATE (:N)").execute().await?;
                graph.delete().await
            }
        });
        for result in futures::future::join_all(queries).await {
            result.expect("query under the in-flight limit");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multiplexed_client_routes_commands_across_cluster_nodes() {
        exercise(
            "rs_cluster_mux",
            ConnectionStrategy::Multiplexed {
                connections: NonZeroU8::new(2).unwrap(),
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pooled_client_routes_commands_across_cluster_nodes() {
        exercise(
            "rs_cluster_pool",
            ConnectionStrategy::Pooled {
                size: NonZeroU8::new(2).unwrap(),
            },
        )
        .await;
    }
}
