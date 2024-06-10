/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use falkordb::{FalkorClientBuilder, FalkorResult};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

// Usage of the asynchronous client REQUIRES the multi-threaded rt
#[tokio::main]
async fn main() -> FalkorResult<()> {
    let client = FalkorClientBuilder::new_async()
        .with_connection_info("falkor://127.0.0.1:6379".try_into()?)
        .build()
        .await?;

    let mut graph = client.select_graph("imdb");
    let mut res = graph.query("MATCH (a:actor) return a").execute().await?;
    assert_eq!(res.data.len(), 1317);

    // Note that parsing is sync, even if a refresh of the graph schema was required, that refresh will happen in a blocking fashion
    // The alternative is writing all the parsing functions to be async, all the way down
    assert!(res.data.next().is_some());
    let collected = res.data.collect::<Vec<_>>();

    // One was already taken, so we should have one less now
    assert_eq!(collected.len(), 1316);

    // And now for something completely different:
    // Add synchronization, if we want to reuse the graph later,
    // Otherwise we can just move it into the scope
    let graph_a = Arc::new(Mutex::new(client.copy_graph("imdb", "imdb_a").await?));
    let graph_b = Arc::new(Mutex::new(client.copy_graph("imdb", "imdb_b").await?));
    let graph_c = Arc::new(Mutex::new(client.copy_graph("imdb", "imdb_c").await?));
    let graph_d = Arc::new(Mutex::new(client.copy_graph("imdb", "imdb_d").await?));

    // Note that in each of the tasks, we have to consume the LazyResultSet somehow, and not return it, because it maintains a mutable reference to graph, and requires the lock guard to be alive
    // By collecting it into a vec, we no longer need to maintain the lifetime, so we just get back our results
    let mut join_set = JoinSet::new();
    join_set.spawn({
        let graph_a = graph_a.clone();
        async move { graph_a.lock().await.query("MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 100").execute().await.map(|res| res.data.collect::<Vec<_>>()) }
    });
    join_set.spawn({
        let graph_b = graph_b.clone();
        async move { graph_b.lock().await.query("MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 100").execute().await.map(|res| res.data.collect::<Vec<_>>()) }
    });
    join_set.spawn({
        let graph_c = graph_c.clone();
        async move { graph_c.lock().await.query("MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 100").execute().await.map(|res| res.data.collect::<Vec<_>>()) }
    });
    join_set.spawn({
        let graph_d = graph_d.clone();
        async move { graph_d.lock().await.query("MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 100").execute().await.map(|res| res.data.collect::<Vec<_>>()) }
    });

    // Order is no longer guaranteed, as all these tasks were nonblocking
    while let Some(Ok(res)) = join_set.join_next().await {
        let actual_res = res?;
        println!("{:?}", actual_res[0])
    }

    Ok(())
}
