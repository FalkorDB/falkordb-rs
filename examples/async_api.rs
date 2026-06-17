/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use falkordb::{FalkorClientBuilder, FalkorResult};
use futures::{StreamExt, TryStreamExt};
use tokio::task::JoinSet;

// Usage of the asynchronous client REQUIRES the multi-threaded rt
#[tokio::main]
async fn main() -> FalkorResult<()> {
    let client = FalkorClientBuilder::new_async()
        .with_connection_info("falkor://127.0.0.1:6379".try_into()?)
        .build()
        .await?;

    let mut graph = client.select_graph("imdb");
    let mut res = graph.query("MATCH (a:actor) RETURN a").execute().await?;
    assert_eq!(res.data.len(), 1317);

    // `res.data` is a `futures::Stream`. Take one row with `StreamExt::next`, then collect the
    // rest. (Parsing is synchronous; if a cold schema refresh is needed it happens in a blocking
    // fashion — see the `RowStream` docs. The alternative is async parsing all the way down.)
    assert!(res.data.next().await.is_some());
    let collected: Vec<_> = res.data.collect().await;
    assert_eq!(collected.len(), 1316);

    // The graph handle is `Send + Clone` and shares its schema cache, so concurrent tasks just take
    // an owned copy — no `Arc<Mutex<_>>` needed. The owned `RowStream` result is `Send + 'static`,
    // so it can be consumed (here folded into a count) entirely inside the spawned task.
    let mut join_set = JoinSet::new();
    for suffix in ["a", "b", "c", "d"] {
        let name = format!("imdb_{suffix}");
        let mut copy = client.copy_graph("imdb", &name).await?;
        join_set.spawn(async move {
            let count = copy
                .query("MATCH (a:actor) RETURN a LIMIT 100")
                .execute()
                .await?
                .data
                .try_fold(0usize, |acc, _row| async move { Ok(acc + 1) })
                .await?;
            copy.delete().await.ok();
            FalkorResult::Ok((name, count))
        });
    }

    // Order is not guaranteed — the tasks ran concurrently over cloned handles.
    while let Some(joined) = join_set.join_next().await {
        let (name, count) = joined.expect("task should not panic")?;
        println!("{name}: {count} actors");
    }

    Ok(())
}
