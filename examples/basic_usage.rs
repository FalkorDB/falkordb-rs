/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use falkordb::{FalkorClientBuilder, FalkorResult};

fn main() -> FalkorResult<()> {
    let client = FalkorClientBuilder::new()
        .with_connection_info("falkor://127.0.0.1:6379".try_into()?)
        .build()?;

    // Dataset is available in the 'resources' directory
    let mut graph = client.select_graph("imdb");

    let mut cloned_graph = client.copy_graph("imdb", "imdb_clone")?;

    let mut res = graph.query("MATCH (a:actor) return a").execute()?;
    let mut clone_graph_res = cloned_graph.query("MATCH (a:actor) return a").execute()?;

    // Parses them one by one, to avoid unneeded performance hits
    assert_eq!(res.data.len(), 1317);
    assert_eq!(clone_graph_res.data.len(), 1317);
    if let (Some(orig), Some(cloned)) = (res.data.next(), clone_graph_res.data.next()) {
        println!("Original one: {orig:?}, Cloned one: {cloned:?}");
        assert_eq!(orig, cloned);
    }

    // We have already parsed one result
    assert_eq!(res.data.len(), 1316);
    assert_eq!(clone_graph_res.data.len(), 1316);

    // more iterator usage:
    for (orig, cloned) in res.data.zip(clone_graph_res.data) {
        println!("Original one: {orig:?}, Cloned one: {cloned:?}");
        assert_eq!(orig, cloned);
    }

    cloned_graph.delete()?;

    let res_again = graph.query("MATCH (a:actor) return a").execute()?;
    let as_vec = res_again.data.collect::<Vec<_>>();
    assert_eq!(as_vec.len(), 1317);

    Ok(())
}
