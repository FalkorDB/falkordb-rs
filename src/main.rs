use anyhow::Result;

mod client;
mod connection;
mod connection_info;
mod error;
mod graph;
mod value;

#[cfg(feature = "redis")]
mod redis_ext;

fn main() -> Result<()> {
    simple_logger::init_utc().expect("Could not start logger");

    let client = client::builder::FalkorDBClientBuilder::new()
        .with_num_connections(4)
        .build()?;

    client.config_set("CMD_INFO", "no")?;
    let res = client.config_get("*")?;

    let graph = client.open_graph("imdb");

    // let res = graph.query(
    //     "MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 10",
    // )?;
    let res = graph.slowlog()?;
    log::info!("{res:?}");

    Ok(())
}
