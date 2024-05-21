/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

#[cfg(not(feature = "redis"))]
compile_error!("The `redis` feature must be enabled.");

pub mod client;
pub mod connection;
pub mod connection_info;
mod error;
pub mod graph;
pub mod value;

#[cfg(feature = "redis")]
mod redis_ext;

pub use error::FalkorDBError;

// Basic tests for now, to be removed in the future and moved to their respective places, currently requires the server to already be up
#[cfg(test)]
mod tests {
    use crate::client::blocking::SyncFalkorClient;
    use crate::client::builder::FalkorDBClientBuilder;
    use crate::value::config::ConfigValue;
    use std::sync::Arc;

    fn create_client() -> Arc<SyncFalkorClient> {
        FalkorDBClientBuilder::new()
            .with_num_connections(4)
            .build()
            .expect("Could not create falkor client")
    }

    #[test]
    fn test_config_get_set() {
        let client = create_client();

        let res = client.config_get("CMD_INFO");
        assert!(res.is_ok());
        assert_eq!(res.unwrap()["CMD_INFO"], ConfigValue::Int64(1));

        assert!(client.config_set("CMD_INFO", "no").is_ok());

        let res = client.config_get("CMD_INFO");
        assert!(res.is_ok());
        assert_eq!(res.unwrap()["CMD_INFO"], ConfigValue::Int64(0));
    }

    #[test]
    fn test_config_get_all() {
        let client = create_client();

        let res = client.config_get("*");
        assert!(res.is_ok());

        // Add some more here, maybe even test all values, but make sure it stays BC
        assert_eq!(
            res.as_ref().unwrap()["EFFECTS_THRESHOLD"],
            ConfigValue::Int64(300)
        );
        assert_eq!(
            res.as_ref().unwrap()["MAX_QUEUED_QUERIES"],
            ConfigValue::Int64(4294967295)
        );
    }

    // #[test]
    // fn test_graph_query() {
    //     let client = create_client();
    //     let graph = client.open_graph("imdb");
    //
    //     let res = graph.query(
    //         "MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 100",
    //     );
    //     assert!(res.is_ok());
    //
    //     let res = res.unwrap().into_vec();
    //     assert!(res.is_ok());
    //
    //     let res = res.unwrap();
    //     assert_eq!(res.len(), 3);
    //
    //     let res = res[2].clone().into_vec();
    //     assert!(res.is_ok());
    //
    //     assert_eq!(res.unwrap().len(), 100);
    // }

    // Race condition here with slowlog_reset test, So ignore this, will be fixed with multi-instance testing
    #[test]
    #[ignore]
    fn test_graph_slowlog() {
        let client = create_client();
        let graph = client.open_graph("imdb");

        assert!(graph.query(
            "MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 2",
        ).is_ok());

        let res = graph.slowlog();
        assert!(res.is_ok());

        assert_eq!(res.unwrap()[0].arguments, "MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 2".to_string());
    }

    #[test]
    fn test_graph_slowlog_reset() {
        let client = create_client();
        let graph = client.open_graph("imdb");

        assert!(graph.query(
            "MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 10",
        ).is_ok());

        assert!(graph.slowlog_reset().is_ok());

        let res = graph.slowlog();
        assert!(res.is_ok());

        assert!(res.unwrap().is_empty());
    }
}
