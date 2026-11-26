/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Helpers shared by the server-backed test suites: which protocol they speak, and which server
//! version they run against.
//!
//! CI runs every suite against FalkorDB 4.x and 6.x, each over RESP2 and RESP3
//! (`FALKORDB_PROTOCOL`). A test whose result depends on the server version or the protocol asserts
//! each one's result; none returns early or is ignored for one of them.

// Each test crate uses a different subset of these helpers.
#![allow(dead_code)]

use falkordb::FalkorConnectionInfo;

/// The protocol the suites speak: `FALKORDB_PROTOCOL`, `resp2` (the default) or `resp3`.
pub fn test_protocol() -> redis::ProtocolVersion {
    match std::env::var("FALKORDB_PROTOCOL").ok().as_deref() {
        None | Some("") | Some("resp2") => redis::ProtocolVersion::RESP2,
        Some("resp3") => redis::ProtocolVersion::RESP3,
        Some(other) => panic!("FALKORDB_PROTOCOL must be resp2 or resp3, not {other:?}"),
    }
}

/// Both protocols, for tests about the wire format, which check both in every run.
pub const PROTOCOLS: [redis::ProtocolVersion; 2] =
    [redis::ProtocolVersion::RESP2, redis::ProtocolVersion::RESP3];

/// `url` (a `falkor://` or `redis://` URL without a query) with the query that selects
/// `protocol`.
pub fn url_with_protocol(
    url: &str,
    protocol: redis::ProtocolVersion,
) -> String {
    match protocol {
        redis::ProtocolVersion::RESP3 => format!("{url}?protocol=resp3"),
        _ => url.to_string(),
    }
}

/// Connection info for `url` over `protocol`. Fails if the parsed info doesn't select that
/// protocol, so a suite can't quietly run RESP2 twice.
pub fn connection_info(
    url: &str,
    protocol: redis::ProtocolVersion,
) -> FalkorConnectionInfo {
    let info = FalkorConnectionInfo::try_from(url_with_protocol(url, protocol).as_str())
        .unwrap_or_else(|err| panic!("{url} must be a valid connection URL: {err}"));
    #[allow(irrefutable_let_patterns)]
    if let FalkorConnectionInfo::Redis(redis_info) = &info {
        assert_eq!(
            redis_info.redis_settings().protocol(),
            protocol,
            "the test connection must speak the protocol it asks for"
        );
    }
    info
}

/// The version of the graph module on the server at `url` (a `falkor://` or `redis://` URL), as
/// `MODULE LIST` reports it: `42204` for 4.22.4, `60201` for 6.2.1.
pub fn server_module_version(url: &str) -> i64 {
    let url = url.replacen("falkor://", "redis://", 1);
    let mut conn = redis::Client::open(url.as_str())
        .and_then(|client| client.get_connection())
        .unwrap_or_else(|err| panic!("a FalkorDB server must be reachable at {url}: {err}"));
    let modules: Vec<std::collections::HashMap<String, redis::Value>> = redis::cmd("MODULE")
        .arg("LIST")
        .query(&mut conn)
        .expect("MODULE LIST");
    modules
        .iter()
        .find(|module| {
            module.get("name").is_some_and(|name| {
                redis::from_redis_value_ref::<String>(name).is_ok_and(|name| name == "graph")
            })
        })
        .and_then(|graph| graph.get("ver"))
        .and_then(|version| redis::from_redis_value_ref::<i64>(version).ok())
        .expect("the graph module's version")
}

/// Whether the server at `url` answers `SCHEMA_HASH`: a FalkorDB module of 6.2.0 or later.
pub fn server_has_schema_hash(url: &str) -> bool {
    server_module_version(url) >= 60200
}
