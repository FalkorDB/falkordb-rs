/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{FalkorDBError, FalkorResult};

pub(crate) mod blocking;

#[cfg(feature = "tokio")]
pub(crate) mod asynchronous;

/// Where a command must go in a Redis Cluster. Graph commands carry the graph name as their key,
/// so they follow the default key-based routing; the few keyless commands below need an explicit
/// choice (a single arbitrary primary, or every primary).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClusterRoute {
    /// Route by the command's key (the graph name).
    ByKey,
    /// Any single primary can answer. A replica could still lag behind a write made on every
    /// primary, such as `GRAPH.CONFIG SET`.
    AnyPrimary,
    /// Every primary must receive the command, e.g. server-wide state such as UDF libraries.
    AllPrimaries,
}

impl ClusterRoute {
    pub(crate) fn for_command(
        command: &str,
        subcommand: Option<&str>,
    ) -> Self {
        match (command, subcommand) {
            ("INFO", _) | ("GRAPH.CONFIG", Some("GET")) | ("GRAPH.UDF", Some("LIST")) => {
                Self::AnyPrimary
            }
            ("GRAPH.LIST", _)
            | ("GRAPH.CONFIG", Some("SET"))
            | ("GRAPH.UDF", Some("LOAD" | "DELETE" | "FLUSH")) => Self::AllPrimaries,
            _ => Self::ByKey,
        }
    }

    /// The `redis` routing for this route, or `None` to leave the default key-based routing.
    pub(crate) fn routing_info(self) -> Option<redis::cluster_routing::RoutingInfo> {
        use redis::cluster_routing::{MultipleNodeRoutingInfo, RoutingInfo, SingleNodeRoutingInfo};
        match self {
            Self::ByKey => None,
            Self::AnyPrimary => Some(RoutingInfo::SingleNode(
                SingleNodeRoutingInfo::RandomPrimary,
            )),
            Self::AllPrimaries => Some(RoutingInfo::MultiNode((
                MultipleNodeRoutingInfo::AllMasters,
                None,
            ))),
        }
    }

    /// Collapse the per-node reply of an [`AllPrimaries`](Self::AllPrimaries) command into the reply
    /// a single node would give. A node that failed fails the whole command: its error reply is
    /// returned, since redis-rs reports per-node errors as values rather than as an `Err`.
    /// Otherwise `GRAPH.LIST` merges every node's graph names, and for the rest all primaries run
    /// the same command, so any one reply stands for them.
    pub(crate) fn collapse_reply(
        self,
        command: &str,
        reply: redis::Value,
    ) -> redis::Value {
        let redis::Value::Map(mut per_node) = reply else {
            return reply;
        };
        if self != Self::AllPrimaries {
            return redis::Value::Map(per_node);
        }
        if let Some(failed) = per_node
            .iter()
            .position(|(_, reply)| matches!(reply, redis::Value::ServerError(_)))
        {
            return per_node.swap_remove(failed).1;
        }
        if command == "GRAPH.LIST" {
            return redis::Value::Array(
                per_node
                    .into_iter()
                    .flat_map(|(_, names)| match names {
                        redis::Value::Array(names) => names,
                        other => vec![other],
                    })
                    .collect(),
            );
        }
        per_node
            .into_iter()
            .next()
            .map_or(redis::Value::Nil, |(_, reply)| reply)
    }
}

/// Turn a reply that is itself a server error into an `Err`. redis-rs returns error replies as
/// `Ok(Value::ServerError)`, so without this a command whose reply is otherwise ignored would
/// succeed silently.
pub(crate) fn reject_server_error(value: redis::Value) -> FalkorResult<redis::Value> {
    match value {
        redis::Value::ServerError(error) => Err(server_error(error)),
        value => Ok(value),
    }
}

/// A server's error reply as a [`FalkorDBError`]. The cluster/master-down family is a connection
/// failure, as for transport errors in [`map_redis_err`]; anything else keeps the server's whole
/// message, minus the generic `ERR` code. redis-rs splits a reply into a code and details, but
/// FalkorDB messages rarely start with a code (`Division by zero`), so both parts are kept.
pub(crate) fn server_error(error: redis::ServerError) -> FalkorDBError {
    match error.kind() {
        Some(redis::ServerErrorKind::ClusterDown | redis::ServerErrorKind::MasterDown) => {
            FalkorDBError::ConnectionDown
        }
        _ => FalkorDBError::RedisError(server_message(error.code(), error.details())),
    }
}

/// The server's whole error message from its code and details, minus the generic `ERR` code.
fn server_message(
    code: &str,
    details: Option<&str>,
) -> String {
    match (code, details) {
        ("ERR", Some(details)) => details.to_string(),
        (code, Some(details)) => format!("{code} {details}"),
        (code, None) => code.to_string(),
    }
}

fn map_redis_err(error: redis::RedisError) -> FalkorDBError {
    match error.kind() {
        redis::ErrorKind::Io
        | redis::ErrorKind::ClusterConnectionNotFound
        | redis::ErrorKind::Server(redis::ServerErrorKind::ClusterDown)
        | redis::ErrorKind::Server(redis::ServerErrorKind::MasterDown) => {
            FalkorDBError::ConnectionDown
        }
        // A server error that redis-rs raised itself (a cluster connection does) reads the same
        // as one returned as a reply.
        _ => FalkorDBError::RedisError(match error.code() {
            Some(code) => server_message(code, error.detail()),
            None => error.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An error reply exactly as the server sends it.
    fn error_reply(reply: &str) -> redis::Value {
        redis::parse_redis_value(format!("-{reply}\r\n").as_bytes()).expect("an error reply")
    }

    #[test]
    fn reject_server_error_keeps_the_whole_server_message() {
        for (reply, message) in [
            // FalkorDB's own messages have no code: nothing may be dropped from them.
            ("Division by zero", "Division by zero"),
            (
                "Invalid input 'X': expected end of input",
                "Invalid input 'X': expected end of input",
            ),
            (
                "graph.RO_QUERY is to be executed only on read-only queries",
                "graph.RO_QUERY is to be executed only on read-only queries",
            ),
            // Only the generic `ERR` code is dropped.
            (
                "ERR Invalid graph operation on empty key",
                "Invalid graph operation on empty key",
            ),
            ("ERR", "ERR"),
        ] {
            assert_eq!(
                reject_server_error(error_reply(reply)),
                Err(FalkorDBError::RedisError(message.to_string())),
                "{reply}"
            );
        }
    }

    #[test]
    fn reject_server_error_reports_a_down_cluster_or_master_as_connection_down() {
        for reply in [
            "CLUSTERDOWN The cluster is down",
            "MASTERDOWN Link with MASTER is down",
        ] {
            assert_eq!(
                reject_server_error(error_reply(reply)),
                Err(FalkorDBError::ConnectionDown),
                "{reply}"
            );
        }
    }

    #[test]
    fn a_raised_server_error_reads_like_a_returned_one() {
        let reply = error_reply("CROSSSLOT Keys in request don't hash to the same slot");
        let raised = map_redis_err(reply.clone().extract_error().unwrap_err());
        assert_eq!(
            raised,
            FalkorDBError::RedisError(
                "CROSSSLOT Keys in request don't hash to the same slot".to_string()
            )
        );
        assert_eq!(reject_server_error(reply), Err(raised));
    }

    #[test]
    fn reject_server_error_passes_other_replies_through() {
        assert_eq!(
            reject_server_error(redis::Value::Okay),
            Ok(redis::Value::Okay)
        );
    }

    #[test]
    fn test_map_redis_err_io_error() {
        let error = redis::RedisError::from((redis::ErrorKind::Io, "test error"));
        let result = map_redis_err(error);
        assert!(matches!(result, FalkorDBError::ConnectionDown));
    }

    #[test]
    fn test_map_redis_err_cluster_not_found() {
        let error = redis::RedisError::from((redis::ErrorKind::ClusterConnectionNotFound, "test"));
        let result = map_redis_err(error);
        assert!(matches!(result, FalkorDBError::ConnectionDown));
    }

    #[test]
    fn test_map_redis_err_cluster_down() {
        let error = redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::ClusterDown),
            "test",
        ));
        let result = map_redis_err(error);
        assert!(matches!(result, FalkorDBError::ConnectionDown));
    }

    #[test]
    fn test_map_redis_err_master_down() {
        let error = redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::MasterDown),
            "test",
        ));
        let result = map_redis_err(error);
        assert!(matches!(result, FalkorDBError::ConnectionDown));
    }

    #[test]
    fn test_map_redis_err_other() {
        let error = redis::RedisError::from((redis::ErrorKind::UnexpectedReturnType, "test error"));
        let result = map_redis_err(error);
        assert!(matches!(result, FalkorDBError::RedisError(_)));
    }

    #[test]
    fn cluster_route_keyless_commands_get_an_explicit_route() {
        use ClusterRoute::*;
        for (command, subcommand, expected) in [
            ("INFO", Some("server"), AnyPrimary),
            ("GRAPH.CONFIG", Some("GET"), AnyPrimary),
            ("GRAPH.UDF", Some("LIST"), AnyPrimary),
            ("GRAPH.LIST", None, AllPrimaries),
            ("GRAPH.CONFIG", Some("SET"), AllPrimaries),
            ("GRAPH.UDF", Some("LOAD"), AllPrimaries),
            ("GRAPH.UDF", Some("DELETE"), AllPrimaries),
            ("GRAPH.UDF", Some("FLUSH"), AllPrimaries),
            ("GRAPH.QUERY", None, ByKey),
            ("GRAPH.RO_QUERY", None, ByKey),
            ("GRAPH.DELETE", None, ByKey),
        ] {
            assert_eq!(
                ClusterRoute::for_command(command, subcommand),
                expected,
                "{command} {subcommand:?}"
            );
        }
    }

    #[test]
    fn cluster_route_routing_info() {
        use redis::cluster_routing::{MultipleNodeRoutingInfo, RoutingInfo, SingleNodeRoutingInfo};
        assert_eq!(ClusterRoute::ByKey.routing_info(), None);
        assert_eq!(
            ClusterRoute::AnyPrimary.routing_info(),
            Some(RoutingInfo::SingleNode(
                SingleNodeRoutingInfo::RandomPrimary
            ))
        );
        assert_eq!(
            ClusterRoute::AllPrimaries.routing_info(),
            Some(RoutingInfo::MultiNode((
                MultipleNodeRoutingInfo::AllMasters,
                None
            )))
        );
    }

    fn node(
        addr: &str,
        reply: redis::Value,
    ) -> (redis::Value, redis::Value) {
        (redis::Value::BulkString(addr.as_bytes().to_vec()), reply)
    }

    fn name(name: &str) -> redis::Value {
        redis::Value::BulkString(name.as_bytes().to_vec())
    }

    #[test]
    fn collapse_reply_merges_graph_lists_from_every_primary() {
        let reply = redis::Value::Map(vec![
            node("a:1", redis::Value::Array(vec![name("g1"), name("g2")])),
            node("b:2", redis::Value::Array(vec![])),
            node("c:3", redis::Value::Array(vec![name("g3")])),
        ]);
        assert_eq!(
            ClusterRoute::AllPrimaries.collapse_reply("GRAPH.LIST", reply),
            redis::Value::Array(vec![name("g1"), name("g2"), name("g3")])
        );
    }

    #[test]
    fn collapse_reply_keeps_a_non_array_graph_list_entry() {
        let reply = redis::Value::Map(vec![node("a:1", name("g1"))]);
        assert_eq!(
            ClusterRoute::AllPrimaries.collapse_reply("GRAPH.LIST", reply),
            redis::Value::Array(vec![name("g1")])
        );
    }

    #[test]
    fn collapse_reply_returns_a_single_primary_reply_for_other_commands() {
        let ok = redis::Value::Okay;
        let reply = redis::Value::Map(vec![node("a:1", ok.clone()), node("b:2", ok.clone())]);
        assert_eq!(
            ClusterRoute::AllPrimaries.collapse_reply("GRAPH.UDF", reply),
            ok
        );
        assert_eq!(
            ClusterRoute::AllPrimaries.collapse_reply("GRAPH.UDF", redis::Value::Map(vec![])),
            redis::Value::Nil
        );
    }

    #[test]
    fn collapse_reply_surfaces_a_failed_primary() {
        let failure = error_reply("ERR boom");
        for command in ["GRAPH.UDF", "GRAPH.LIST"] {
            let reply = redis::Value::Map(vec![
                node("a:1", redis::Value::Okay),
                node("b:2", failure.clone()),
            ]);
            assert_eq!(
                ClusterRoute::AllPrimaries.collapse_reply(command, reply),
                failure,
                "{command}"
            );
        }
    }

    #[test]
    fn collapse_reply_leaves_other_replies_untouched() {
        let map = redis::Value::Map(vec![node("a:1", redis::Value::Okay)]);
        assert_eq!(
            ClusterRoute::AnyPrimary.collapse_reply("INFO", map.clone()),
            map
        );
        assert_eq!(
            ClusterRoute::AllPrimaries.collapse_reply("GRAPH.UDF", redis::Value::Okay),
            redis::Value::Okay
        );
    }
}
