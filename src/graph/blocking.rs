/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::client::blocking::SyncFalkorClient;
use crate::connection::blocking::FalkorSyncConnection;
use crate::value::slowlog_entry::SlowlogEntry;
use crate::value::FalkorValue;
use anyhow::Result;
use redis::ConnectionLike;

pub struct SyncGraph<'a> {
    pub(crate) client: &'a SyncFalkorClient,
    pub(crate) graph_name: String,
}

impl SyncGraph<'_> {
    pub fn graph_name(&self) -> &str {
        self.graph_name.as_str()
    }

    fn send_command(&self, command: &str, args: &[FalkorValue]) -> Result<FalkorValue> {
        let mut conn = self.client.borrow_connection()?;
        Ok(match conn.as_inner()? {
            #[cfg(feature = "redis")]
            FalkorSyncConnection::Redis(redis_conn) => {
                let mut cmd = redis::cmd(command);
                cmd.arg(self.graph_name.as_str());
                for arg in args {
                    let _ = cmd.arg(arg);
                }
                redis::FromRedisValue::from_owned_redis_value(redis_conn.req_command(&cmd)?)?
            }
        })
    }

    pub fn copy<T: ToString>(&self, cloned_graph_name: T) -> Result<SyncGraph> {
        self.send_command("GRAPH.COPY", &[cloned_graph_name.to_string().into()])?;
        Ok(self.client.open_graph(cloned_graph_name))
    }

    pub fn delete(&self) -> Result<()> {
        self.send_command("GRAPH.DELETE", &[self.graph_name.as_str().into()])?;
        Ok(())
    }

    pub fn slowlog(&self) -> Result<Vec<SlowlogEntry>> {
        let res = self.send_command("GRAPH.SLOWLOG", &[])?.into_vec()?;

        if res.is_empty() {
            return Ok(vec![]);
        }

        Ok(res
            .into_iter()
            .flat_map(FalkorValue::into_vec)
            .flat_map(TryInto::<[FalkorValue; 4]>::try_into)
            .flat_map(SlowlogEntry::from_value_array)
            .collect::<Vec<_>>())
    }

    pub fn slowlog_reset(&self) -> Result<()> {
        self.send_command("GRAPH.SLOWLOG", &["RESET".into()])?;
        Ok(())
    }

    pub fn profile<T: Into<FalkorValue>>(&self, args: T) -> Result<FalkorValue> {
        self.send_command("GRAPH.PROFILE", &[args.into()])
    }

    pub fn explain<T: Into<FalkorValue>>(&self, args: T) -> Result<FalkorValue> {
        self.send_command("GRAPH.EXPLAIN", &[args.into()])
    }

    pub fn query<T: Into<FalkorValue>>(&self, args: T) -> Result<FalkorValue> {
        self.send_command("GRAPH.QUERY", &[args.into(), "--compact".into()])
    }

    pub fn query_readonly<T: Into<FalkorValue>>(&self, args: T) -> Result<FalkorValue> {
        self.send_command("GRAPH.RO_QUERY", &[args.into(), "--compact".into()])
    }
}
