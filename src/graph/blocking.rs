/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::client::blocking::SyncFalkorClient;
use crate::connection::blocking::FalkorSyncConnection;
use crate::error::FalkorDBError;
use crate::graph::schema::GraphSchema;
use crate::graph::utils::generate_procedure_call;
use crate::parser::FalkorParsable;
use crate::value::execution_plan::ExecutionPlan;
use crate::value::query_result::QueryResult;
use crate::value::slowlog_entry::SlowlogEntry;
use crate::value::FalkorValue;
use anyhow::Result;
use redis::ConnectionLike;
use std::collections::HashMap;

pub struct SyncGraph<'a> {
    pub(crate) client: &'a SyncFalkorClient,
    pub(crate) graph_name: String,
    pub(crate) graph_schema: GraphSchema,
}

fn construct_query<Q: ToString, T: ToString, Z: ToString>(
    query_str: Q,
    params: Option<&HashMap<T, Z>>,
) -> String {
    params
        .map(|params| {
            params
                .iter()
                .fold("CYPHER ".to_string(), |acc, (key, val)| {
                    acc + format!("{}={}", key.to_string(), val.to_string()).as_str()
                })
        })
        .unwrap_or_default()
        + query_str.to_string().as_str()
}

impl SyncGraph<'_> {
    pub fn graph_name(&self) -> &str {
        self.graph_name.as_str()
    }

    fn send_command(&self, command: &str, params: Option<String>) -> Result<FalkorValue> {
        let mut conn = self.client.borrow_connection()?;
        conn.send_command(Some(self.graph_name.clone()), command, params)
    }

    pub fn copy<T: ToString>(&self, cloned_graph_name: T) -> Result<SyncGraph> {
        self.send_command("GRAPH.COPY", Some(cloned_graph_name.to_string()))?;
        Ok(self.client.open_graph(cloned_graph_name))
    }

    pub fn delete(&self) -> Result<()> {
        self.send_command("GRAPH.DELETE", None)?;
        Ok(())
    }

    pub fn slowlog(&self) -> Result<Vec<SlowlogEntry>> {
        let res = self.send_command("GRAPH.SLOWLOG", None)?.into_vec()?;

        if res.is_empty() {
            return Ok(vec![]);
        }

        let mut slowlog_entries = Vec::with_capacity(res.len());
        for entry_raw in res {
            slowlog_entries.push(SlowlogEntry::from_value_array(
                entry_raw
                    .into_vec()?
                    .try_into()
                    .map_err(|_| FalkorDBError::ParsingSlowlogEntryElementCount)?,
            )?);
        }

        Ok(slowlog_entries)
    }

    pub fn slowlog_reset(&self) -> Result<()> {
        self.send_command("GRAPH.SLOWLOG", Some("RESET".to_string()))?;
        Ok(())
    }

    pub fn profile_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
    ) -> Result<ExecutionPlan> {
        let query = construct_query(query_string, params);

        ExecutionPlan::try_from(self.send_command("GRAPH.PROFILE", Some(query))?)
    }

    pub fn profile<Q: ToString>(&self, query_string: Q) -> Result<ExecutionPlan> {
        self.profile_with_params::<Q, &str, &str>(query_string, None)
    }

    pub fn explain_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
    ) -> Result<ExecutionPlan> {
        let query = construct_query(query_string, params);
        ExecutionPlan::try_from(self.send_command("GRAPH.EXPLAIN", Some(query))?)
    }

    pub fn explain<Q: ToString>(&self, query_string: Q) -> Result<ExecutionPlan> {
        self.explain_with_params::<Q, &str, &str>(query_string, None)
    }

    fn query_with_parser<Q: ToString, T: ToString, Z: ToString, P: FalkorParsable>(
        &self,
        command: &str,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
        timeout: Option<u64>,
    ) -> Result<P> {
        let query = construct_query(query_string, params);

        let mut conn = self.client.borrow_connection()?;
        let falkor_result = match conn.as_inner()? {
            #[cfg(feature = "redis")]
            FalkorSyncConnection::Redis(redis_conn) => {
                use redis::FromRedisValue as _;
                let redis_val = redis_conn.req_command(
                    redis::cmd(command)
                        .arg(self.graph_name.as_str())
                        .arg(query)
                        .arg("--compact")
                        .arg(timeout.map(|timeout| format!("timeout {timeout}"))),
                )?;
                FalkorValue::from_owned_redis_value(redis_val)?
            }
        };

        P::from_falkor_value(falkor_result, &self.graph_schema, &mut conn)
    }

    pub fn query_with_params<Q: ToString, T: ToString, Z: ToString, P: FalkorParsable>(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
        readonly: bool,
        timeout: Option<u64>,
    ) -> Result<P> {
        self.query_with_parser(
            if readonly {
                "GRAPH.RO_QUERY"
            } else {
                "GRAPH.QUERY"
            },
            query_string,
            params,
            timeout,
        )
    }

    pub fn query<Q: ToString>(&self, query_string: Q, timeout: Option<u64>) -> Result<QueryResult> {
        self.query_with_params::<Q, &str, &str, QueryResult>(query_string, None, false, timeout)
    }

    pub fn query_readonly<Q: ToString>(
        &self,
        query_string: Q,
        timeout: Option<u64>,
    ) -> Result<QueryResult> {
        self.query_with_params::<Q, &str, &str, QueryResult>(query_string, None, true, timeout)
    }

    pub fn call_procedure<C: ToString, P: FalkorParsable>(
        &self,
        procedure: C,
        args: Option<&[String]>,
        yields: Option<&[String]>,
        read_only: Option<bool>,
        timeout: Option<u64>,
    ) -> Result<P> {
        let (query_string, params) = generate_procedure_call(procedure, args, yields);

        self.query_with_params(
            query_string,
            params.as_ref(),
            read_only.unwrap_or_default(),
            timeout,
        )
    }

    pub fn list_indices(&self) -> Result<FalkorValue> {
        let query_res = self
            .call_procedure::<&str, FalkorValue>("DB.INDEXES", None, None, None, None)?
            .into_vec()?;

        for item in query_res {
            log::info!("{item:?}");
        }
        Ok(FalkorValue::None)
    }

    pub fn list_constraints(&self) -> Result<FalkorValue> {
        let query_res = self
            .call_procedure::<&str, FalkorValue>("DB.CONSTRAINTS", None, None, None, None)?
            .into_vec()?;

        for item in query_res {
            log::info!("{item:?}");
        }
        Ok(FalkorValue::None)
    }
}
