/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::client::blocking::SyncFalkorClient;
use crate::connection::blocking::FalkorSyncConnection;
use crate::value::query_result::QueryResult;
use crate::value::slowlog_entry::SlowlogEntry;
use crate::value::FalkorValue;
use anyhow::Result;
use redis::ConnectionLike;
use std::collections::HashMap;

pub struct SyncGraph<'a> {
    pub(crate) client: &'a SyncFalkorClient,
    pub(crate) graph_name: String,
}

fn construct_query<Q: ToString, T: ToString, Z: ToString>(
    query_str: Q,
    params: Option<&HashMap<T, Z>>,
) -> String {
    params
        .map(|params| {
            params.iter().fold(String::new(), |acc, (key, val)| {
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
        Ok(match conn.as_inner()? {
            #[cfg(feature = "redis")]
            FalkorSyncConnection::Redis(redis_conn) => {
                redis::FromRedisValue::from_owned_redis_value(
                    redis_conn.req_command(
                        redis::cmd(command)
                            .arg(self.graph_name.as_str())
                            .arg(params),
                    )?,
                )?
            }
        })
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

        Ok(res
            .into_iter()
            .flat_map(FalkorValue::into_vec)
            .flat_map(TryInto::<[FalkorValue; 4]>::try_into)
            .flat_map(SlowlogEntry::from_value_array)
            .collect::<Vec<_>>())
    }

    pub fn slowlog_reset(&self) -> Result<()> {
        self.send_command("GRAPH.SLOWLOG", Some("RESET".to_string()))?;
        Ok(())
    }

    pub fn profile_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
    ) -> Result<Vec<String>> {
        let query = construct_query(query_string, params);
        Ok(self
            .send_command("GRAPH.PROFILE", Some(query))
            .and_then(|res| res.into_vec())?
            .into_iter()
            .flat_map(FalkorValue::into_string)
            .map(|step| step.trim().to_string())
            .collect::<Vec<_>>())
    }

    pub fn profile<Q: ToString>(&self, query_string: Q) -> Result<Vec<String>> {
        self.profile_with_params::<Q, &str, &str>(query_string, None)
    }

    pub fn explain_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
    ) -> Result<Vec<String>> {
        let query = construct_query(query_string, params);
        Ok(self
            .send_command("GRAPH.EXPLAIN", Some(query))
            .and_then(|res| res.into_vec())?
            .into_iter()
            .flat_map(FalkorValue::into_string)
            .map(|step| step.trim().to_string())
            .collect::<Vec<_>>())
    }

    pub fn explain<Q: ToString>(&self, query_string: Q) -> Result<Vec<String>> {
        self.explain_with_params::<Q, &str, &str>(query_string, None)
    }

    fn query_with_parser<Q: ToString, T: ToString, Z: ToString>(
        &self,
        command: &str,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
    ) -> Result<QueryResult> {
        let query = construct_query(query_string, params);

        let mut conn = self.client.borrow_connection()?;
        Ok(match conn.as_inner()? {
            #[cfg(feature = "redis")]
            FalkorSyncConnection::Redis(redis_conn) => {
                let redis_val = redis_conn.req_command(
                    redis::cmd(command)
                        .arg(self.graph_name.as_str())
                        .arg(query)
                        .arg("--compact"),
                )?;
                QueryResult::try_from(redis_val)?
            }
        })
    }

    pub fn query_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
        readonly: bool,
    ) -> Result<QueryResult> {
        self.query_with_parser(
            if readonly {
                "GRAPH.RO_QUERY"
            } else {
                "GRAPH.QUERY"
            },
            query_string,
            params,
        )
    }

    pub fn query<Q: ToString>(&self, query_string: Q) -> Result<QueryResult> {
        self.query_with_params::<Q, &str, &str>(query_string, None, false)
    }

    pub fn query_readonly<Q: ToString>(&self, query_string: Q) -> Result<QueryResult> {
        self.query_with_params::<Q, &str, &str>(query_string, None, true)
    }

    pub fn call_procedure<P: ToString, A: ToString, E: ToString>(
        &self,
        procedure: P,
        args: Option<&[A]>,
        yields: Option<&[E]>,
        read_only: Option<bool>,
    ) -> Result<QueryResult> {
        let params = args.map(|args| {
            args.iter()
                .enumerate()
                .fold(HashMap::new(), |mut acc, (idx, param)| {
                    acc.insert(format!("param{idx}"), param.to_string());
                    acc
                })
        });

        let mut query_string = format!(
            "CALL {}({})",
            procedure.to_string(),
            args.unwrap_or_default()
                .iter()
                .map(|element| format!("${}", element.to_string()))
                .collect::<Vec<_>>()
                .join(",")
        );

        let yields = yields.unwrap_or_default();
        if !yields.is_empty() {
            query_string += format!(
                "YIELD {}",
                yields
                    .iter()
                    .map(|element| element.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )
            .as_str();
        }

        self.query_with_params(query_string, params.as_ref(), read_only.unwrap_or_default())
    }
}
