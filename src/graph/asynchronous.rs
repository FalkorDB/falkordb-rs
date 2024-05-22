/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use super::utils::{construct_query, generate_procedure_call};
use crate::{
    AsyncFalkorClient, AsyncGraphSchema, ExecutionPlan, FalkorAsyncConnection,
    FalkorAsyncParseable, FalkorDBError, FalkorValue, QueryResult, SlowlogEntry,
};
use std::collections::HashMap;

pub struct AsyncGraph<'a> {
    pub(crate) client: &'a AsyncFalkorClient,
    pub(crate) graph_name: String,
    pub(crate) graph_schema: AsyncGraphSchema,
}

impl AsyncGraph<'_> {
    pub fn graph_name(&self) -> &str {
        self.graph_name.as_str()
    }

    async fn send_command(
        &self,
        command: &str,
        params: Option<String>,
    ) -> anyhow::Result<FalkorValue> {
        let mut conn = self.client.clone_connection();
        conn.send_command(Some(self.graph_name.clone()), command, params)
            .await
    }

    pub async fn copy<T: ToString>(&self, cloned_graph_name: T) -> anyhow::Result<AsyncGraph> {
        self.send_command("GRAPH.COPY", Some(cloned_graph_name.to_string()))
            .await?;
        Ok(self.client.open_graph(cloned_graph_name).await)
    }

    pub async fn delete(&self) -> anyhow::Result<()> {
        self.send_command("GRAPH.DELETE", None).await?;
        Ok(())
    }

    pub async fn slowlog(&self) -> anyhow::Result<Vec<SlowlogEntry>> {
        let res = self.send_command("GRAPH.SLOWLOG", None).await?.into_vec()?;

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

    pub async fn slowlog_reset(&self) -> anyhow::Result<()> {
        self.send_command("GRAPH.SLOWLOG", Some("RESET".to_string()))
            .await?;
        Ok(())
    }

    pub async fn profile_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
    ) -> anyhow::Result<ExecutionPlan> {
        let query = construct_query(query_string, params);

        ExecutionPlan::try_from(self.send_command("GRAPH.PROFILE", Some(query)).await?)
    }

    pub async fn profile<Q: ToString>(&self, query_string: Q) -> anyhow::Result<ExecutionPlan> {
        self.profile_with_params::<Q, &str, &str>(query_string, None)
            .await
    }

    pub async fn explain_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
    ) -> anyhow::Result<ExecutionPlan> {
        let query = construct_query(query_string, params);
        ExecutionPlan::try_from(self.send_command("GRAPH.EXPLAIN", Some(query)).await?)
    }

    pub async fn explain<Q: ToString>(&self, query_string: Q) -> anyhow::Result<ExecutionPlan> {
        self.explain_with_params::<Q, &str, &str>(query_string, None)
            .await
    }

    async fn query_with_parser<Q: ToString, T: ToString, Z: ToString, P: FalkorAsyncParseable>(
        &self,
        command: &str,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
        timeout: Option<u64>,
    ) -> anyhow::Result<P> {
        let query = construct_query(query_string, params);

        let mut conn = self.client.clone_connection();
        let falkor_result = match &mut conn {
            #[cfg(feature = "redis")]
            FalkorAsyncConnection::Redis(redis_conn) => {
                use redis::FromRedisValue;
                let redis_val = redis_conn
                    .send_packed_command(
                        redis::cmd(command)
                            .arg(self.graph_name.as_str())
                            .arg(query)
                            .arg("--compact")
                            .arg(timeout.map(|timeout| format!("timeout {timeout}"))),
                    )
                    .await?;
                FalkorValue::from_owned_redis_value(redis_val)?
            }
        };

        P::from_falkor_value_async(falkor_result, &self.graph_schema, &mut conn).await
    }

    pub async fn query_with_params<
        Q: ToString,
        T: ToString,
        Z: ToString,
        P: FalkorAsyncParseable,
    >(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
        readonly: bool,
        timeout: Option<u64>,
    ) -> anyhow::Result<P> {
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
        .await
    }

    pub async fn query<Q: ToString>(
        &self,
        query_string: Q,
        timeout: Option<u64>,
    ) -> anyhow::Result<QueryResult> {
        self.query_with_params::<Q, &str, &str, QueryResult>(query_string, None, false, timeout)
            .await
    }

    pub async fn query_readonly<Q: ToString>(
        &self,
        query_string: Q,
        timeout: Option<u64>,
    ) -> anyhow::Result<QueryResult> {
        self.query_with_params::<Q, &str, &str, QueryResult>(query_string, None, true, timeout)
            .await
    }

    pub async fn call_procedure<C: ToString, P: FalkorAsyncParseable>(
        &self,
        procedure: C,
        args: Option<&[String]>,
        yields: Option<&[String]>,
        read_only: Option<bool>,
        timeout: Option<u64>,
    ) -> anyhow::Result<P> {
        let (query_string, params) = generate_procedure_call(procedure, args, yields);

        self.query_with_params(
            query_string,
            params.as_ref(),
            read_only.unwrap_or_default(),
            timeout,
        )
        .await
    }

    pub async fn list_indices(&self) -> anyhow::Result<FalkorValue> {
        let query_res = self
            .call_procedure::<&str, FalkorValue>("DB.INDEXES", None, None, None, None)
            .await?
            .into_vec()?;

        for item in query_res {
            log::info!("{item:?}");
        }
        Ok(FalkorValue::None)
    }

    pub async fn list_constraints(&self) -> anyhow::Result<FalkorValue> {
        let query_res = self
            .call_procedure::<&str, FalkorValue>("DB.CONSTRAINTS", None, None, None, None)
            .await?
            .into_vec()?;

        for item in query_res {
            for sub_item in item.into_vec()? {
                log::info!("{sub_item:?}");
            }
        }
        Ok(FalkorValue::None)
    }
}
