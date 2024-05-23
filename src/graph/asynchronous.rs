/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use super::utils::{construct_query, generate_procedure_call};
use crate::{
    AsyncGraphSchema, ExecutionPlan, FalkorAsyncConnection, FalkorAsyncParseable, FalkorDBError,
    FalkorValue, QueryResult, SlowlogEntry,
};
use anyhow::Result;
use std::collections::HashMap;

pub struct AsyncGraph {
    pub(crate) graph_name: String,
    pub(crate) connection: FalkorAsyncConnection,
    pub(crate) graph_schema: AsyncGraphSchema,
}

impl AsyncGraph {
    pub fn graph_name(&self) -> &str {
        self.graph_name.as_str()
    }

    async fn send_command(
        &self,
        command: &str,
        params: Option<String>,
    ) -> Result<FalkorValue> {
        let mut conn = self.connection.clone();
        conn.send_command(Some(self.graph_name.clone()), command, params)
            .await
    }

    pub async fn delete(&self) -> Result<()> {
        self.send_command("GRAPH.DELETE", None).await?;
        Ok(())
    }

    pub async fn slowlog(&self) -> Result<Vec<SlowlogEntry>> {
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
                    .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?,
            )?);
        }

        Ok(slowlog_entries)
    }

    pub async fn slowlog_reset(&self) -> Result<()> {
        self.send_command("GRAPH.SLOWLOG", Some("RESET".to_string()))
            .await?;
        Ok(())
    }

    pub async fn profile_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
    ) -> Result<ExecutionPlan> {
        let query = construct_query(query_string, params);

        ExecutionPlan::try_from(self.send_command("GRAPH.PROFILE", Some(query)).await?)
            .map_err(Into::into)
    }

    pub async fn profile<Q: ToString>(
        &self,
        query_string: Q,
    ) -> Result<ExecutionPlan> {
        self.profile_with_params::<Q, &str, &str>(query_string, None)
            .await
    }

    pub async fn explain_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
    ) -> Result<ExecutionPlan> {
        let query = construct_query(query_string, params);
        ExecutionPlan::try_from(self.send_command("GRAPH.EXPLAIN", Some(query)).await?)
            .map_err(Into::into)
    }

    pub async fn explain<Q: ToString>(
        &self,
        query_string: Q,
    ) -> Result<ExecutionPlan> {
        self.explain_with_params::<Q, &str, &str>(query_string, None)
            .await
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
        .await
    }

    pub async fn query<Q: ToString>(
        &self,
        query_string: Q,
        timeout: Option<u64>,
    ) -> Result<QueryResult> {
        self.query_with_params::<Q, &str, &str, QueryResult>(query_string, None, false, timeout)
            .await
    }

    pub async fn query_readonly<Q: ToString>(
        &self,
        query_string: Q,
        timeout: Option<u64>,
    ) -> Result<QueryResult> {
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
    ) -> Result<P> {
        let (query_string, params) = generate_procedure_call(procedure, args, yields);

        self.query_with_params(
            query_string,
            params.as_ref(),
            read_only.unwrap_or_default(),
            timeout,
        )
        .await
    }

    pub async fn list_indices(&self) -> Result<FalkorValue> {
        let query_res = self
            .call_procedure::<&str, FalkorValue>("DB.INDEXES", None, None, None, None)
            .await?
            .into_vec()?;

        for item in query_res {
            log::info!("{item:?}");
        }
        Ok(FalkorValue::None)
    }

    pub async fn list_constraints(&self) -> Result<FalkorValue> {
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
