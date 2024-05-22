/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use super::utils::{construct_query, generate_procedure_call};
use crate::{
    client::blocking::FalkorSyncClientInner, connection::blocking::FalkorSyncConnection,
    Constraint, ExecutionPlan, FalkorDBError, FalkorParsable, FalkorValue, QueryResult,
    SlowlogEntry, SyncGraphSchema,
};
use anyhow::Result;
use redis::ConnectionLike;
use std::{collections::HashMap, sync::Arc};

/// The main graph API, this allows the user to perform graph operations while exposing as little details as possible.
///
/// # Thread Safety
/// This struct is fully thread safe, it can be cloned and passed within threads without constraints,
/// Its API uses only immutable references
#[derive(Clone)]
pub struct SyncGraph {
    pub(crate) client: Arc<FalkorSyncClientInner>,
    pub(crate) graph_name: String,
    /// Provides user with access to the current graph schema,
    /// which contains a safe cache of id to labels/properties/relationship maps
    pub graph_schema: SyncGraphSchema,
}

impl SyncGraph {
    /// Returns the name of the graph for which this API performs operations.
    ///
    /// # Returns
    /// The graph name as a string slice, without cloning.
    pub fn graph_name(&self) -> &str {
        self.graph_name.as_str()
    }

    fn send_command(&self, command: &str, params: Option<String>) -> Result<FalkorValue> {
        let mut conn = self.client.borrow_connection()?;
        conn.send_command(Some(self.graph_name.clone()), command, params)
    }

    /// Deletes the graph stored in the database, and drop all the schema caches.
    /// NOTE: This still maintains the graph API, operations are still viable.
    pub fn delete(&self) -> Result<()> {
        self.send_command("GRAPH.DELETE", None)?;
        self.graph_schema.clear();
        Ok(())
    }

    /// Retrieves the slowlog data, which contains info about the N slowest queries.
    ///
    /// # Returns
    /// A [`Vec`] of [`SlowlogEntry`], providing information about each query.
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

    /// Resets the slowlog, all query time data will be cleared.
    pub fn slowlog_reset(&self) -> Result<()> {
        self.send_command("GRAPH.SLOWLOG", Some("RESET".to_string()))?;
        Ok(())
    }

    /// Returns an [`ExecutionPlan`] object for the selected query,
    /// showing how long each step took to perform.
    /// This function variant allows adding extra parameters after the query
    ///
    /// # Arguments
    /// * `query_string`: The query to profile
    /// * `params`: a map of parameters and values, note that all keys should be of the same type, and all values should be of the same type.
    ///
    /// # Returns
    /// An [`ExecutionPlan`], which can provide info about each step, or a plaintext explanation of the whole thing for printing.
    pub fn profile_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<HashMap<T, Z>>,
    ) -> Result<ExecutionPlan> {
        let query = construct_query(query_string, params);

        ExecutionPlan::try_from(self.send_command("GRAPH.PROFILE", Some(query))?)
    }

    /// Returns an [`ExecutionPlan`] object for the selected query,
    /// showing how long each step took to perform.
    ///
    /// # Arguments
    /// * `query_string`: The query to profile
    ///
    /// # Returns
    /// An [`ExecutionPlan`], which can provide info about each step, or a plaintext explanation of the whole thing for printing.
    pub fn profile<Q: ToString>(&self, query_string: Q) -> Result<ExecutionPlan> {
        self.profile_with_params::<Q, &str, &str>(query_string, None)
    }

    /// Returns an [`ExecutionPlan`] object for the selected query,
    /// showing the internals steps the database will go through to perform the query.
    /// This function variant allows adding extra parameters after the query
    ///
    /// # Arguments
    /// * `query_string`: The query to explain
    /// * `params`: a map of parameters and values, note that all keys should be of the same type, and all values should be of the same type.
    ///
    /// # Returns
    /// An [`ExecutionPlan`], which can provide info about each step, or a plaintext explanation of the whole thing for printing.
    pub fn explain_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<HashMap<T, Z>>,
    ) -> Result<ExecutionPlan> {
        let query = construct_query(query_string, params);
        ExecutionPlan::try_from(self.send_command("GRAPH.EXPLAIN", Some(query))?)
    }

    /// Returns an [`ExecutionPlan`] object for the selected query,
    /// showing the internals steps the database will go through to perform the query.
    ///
    /// # Arguments
    /// * `query_string`: The query to explain
    ///
    /// # Returns
    /// An [`ExecutionPlan`], which can provide info about each step, or a plaintext explanation of the whole thing for printing.
    pub fn explain<Q: ToString>(&self, query_string: Q) -> Result<ExecutionPlan> {
        self.explain_with_params::<Q, &str, &str>(query_string, None)
    }

    fn query_inner<Q: ToString, T: ToString, Z: ToString, P: FalkorParsable>(
        &self,
        command: &str,
        query_string: Q,
        params: Option<HashMap<T, Z>>,
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

    /// Run a query on the graph
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    /// * `timeout`: Specify how long should the query run before aborting.
    ///
    /// # Returns
    /// A [`QueryResult`] object, containing the headers, statistics and the result set for the query
    pub fn query<Q: ToString>(&self, query_string: Q, timeout: Option<u64>) -> Result<QueryResult> {
        self.query_inner::<Q, &str, &str, QueryResult>("GRAPH.QUERY", query_string, None, timeout)
    }

    /// Run a query on the graph
    /// This function variant allows adding extra parameters after the query
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    /// * `timeout`: Specify how long should the query run before aborting.
    /// * `params`: a map of parameters and values, note that all keys should be of the same type, and all values should be of the same type.
    ///
    /// # Returns
    /// A [`QueryResult`] object, containing the headers, statistics and the result set for the query
    pub fn query_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        timeout: Option<u64>,
        params: HashMap<T, Z>,
    ) -> Result<QueryResult> {
        self.query_inner("GRAPH.QUERY", query_string, Some(params), timeout)
    }

    /// Run a query on the graph
    /// Read-only queries are more limited with the operations they are allowed to perform.
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    /// * `timeout`: Specify how long should the query run before aborting.
    ///
    /// # Returns
    /// A [`QueryResult`] object, containing the headers, statistics and the result set for the query
    pub fn query_readonly<Q: ToString>(
        &self,
        query_string: Q,
        timeout: Option<u64>,
    ) -> Result<QueryResult> {
        self.query_inner::<Q, &str, &str, QueryResult>(
            "GRAPH.QUERY_RO",
            query_string,
            None,
            timeout,
        )
    }

    /// Run a read-only query on the graph
    /// Read-only queries are more limited with the operations they are allowed to perform.
    /// This function variant allows adding extra parameters after the query
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    /// * `timeout`: Specify how long should the query run before aborting.
    /// * `params`: a map of parameters and values, note that all keys should be of the same type, and all values should be of the same type.
    ///
    /// # Returns
    /// A [`QueryResult`] object, containing the headers, statistics and the result set for the query
    pub fn query_readonly_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        timeout: Option<u64>,
        params: HashMap<T, Z>,
    ) -> Result<QueryResult> {
        self.query_inner("GRAPH.QUERY_RO", query_string, Some(params), timeout)
    }

    /// Run a query which calls a procedure on the graph, read-only, or otherwise.
    /// Read-only queries are more limited with the operations they are allowed to perform.
    /// This function allows adding extra parameters after the query, and adding a YIELD block afterwards
    ///
    /// # Arguments
    /// * `procedure`: The procedure to call
    /// * `args`: An optional slice of strings containing the parameters.
    /// * `yields`: The optional yield block arguments.
    /// * `read_only`: Whether this procedure is read-only.
    /// * `timeout`: If provided, the query will abort if overruns the timeout.
    ///
    /// # Returns
    /// A caller-provided type which implements [`FalkorParsable`]
    pub fn call_procedure<C: ToString, P: FalkorParsable>(
        &self,
        procedure: C,
        args: Option<&[String]>,
        yields: Option<&[String]>,
        read_only: bool,
        timeout: Option<u64>,
    ) -> Result<P> {
        let (query_string, params) = generate_procedure_call(procedure, args, yields);

        self.query_inner(
            read_only
                .then_some("GRAPH.QUERY_RO")
                .unwrap_or("GRAPH.QUERY"),
            query_string,
            params,
            timeout,
        )
    }

    /// Calls the DB.INDICES procedure on the graph
    /// TODO: <What does this actually do?>
    pub fn list_indices(&self) -> Result<FalkorValue> {
        let query_res = self
            .call_procedure::<&str, FalkorValue>("DB.INDEXES", None, None, true, None)?
            .into_vec()?;

        for item in query_res {
            log::info!("{item:?}");
        }
        Ok(FalkorValue::None)
    }

    /// Calls the DB.CONSTRAINTS procedure on the graph, returning an array of the graph's constraints
    ///
    /// # Returns
    /// A [`Vec`] of [`Constraint`]s
    pub fn list_constraints(&self) -> Result<Vec<Constraint>> {
        let mut conn = self.client.borrow_connection()?;
        let query_res = self
            .call_procedure::<&str, FalkorValue>("DB.CONSTRAINTS", None, None, true, None)?
            .into_vec()?;

        let mut constraints_vec = Vec::with_capacity(query_res.len());
        for item in query_res {
            constraints_vec.push(Constraint::from_falkor_value(
                item,
                &self.graph_schema,
                &mut conn,
            )?);
        }

        Ok(constraints_vec)
    }
}
