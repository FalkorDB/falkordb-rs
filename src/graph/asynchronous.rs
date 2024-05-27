/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use super::utils::{construct_query, generate_procedure_call};
use crate::{
    client::asynchronous::FalkorAsyncClientInner, AsyncGraphSchema, Constraint, ConstraintType,
    EntityType, ExecutionPlan, FalkorAsyncConnection, FalkorAsyncParseable, FalkorDBError,
    FalkorIndex, FalkorValue, IndexType, QueryResult, SlowlogEntry,
};
use anyhow::Result;
use std::{collections::HashMap, fmt::Display, sync::Arc};

/// The main graph API, this allows the user to perform graph operations while exposing as little details as possible.
///
/// # Thread Safety
/// This struct is fully thread safe, it can be cloned and passed within threads without constraints,
/// Its API uses only immutable references
#[derive(Clone)]
pub struct AsyncGraph {
    pub(crate) client: Arc<FalkorAsyncClientInner>,
    pub(crate) graph_name: String,
    /// Provides user with access to the current graph schema,
    /// which contains a safe cache of id to labels/properties/relationship maps
    pub graph_schema: AsyncGraphSchema,
}

impl AsyncGraph {
    /// Returns the name of the graph for which this API performs operations.
    ///
    /// # Returns
    /// The graph name as a string slice, without cloning.
    pub fn graph_name(&self) -> &str {
        self.graph_name.as_str()
    }

    async fn send_command(
        &self,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[String]>,
    ) -> Result<FalkorValue> {
        let mut conn = self.client.get_connection().await?;
        conn.send_command(Some(self.graph_name.clone()), command, subcommand, params)
            .await
    }

    /// Deletes the graph stored in the database, and drop all the schema caches.
    /// NOTE: This still maintains the graph API, operations are still viable.
    pub async fn delete(&self) -> Result<()> {
        self.send_command("GRAPH.DELETE", None, None).await?;
        Ok(())
    }

    /// Retrieves the slowlog data, which contains info about the N slowest queries.
    ///
    /// # Returns
    /// A [`Vec`] of [`SlowlogEntry`], providing information about each query.
    pub async fn slowlog(&self) -> Result<Vec<SlowlogEntry>> {
        let res = self
            .send_command("GRAPH.SLOWLOG", None, None)
            .await?
            .into_vec()?;

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

    /// Resets the slowlog, all query time data will be cleared.
    pub async fn slowlog_reset(&self) -> Result<FalkorValue> {
        self.send_command("GRAPH.SLOWLOG", Some("RESET"), None)
            .await
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
    pub async fn profile_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
    ) -> Result<ExecutionPlan> {
        let query = construct_query(query_string, params);

        ExecutionPlan::try_from(
            self.send_command("GRAPH.PROFILE", None, Some(&[query]))
                .await?,
        )
        .map_err(Into::into)
    }

    /// Returns an [`ExecutionPlan`] object for the selected query,
    /// showing how long each step took to perform.
    ///
    /// # Arguments
    /// * `query_string`: The query to profile
    ///
    /// # Returns
    /// An [`ExecutionPlan`], which can provide info about each step, or a plaintext explanation of the whole thing for printing.
    pub async fn profile<Q: ToString>(
        &self,
        query_string: Q,
    ) -> Result<ExecutionPlan> {
        self.profile_with_params::<Q, &str, &str>(query_string, None)
            .await
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
    pub async fn explain_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
    ) -> Result<ExecutionPlan> {
        let query = construct_query(query_string, params);
        ExecutionPlan::try_from(
            self.send_command("GRAPH.EXPLAIN", None, Some(&[query]))
                .await?,
        )
        .map_err(Into::into)
    }

    /// Returns an [`ExecutionPlan`] object for the selected query,
    /// showing the internals steps the database will go through to perform the query.
    ///
    /// # Arguments
    /// * `query_string`: The query to explain
    ///
    /// # Returns
    /// An [`ExecutionPlan`], which can provide info about each step, or a plaintext explanation of the whole thing for printing.
    pub async fn explain<Q: ToString>(
        &self,
        query_string: Q,
    ) -> Result<ExecutionPlan> {
        self.explain_with_params::<Q, &str, &str>(query_string, None)
            .await
    }

    async fn query_inner<Q: ToString, T: ToString, Z: ToString, P: FalkorAsyncParseable>(
        &self,
        command: &str,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
        timeout: Option<i64>,
    ) -> Result<P> {
        let query = construct_query(query_string, params);

        let conn = self.client.get_connection().await?;
        let falkor_result = match conn.clone() {
            #[cfg(feature = "redis")]
            FalkorAsyncConnection::Redis(mut redis_conn) => {
                use redis::FromRedisValue as _;
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

        P::from_falkor_value_async(falkor_result, &self.graph_schema, conn).await
    }

    /// Run a query on the graph
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    /// * `timeout`: Specify how long should the query run before aborting.
    ///
    /// # Returns
    /// A [`QueryResult`] object, containing the headers, statistics and the result set for the query
    pub async fn query<Q: Display>(
        &self,
        query_string: Q,
        timeout: Option<i64>,
    ) -> Result<QueryResult> {
        self.query_inner::<Q, &str, &str, QueryResult>("GRAPH.QUERY", query_string, None, timeout)
            .await
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
    pub async fn query_with_params<Q: Display, T: Display, Z: Display>(
        &self,
        query_string: Q,
        timeout: Option<i64>,
        params: &HashMap<T, Z>,
    ) -> Result<QueryResult> {
        self.query_inner("GRAPH.QUERY", query_string, Some(params), timeout)
            .await
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
    pub async fn query_readonly<Q: Display>(
        &self,
        query_string: Q,
        timeout: Option<i64>,
    ) -> Result<QueryResult> {
        self.query_inner::<Q, &str, &str, QueryResult>(
            "GRAPH.QUERY_RO",
            query_string,
            None,
            timeout,
        )
        .await
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
    pub async fn query_readonly_with_params<Q: ToString, T: ToString, Z: ToString>(
        &self,
        query_string: Q,
        timeout: Option<i64>,
        params: Option<&HashMap<T, Z>>,
    ) -> Result<QueryResult> {
        self.query_inner("GRAPH.QUERY_RO", query_string, params, timeout)
            .await
    }

    /// Run a query which calls a procedure on the graph, read-only, or otherwise.
    /// Read-only queries are more limited with the operations they are allowed to perform.
    /// This function allows adding extra parameters after the query, and adding a YIELD block afterward
    ///
    /// # Arguments
    /// * `procedure`: The procedure to call
    /// * `args`: An optional slice of strings containing the parameters.
    /// * `yields`: The optional yield block arguments.
    /// * `read_only`: Whether this procedure is read-only.
    /// * `timeout`: If provided, the query will abort if overruns the timeout.
    ///
    /// # Returns
    /// A caller-provided type which implements [`FalkorAsyncParseable`]
    pub async fn call_procedure<C: ToString, P: FalkorAsyncParseable>(
        &self,
        procedure: C,
        args: Option<&[&str]>,
        yields: Option<&[&str]>,
        read_only: bool,
        timeout: Option<i64>,
    ) -> Result<P> {
        let (query_string, params) = generate_procedure_call(procedure, args, yields);

        self.query_inner(
            if read_only {
                "GRAPH.QUERY_RO"
            } else {
                "GRAPH.QUERY"
            },
            query_string,
            params.as_ref(),
            timeout,
        )
        .await
    }

    /// Calls the DB.INDICES procedure on the graph, returning all the indexing methods currently used
    ///
    /// # Returns
    /// A [`Vec`] of [`Index`]
    pub async fn list_indices(&self) -> Result<Vec<FalkorIndex>> {
        let conn = self.client.get_connection().await?;
        let [_, indices, _]: [FalkorValue; 3] = self
            .call_procedure::<&str, FalkorValue>("DB.INDEXES", None, None, false, None)
            .await?
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        let indices = indices.into_vec()?;

        let mut out_vec = Vec::with_capacity(indices.len());
        for index in indices {
            out_vec.push(
                FalkorIndex::from_falkor_value_async(index, &self.graph_schema, conn.clone())
                    .await?,
            );
        }

        Ok(out_vec)
    }

    pub async fn create_index<L: ToString, P: ToString>(
        &self,
        index_field_type: IndexType,
        entity_type: EntityType,
        label: L,
        properties: &[P],
        options: Option<&HashMap<String, String>>,
    ) -> Result<QueryResult> {
        // Create index from these properties
        let properties_string = properties
            .iter()
            .map(|element| format!("l.{}", element.to_string()))
            .collect::<Vec<_>>()
            .join(", ");

        let pattern = match entity_type {
            EntityType::Node => format!("(l:{})", label.to_string()),
            EntityType::Edge => format!("()-[l:{}]->()", label.to_string()),
        };

        let idx_type = match index_field_type {
            IndexType::Range => "",
            IndexType::Vector => "VECTOR ",
            IndexType::Fulltext => "FULLTEXT ",
        }
        .to_string();

        let options_string = options
            .map(|hashmap| {
                hashmap
                    .into_iter()
                    .map(|(key, val)| format!("'{key}':'{val}'"))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .map(|options_string| format!(" OPTIONS {{ {} }}", options_string))
            .unwrap_or_default();

        let full_query = format!(
            "CREATE {idx_type}INDEX FOR {pattern} ON ({}){}",
            properties_string, options_string
        );
        self.query(full_query, None).await
    }

    /// Drop an existing index, by specifying its type, entity, label and specific properties
    ///
    /// # Arguments
    /// * `index_field_type`
    pub async fn drop_index<L: ToString, P: ToString>(
        &self,
        index_field_type: IndexType,
        entity_type: EntityType,
        label: L,
        properties: &[P],
    ) -> Result<QueryResult> {
        let properties_string = properties
            .iter()
            .map(|element| format!("e.{}", element.to_string()))
            .collect::<Vec<_>>()
            .join(", ");

        let pattern = match entity_type {
            EntityType::Node => format!("(e:{})", label.to_string()),
            EntityType::Edge => format!("()-[e:{}]->()", label.to_string()),
        };

        let idx_type = match index_field_type {
            IndexType::Range => "",
            IndexType::Vector => "VECTOR",
            IndexType::Fulltext => "FULLTEXT",
        }
        .to_string();

        self.query(
            format!(
                "DROP {idx_type} INDEX for {pattern} ON ({})",
                properties_string
            ),
            None,
        )
        .await
    }

    /// Calls the DB.CONSTRAINTS procedure on the graph, returning an array of the graph's constraints
    ///
    /// # Returns
    /// A [`Vec`] of [`Constraint`]s
    pub async fn list_constraints(&self) -> Result<Vec<Constraint>> {
        let conn = self.client.get_connection().await?;
        let [_, query_res, _]: [FalkorValue; 3] = self
            .call_procedure::<&str, FalkorValue>("DB.CONSTRAINTS", None, None, false, None)
            .await?
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        let query_res = query_res.into_vec()?;

        let mut constraints_vec = Vec::with_capacity(query_res.len());
        for item in query_res {
            constraints_vec.push(
                Constraint::from_falkor_value_async(item, &self.graph_schema, conn.clone()).await?,
            );
        }

        Ok(constraints_vec)
    }

    /// Creates a new constraint for this graph, making the provided properties mandatory
    ///
    /// # Arguments
    /// * `entity_type`: Whether to apply this constraint on nodes or relationships.
    /// * `label`: Entities with this label will have this constraint applied to them.
    /// * `properties`: A slice of the names of properties this constraint will apply to.
    pub async fn create_mandatory_constraint<L: ToString, P: ToString>(
        &self,
        entity_type: EntityType,
        label: L,
        properties: &[P],
    ) -> Result<FalkorValue> {
        let mut params = Vec::with_capacity(5 + properties.len());
        params.extend([
            "MANDATORY".to_string(),
            entity_type.to_string(),
            label.to_string(),
            "PROPERTIES".to_string(),
            properties.len().to_string(),
        ]);
        params.extend(properties.iter().map(|property| property.to_string()));

        self.send_command("GRAPH.CONSTRAINT", Some("CREATE"), Some(params.as_slice()))
            .await
    }

    /// Creates a new constraint for this graph, making the provided properties unique
    ///
    /// # Arguments
    /// * `entity_type`: Whether to apply this constraint on nodes or relationships.
    /// * `label`: Entities with this label will have this constraint applied to them.
    /// * `properties`: A slice of the names of properties this constraint will apply to.
    pub async fn create_unique_constraint<P: ToString>(
        &self,
        entity_type: EntityType,
        label: String,
        properties: &[P],
    ) -> Result<FalkorValue> {
        self.create_index(
            IndexType::Range,
            entity_type,
            label.as_str(),
            properties,
            None,
        )
        .await?;

        let mut params: Vec<String> = Vec::with_capacity(5 + properties.len());
        params.extend([
            "UNIQUE".to_string(),
            entity_type.to_string(),
            label.to_string(),
            "PROPERTIES".to_string(),
            properties.len().to_string(),
        ]);
        params.extend(properties.into_iter().map(|property| property.to_string()));

        // create constraint using index
        self.send_command("GRAPH.CONSTRAINT", Some("CREATE"), Some(params.as_slice()))
            .await
    }

    /// Drop an existing constraint from the graph
    ///
    /// # Arguments
    /// * `constraint_type`: Which type of constraint to remove.
    /// * `entity_type`: Whether this constraint exists on nodes or relationships.
    /// * `label`: Remove the constraint from entities with this label.
    /// * `properties`: A slice of the names of properties to remove the constraint from.
    pub async fn drop_constraint<L: ToString, P: ToString>(
        &self,
        constraint_type: ConstraintType,
        entity_type: EntityType,
        label: L,
        properties: &[P],
    ) -> Result<FalkorValue> {
        let mut params = Vec::with_capacity(5 + properties.len());
        params.extend([
            constraint_type.to_string(),
            entity_type.to_string(),
            label.to_string(),
            "PROPERTIES".to_string(),
            properties.len().to_string(),
        ]);
        params.extend(properties.iter().map(|property| property.to_string()));

        self.send_command("GRAPH.CONSTRAINT", Some("DROP"), Some(params.as_slice()))
            .await
    }
}
