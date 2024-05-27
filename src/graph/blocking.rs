/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::blocking::FalkorSyncClientInner,
    graph::utils::{construct_query, generate_procedure_call},
    Constraint, ConstraintType, EntityType, ExecutionPlan, FalkorDBError, FalkorIndex,
    FalkorParsable, FalkorValue, IndexType, QueryResult, SlowlogEntry, SyncGraphSchema,
};
use anyhow::Result;
use std::{collections::HashMap, fmt::Display, sync::Arc};

/// The main graph API, this allows the user to perform graph operations while exposing as little details as possible.
/// # Thread Safety
/// This struct is NOT thread safe, and synchronization is up to the user.
/// Also, graph schema is not shared between instances of SyncGraph, even with the same name
#[derive(Clone)]
pub struct SyncGraph {
    pub(crate) client: Arc<FalkorSyncClientInner>,
    pub(crate) graph_name: String,
    /// Provides user with access to the current graph schema,
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

    fn send_command(
        &self,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[String]>,
    ) -> Result<FalkorValue> {
        let mut conn = self.client.borrow_connection()?;
        conn.send_command(Some(self.graph_name.as_str()), command, subcommand, params)
    }

    /// Deletes the graph stored in the database, and drop all the schema caches.
    /// NOTE: This still maintains the graph API, operations are still viable.
    pub fn delete(&mut self) -> Result<()> {
        self.send_command("GRAPH.DELETE", None, None)?;
        self.graph_schema.clear();
        Ok(())
    }

    /// Retrieves the slowlog data, which contains info about the N slowest queries.
    ///
    /// # Returns
    /// A [`Vec`] of [`SlowlogEntry`], providing information about each query.
    pub fn slowlog(&self) -> Result<Vec<SlowlogEntry>> {
        let res = self.send_command("GRAPH.SLOWLOG", None, None)?.into_vec()?;

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
    pub fn slowlog_reset(&self) -> Result<FalkorValue> {
        self.send_command("GRAPH.SLOWLOG", None, Some(&["RESET".to_string()]))
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
        params: Option<&HashMap<T, Z>>,
    ) -> Result<ExecutionPlan> {
        let query = construct_query(query_string, params);

        ExecutionPlan::try_from(self.send_command("GRAPH.PROFILE", None, Some(&[query]))?)
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
    pub fn profile<Q: ToString>(
        &self,
        query_string: Q,
    ) -> Result<ExecutionPlan> {
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
        params: Option<&HashMap<T, Z>>,
    ) -> Result<ExecutionPlan> {
        let query = construct_query(query_string, params);
        ExecutionPlan::try_from(self.send_command("GRAPH.EXPLAIN", None, Some(&[query]))?)
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
    pub fn explain<Q: ToString>(
        &self,
        query_string: Q,
    ) -> Result<ExecutionPlan> {
        self.explain_with_params::<Q, &str, &str>(query_string, None)
    }

    fn query_inner_with_timeout<Q: ToString, T: ToString, Z: ToString, P: FalkorParsable>(
        &mut self,
        command: &str,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
        timeout: i64,
    ) -> Result<P> {
        let query = construct_query(query_string, params);

        let mut conn = self.client.borrow_connection()?;
        P::from_falkor_value(
            conn.send_command(
                Some(self.graph_name.as_str()),
                command,
                None,
                Some(&[query, "--compact".to_string(), format!("timeout {timeout}")]),
            )?,
            &mut self.graph_schema,
            &mut conn,
        )
    }

    fn query_inner<Q: ToString, T: ToString, Z: ToString, P: FalkorParsable>(
        &mut self,
        command: &str,
        query_string: Q,
        params: Option<&HashMap<T, Z>>,
    ) -> Result<P> {
        let query = construct_query(query_string, params);

        let mut conn = self.client.borrow_connection()?;
        P::from_falkor_value(
            conn.send_command(
                Some(self.graph_name.as_str()),
                command,
                None,
                Some(&[query, "--compact".to_string()]),
            )?,
            &mut self.graph_schema,
            &mut conn,
        )
    }

    /// Run a query on the graph
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    ///
    /// # Returns
    /// A [`QueryResult`] object, containing the headers, statistics and the result set for the query
    pub fn query<Q: Display>(
        &mut self,
        query_string: Q,
    ) -> Result<QueryResult> {
        self.query_inner::<Q, &str, &str, QueryResult>("GRAPH.QUERY", query_string, None)
    }

    /// Run a query on the graph, but abort it if it exceeds the timeout
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    /// * `timeout`: Specify how long should the query run before aborting.
    ///
    /// # Returns
    /// A [`QueryResult`] object, containing the headers, statistics and the result set for the query
    pub fn query_with_timeout<Q: Display>(
        &mut self,
        query_string: Q,
        timeout: i64,
    ) -> Result<QueryResult> {
        self.query_inner_with_timeout::<Q, &str, &str, QueryResult>(
            "GRAPH.QUERY",
            query_string,
            None,
            timeout,
        )
    }

    /// Run a query on the graph
    /// This function variant allows adding extra parameters after the query
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    /// * `params`: a map of parameters and values, note that all keys should be of the same type, and all values should be of the same type.
    ///
    /// # Returns
    /// A [`QueryResult`] object, containing the headers, statistics and the result set for the query
    pub fn query_with_params<Q: Display, T: Display, Z: Display>(
        &mut self,
        query_string: Q,
        params: &HashMap<T, Z>,
    ) -> Result<QueryResult> {
        self.query_inner("GRAPH.QUERY", query_string, Some(params))
    }

    /// Run a query on the graph but abort it if it exceeds the timeout
    /// This function variant allows adding extra parameters after the query
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    /// * `timeout`: Specify how long should the query run before aborting.
    /// * `params`: a map of parameters and values, note that all keys should be of the same type, and all values should be of the same type.
    ///
    /// # Returns
    /// A [`QueryResult`] object, containing the headers, statistics and the result set for the query
    pub fn query_with_params_and_timeout<Q: Display, T: Display, Z: Display>(
        &mut self,
        query_string: Q,
        timeout: i64,
        params: &HashMap<T, Z>,
    ) -> Result<QueryResult> {
        self.query_inner_with_timeout("GRAPH.QUERY", query_string, Some(params), timeout)
    }

    /// Run a query on the graph
    /// Read-only queries are more limited with the operations they are allowed to perform.
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    ///
    /// # Returns
    /// A [`QueryResult`] object, containing the headers, statistics and the result set for the query
    pub fn query_readonly<Q: Display>(
        &mut self,
        query_string: Q,
    ) -> Result<QueryResult> {
        self.query_inner::<Q, &str, &str, QueryResult>("GRAPH.QUERY_RO", query_string, None)
    }

    /// Run a query on the graph, but abort it if it exceeds the timeout
    /// Read-only queries are more limited with the operations they are allowed to perform.
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    /// * `timeout`: Specify how long should the query run before aborting.
    ///
    /// # Returns
    /// A [`QueryResult`] object, containing the headers, statistics and the result set for the query
    pub fn query_readonly_with_timeout<Q: Display>(
        &mut self,
        query_string: Q,
        timeout: i64,
    ) -> Result<QueryResult> {
        self.query_inner_with_timeout::<Q, &str, &str, QueryResult>(
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
        &mut self,
        query_string: Q,
        params: &HashMap<T, Z>,
    ) -> Result<QueryResult> {
        self.query_inner("GRAPH.QUERY_RO", query_string, Some(params))
    }

    /// Run a read-only query on the graph, but abort it if it exceeds the timeout
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
    pub fn query_readonly_with_params_and_timeout<Q: ToString, T: ToString, Z: ToString>(
        &mut self,
        query_string: Q,
        params: &HashMap<T, Z>,
        timeout: i64,
    ) -> Result<QueryResult> {
        self.query_inner_with_timeout("GRAPH.QUERY_RO", query_string, Some(params), timeout)
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
    /// A caller-provided type which implements [`FalkorParsable`]
    pub fn call_procedure<C: ToString, P: FalkorParsable>(
        &mut self,
        procedure: C,
        args: Option<&[&str]>,
        yields: Option<&[&str]>,
        read_only: bool,
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
        )
    }

    /// Run a query which calls a procedure on the graph, read-only, or otherwise.
    /// Read-only queries are more limited with the operations they are allowed to perform.
    /// This function allows adding extra parameters after the query, and adding a YIELD block afterward
    /// This function will cause the query to abort if it exceeds a certain timeout
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
    pub fn call_procedure_with_timeout<C: ToString, P: FalkorParsable>(
        &mut self,
        procedure: C,
        args: Option<&[&str]>,
        yields: Option<&[&str]>,
        read_only: bool,
        timeout: i64,
    ) -> Result<P> {
        let (query_string, params) = generate_procedure_call(procedure, args, yields);

        self.query_inner_with_timeout(
            if read_only {
                "GRAPH.QUERY_RO"
            } else {
                "GRAPH.QUERY"
            },
            query_string,
            params.as_ref(),
            timeout,
        )
    }

    /// Calls the DB.INDICES procedure on the graph, returning all the indexing methods currently used
    ///
    /// # Returns
    /// A [`Vec`] of [`Index`]
    pub fn list_indices(&mut self) -> Result<Vec<FalkorIndex>> {
        let mut conn = self.client.borrow_connection()?;
        let [_, indices, _]: [FalkorValue; 3] = self
            .call_procedure::<&str, FalkorValue>("DB.INDEXES", None, None, false)?
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        let indices = indices.into_vec()?;

        let mut out_vec = Vec::with_capacity(indices.len());
        for index in indices {
            out_vec.push(FalkorIndex::from_falkor_value(
                index,
                &mut self.graph_schema,
                &mut conn,
            )?);
        }

        Ok(out_vec)
    }

    pub fn create_index<L: ToString, P: ToString>(
        &mut self,
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
                    .iter()
                    .map(|(key, val)| format!("'{key}':'{val}'"))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .map(|options_string| format!(" OPTIONS {{ {} }}", options_string))
            .unwrap_or_default();

        self.query(format!(
            "CREATE {idx_type}INDEX FOR {pattern} ON ({}){}",
            properties_string, options_string
        ))
    }

    /// Drop an existing index, by specifying its type, entity, label and specific properties
    ///
    /// # Arguments
    /// * `index_field_type`
    pub fn drop_index<L: ToString, P: ToString>(
        &mut self,
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

        self.query(format!(
            "DROP {idx_type} INDEX for {pattern} ON ({})",
            properties_string
        ))
    }

    /// Calls the DB.CONSTRAINTS procedure on the graph, returning an array of the graph's constraints
    ///
    /// # Returns
    /// A tuple where the first element is a [`Vec`] of [`Constraint`]s, and the second element is a [`Vec`] of stats as [`String`]s
    pub fn list_constraints(&mut self) -> Result<(Vec<Constraint>, Vec<String>)> {
        let mut conn = self.client.borrow_connection()?;
        let [_, query_res, stats]: [FalkorValue; 3] = self
            .call_procedure::<&str, FalkorValue>("DB.CONSTRAINTS", None, None, false)?
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        Ok((
            query_res
                .into_vec()?
                .into_iter()
                .flat_map(|item| {
                    Constraint::from_falkor_value(item, &mut self.graph_schema, &mut conn)
                })
                .collect(),
            stats
                .into_vec()?
                .into_iter()
                .flat_map(|stat| stat.into_string())
                .collect(),
        ))
    }

    /// Creates a new constraint for this graph, making the provided properties mandatory
    ///
    /// # Arguments
    /// * `entity_type`: Whether to apply this constraint on nodes or relationships.
    /// * `label`: Entities with this label will have this constraint applied to them.
    /// * `properties`: A slice of the names of properties this constraint will apply to.
    pub fn create_mandatory_constraint<L: ToString, P: ToString>(
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
    }

    /// Creates a new constraint for this graph, making the provided properties unique
    ///
    /// # Arguments
    /// * `entity_type`: Whether to apply this constraint on nodes or relationships.
    /// * `label`: Entities with this label will have this constraint applied to them.
    /// * `properties`: A slice of the names of properties this constraint will apply to.
    pub fn create_unique_constraint<P: ToString>(
        &mut self,
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
        )?;

        let mut params: Vec<String> = Vec::with_capacity(5 + properties.len());
        params.extend([
            "UNIQUE".to_string(),
            entity_type.to_string(),
            label.to_string(),
            "PROPERTIES".to_string(),
            properties.len().to_string(),
        ]);
        params.extend(properties.iter().map(|property| property.to_string()));

        // create constraint using index
        self.send_command("GRAPH.CONSTRAINT", Some("CREATE"), Some(params.as_slice()))
    }

    /// Drop an existing constraint from the graph
    ///
    /// # Arguments
    /// * `constraint_type`: Which type of constraint to remove.
    /// * `entity_type`: Whether this constraint exists on nodes or relationships.
    /// * `label`: Remove the constraint from entities with this label.
    /// * `properties`: A slice of the names of properties to remove the constraint from.
    pub fn drop_constraint<L: ToString, P: ToString>(
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test_utils::open_test_graph, IndexType};

    #[test]
    fn test_create_drop_index() {
        let mut graph = open_test_graph("test_create_drop_index");
        let res = graph.inner.create_index(
            IndexType::Fulltext,
            EntityType::Node,
            "actor".to_string(),
            &["Hello"],
            None,
        );
        assert!(res.is_ok());

        let res = graph.inner.list_indices();
        assert!(res.is_ok());

        let indices = res.unwrap();
        assert_eq!(indices.len(), 2);
        assert_eq!(indices[0].field_types["Hello"], vec![IndexType::Fulltext]);

        let res = graph.inner.drop_index(
            IndexType::Fulltext,
            EntityType::Node,
            "actor".to_string(),
            &["Hello"],
        );
        assert!(res.is_ok());
    }

    #[test]
    fn test_list_indices() {
        let mut graph = open_test_graph("test_list_indices");
        let indices = graph.inner.list_indices().expect("Could not list indices");

        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].entity_type, EntityType::Node);
        assert_eq!(indices[0].index_label, "actor".to_string());
        assert_eq!(indices[0].field_types.len(), 2);
        assert_eq!(
            indices[0].field_types,
            HashMap::from([
                ("age".to_string(), vec![IndexType::Range]),
                ("name".to_string(), vec![IndexType::Fulltext])
            ])
        );
    }

    #[test]
    fn test_create_drop_mandatory_constraint() {
        let graph = open_test_graph("test_mandatory_constraint");

        graph
            .inner
            .create_mandatory_constraint(EntityType::Edge, "act", &["hello", "goodbye"])
            .expect("Could not create constraint");

        graph
            .inner
            .drop_constraint(
                ConstraintType::Mandatory,
                EntityType::Edge,
                "act",
                &["hello", "goodbye"],
            )
            .expect("Could not drop constraint");
    }

    #[test]
    fn test_create_drop_unique_constraint() {
        let mut graph = open_test_graph("test_unique_constraint");

        graph
            .inner
            .create_unique_constraint(
                EntityType::Node,
                "actor".to_string(),
                &["first_name", "last_name"],
            )
            .expect("Could not create constraint");

        graph
            .inner
            .drop_constraint(
                ConstraintType::Unique,
                EntityType::Node,
                "actor",
                &["first_name", "last_name"],
            )
            .expect("Could not drop constraint");
    }

    #[test]
    fn test_list_constraints() {
        let mut graph = open_test_graph("test_list_constraint");

        graph
            .inner
            .create_unique_constraint(
                EntityType::Node,
                "actor".to_string(),
                &["first_name", "last_name"],
            )
            .expect("Could not create constraint");

        let (constraints, _) = graph
            .inner
            .list_constraints()
            .expect("Could not list constraints");
        assert_eq!(constraints.len(), 1);
    }

    #[test]
    fn test_slowlog() {
        let mut graph = open_test_graph("test_slowlog");

        graph
            .inner
            .query("UNWIND range(0, 500) AS x RETURN x")
            .expect("Could not generate the fast query");
        graph
            .inner
            .query("UNWIND range(0, 100000) AS x RETURN x")
            .expect("Could not generate the slow query");

        let slowlog = graph
            .inner
            .slowlog()
            .expect("Could not get slowlog entries");

        assert_eq!(slowlog.len(), 2);
        assert_eq!(
            slowlog[0].arguments,
            "UNWIND range(0, 500) AS x RETURN x".to_string()
        );
        assert_eq!(
            slowlog[1].arguments,
            "UNWIND range(0, 100000) AS x RETURN x".to_string()
        );

        graph
            .inner
            .slowlog_reset()
            .expect("Could not reset slowlog memory");
        let slowlog_after_reset = graph
            .inner
            .slowlog()
            .expect("Could not get slowlog entries after reset");
        assert!(slowlog_after_reset.is_empty());
    }

    #[test]
    fn test_explain() {
        let graph = open_test_graph("test_explain");

        let execution_plan = graph.inner.explain("MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 100").expect("Could not create execution plan");
        assert_eq!(execution_plan.steps().len(), 7);
        assert_eq!(
            execution_plan.text(),
            "\nResults\n    Limit\n        Aggregate\n            Filter\n                Node By Index Scan | (b:actor)\n                    Project\n                        Node By Label Scan | (a:actor)"
        );
    }

    #[test]
    fn test_profile() {
        let graph = open_test_graph("test_profile");

        let execution_plan = graph
            .inner
            .profile("UNWIND range(0, 1000) AS x RETURN x")
            .expect("Could not generate the query");

        let steps = execution_plan.steps().to_vec();
        assert_eq!(steps.len(), 3);

        let expected = vec!["Results", "Project", "Unwind"];
        for (step, expected) in steps.into_iter().zip(expected) {
            assert!(step.starts_with(expected));
            assert!(step.ends_with("ms"));
        }
    }
}
