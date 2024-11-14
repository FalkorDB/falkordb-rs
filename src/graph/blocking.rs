/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    client::blocking::FalkorSyncClientInner,
    graph::{generate_create_index_query, generate_drop_index_query, HasGraphSchema},
    parser::redis_value_as_vec,
    Constraint, ConstraintType, EntityType, ExecutionPlan, FalkorIndex, FalkorResult, GraphSchema,
    IndexType, LazyResultSet, ProcedureQueryBuilder, QueryBuilder, QueryResult, SlowlogEntry,
};
use std::{collections::HashMap, fmt::Display, sync::Arc};

/// The main graph API, this allows the user to perform graph operations while exposing as little details as possible.
/// # Thread Safety
/// This struct is NOT thread safe, and synchronization is up to the user.
/// Graph schema is not shared between instances of SyncGraph, even with the same name, but cloning will maintain the current schema
#[derive(Clone)]
pub struct SyncGraph {
    client: Arc<FalkorSyncClientInner>,
    graph_name: String,
    graph_schema: GraphSchema,
}

impl SyncGraph {
    pub(crate) fn new<T: ToString>(
        client: Arc<FalkorSyncClientInner>,
        graph_name: T,
    ) -> Self {
        Self {
            graph_name: graph_name.to_string(),
            graph_schema: GraphSchema::new(graph_name, client.clone()), // Required for requesting refreshes
            client,
        }
    }

    /// Returns the name of the graph for which this API performs operations.
    ///
    /// # Returns
    /// The graph name as a string slice, without cloning.
    pub fn graph_name(&self) -> &str {
        self.graph_name.as_str()
    }

    pub(crate) fn get_client(&self) -> &Arc<FalkorSyncClientInner> {
        &self.client
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Graph Execute Command", skip_all, level = "info")
    )]
    fn execute_command(
        &self,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[&str]>,
    ) -> FalkorResult<redis::Value> {
        self.client
            .borrow_connection(self.client.clone())?
            .execute_command(Some(self.graph_name.as_str()), command, subcommand, params)
    }

    /// Deletes the graph stored in the database, and drop all the schema caches.
    /// NOTE: This still maintains the graph API, operations are still viable.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Delete Graph", skip_all, level = "info")
    )]
    pub fn delete(&mut self) -> FalkorResult<()> {
        self.execute_command("GRAPH.DELETE", None, None)?;
        self.graph_schema.clear();
        Ok(())
    }

    /// Retrieves the slowlog data, which contains info about the N slowest queries.
    ///
    /// # Returns
    /// A [`Vec`] of [`SlowlogEntry`], providing information about each query.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Get Graph Slowlog", skip_all, level = "info")
    )]
    pub fn slowlog(&self) -> FalkorResult<Vec<SlowlogEntry>> {
        self.execute_command("GRAPH.SLOWLOG", None, None)
            .and_then(|res| {
                redis_value_as_vec(res)
                    .map(|as_vec| as_vec.into_iter().flat_map(SlowlogEntry::parse).collect())
            })
    }

    /// Resets the slowlog, all query time data will be cleared.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Reset Graph Slowlog", skip_all, level = "info")
    )]
    pub fn slowlog_reset(&self) -> FalkorResult<redis::Value> {
        self.execute_command("GRAPH.SLOWLOG", None, Some(&["RESET"]))
    }

    /// Creates a [`QueryBuilder`] for this graph, in an attempt to profile a specific query
    /// This [`QueryBuilder`] has to be dropped or ran using [`QueryBuilder::execute`], before reusing the graph, as it takes a mutable reference to the graph for as long as it exists
    ///
    /// # Arguments
    /// * `query_string`: The query to profile
    ///
    /// # Returns
    /// A [`QueryBuilder`] object, which when performed will return an [`ExecutionPlan`]
    pub fn profile<'a>(
        &'a mut self,
        query_string: &'a str,
    ) -> QueryBuilder<ExecutionPlan, &str, Self> {
        QueryBuilder::<'a>::new(self, "GRAPH.PROFILE", query_string)
    }

    /// Creates a [`QueryBuilder`] for this graph, in an attempt to explain a specific query
    /// This [`QueryBuilder`] has to be dropped or ran using [`QueryBuilder::execute`], before reusing the graph, as it takes a mutable reference to the graph for as long as it exists
    ///
    /// # Arguments
    /// * `query_string`: The query to explain the process for
    ///
    /// # Returns
    /// A [`QueryBuilder`] object, which when performed will return an [`ExecutionPlan`]
    pub fn explain<'a>(
        &'a mut self,
        query_string: &'a str,
    ) -> QueryBuilder<ExecutionPlan, &str, Self> {
        QueryBuilder::new(self, "GRAPH.EXPLAIN", query_string)
    }

    /// Creates a [`QueryBuilder`] for this graph
    /// This [`QueryBuilder`] has to be dropped or ran using [`QueryBuilder::execute`], before reusing the graph, as it takes a mutable reference to the graph for as long as it exists
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    ///
    /// # Returns
    /// A [`QueryBuilder`] object, which when performed will return a [`QueryResult<FalkorResultSet>`]
    pub fn query<T: Display>(
        &mut self,
        query_string: T,
    ) -> QueryBuilder<QueryResult<LazyResultSet>, T, Self> {
        QueryBuilder::new(self, "GRAPH.QUERY", query_string)
    }

    /// Creates a [`QueryBuilder`] for this graph, for a readonly query
    /// This [`QueryBuilder`] has to be dropped or ran using [`QueryBuilder::execute`], before reusing the graph, as it takes a mutable reference to the graph for as long as it exists
    /// Read-only queries are more limited with the operations they are allowed to perform.
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    ///
    /// # Returns
    /// A [`QueryBuilder`] object
    pub fn ro_query<'a>(
        &'a mut self,
        query_string: &'a str,
    ) -> QueryBuilder<QueryResult<LazyResultSet>, &str, Self> {
        QueryBuilder::new(self, "GRAPH.QUERY_RO", query_string)
    }

    /// Creates a [`ProcedureQueryBuilder`] for this graph
    /// This [`ProcedureQueryBuilder`] has to be dropped or ran using [`ProcedureQueryBuilder::execute`], before reusing the graph, as it takes a mutable reference to the graph for as long as it exists
    /// Read-only queries are more limited with the operations they are allowed to perform.
    ///
    /// # Arguments
    /// * `procedure_name`: The name of the procedure to call
    ///
    /// # Returns
    /// A [`ProcedureQueryBuilder`] object
    pub fn call_procedure<'a, P>(
        &'a mut self,
        procedure_name: &'a str,
    ) -> ProcedureQueryBuilder<P, Self> {
        ProcedureQueryBuilder::new(self, procedure_name)
    }

    /// Creates a [`ProcedureQueryBuilder`] for this graph, for a readonly procedure
    /// This [`ProcedureQueryBuilder`] has to be dropped or ran using [`ProcedureQueryBuilder::execute`], before reusing the graph, as it takes a mutable reference to the graph for as long as it exists
    /// Read-only procedures are more limited with the operations they are allowed to perform.
    ///
    /// # Arguments
    /// * `procedure_name`: The name of the procedure to call
    ///
    /// # Returns
    /// A [`ProcedureQueryBuilder`] object
    pub fn call_procedure_ro<'a, P>(
        &'a mut self,
        procedure_name: &'a str,
    ) -> ProcedureQueryBuilder<P, Self> {
        ProcedureQueryBuilder::new_readonly(self, procedure_name)
    }

    /// Calls the DB.INDICES procedure on the graph, returning all the indexing methods currently used
    ///
    /// # Returns
    /// A [`Vec`] of [`FalkorIndex`]
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "List Graph Indices", skip_all, level = "info")
    )]
    pub fn list_indices(&mut self) -> FalkorResult<QueryResult<Vec<FalkorIndex>>> {
        ProcedureQueryBuilder::<QueryResult<Vec<FalkorIndex>>, Self>::new(self, "DB.INDEXES")
            .execute()
    }

    /// Creates a new index in the graph, for the selected entity type(Node/Edge), selected label, and properties
    ///
    /// # Arguments
    /// * `index_field_type`:
    /// * `entity_type`:
    /// * `label`:
    /// * `properties`:
    /// * `options`:
    ///
    /// # Returns
    /// A [`LazyResultSet`] containing information on the created index
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Graph Create Index", skip_all, level = "info")
    )]
    pub fn create_index<P: Display>(
        &mut self,
        index_field_type: IndexType,
        entity_type: EntityType,
        label: &str,
        properties: &[P],
        options: Option<&HashMap<String, String>>,
    ) -> FalkorResult<QueryResult<LazyResultSet>> {
        // Create index from these properties

        let query_str =
            generate_create_index_query(index_field_type, entity_type, label, properties, options);

        QueryBuilder::<QueryResult<LazyResultSet>, String, Self>::new(
            self,
            "GRAPH.QUERY",
            query_str,
        )
        .execute()
    }

    /// Drop an existing index, by specifying its type, entity, label and specific properties
    ///
    /// # Arguments
    /// * `index_field_type`
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Graph Drop Index", skip_all, level = "info")
    )]
    pub fn drop_index<P: Display>(
        &mut self,
        index_field_type: IndexType,
        entity_type: EntityType,
        label: &str,
        properties: &[P],
    ) -> FalkorResult<QueryResult<LazyResultSet>> {
        let query_str = generate_drop_index_query(index_field_type, entity_type, label, properties);
        self.query(query_str).execute()
    }

    /// Calls the DB.CONSTRAINTS procedure on the graph, returning an array of the graph's constraints
    ///
    /// # Returns
    /// A tuple where the first element is a [`Vec`] of [`Constraint`]s, and the second element is a [`Vec`] of stats as [`String`]s
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "List Graph Constraints", skip_all, level = "info")
    )]
    pub fn list_constraints(&mut self) -> FalkorResult<QueryResult<Vec<Constraint>>> {
        ProcedureQueryBuilder::<QueryResult<Vec<Constraint>>, Self>::new(self, "DB.CONSTRAINTS")
            .execute()
    }

    /// Creates a new constraint for this graph, making the provided properties mandatory
    ///
    /// # Arguments
    /// * `entity_type`: Whether to apply this constraint on nodes or relationships.
    /// * `label`: Entities with this label will have this constraint applied to them.
    /// * `properties`: A slice of the names of properties this constraint will apply to.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Create Graph Mandatory Constraint", skip_all, level = "info")
    )]
    pub fn create_mandatory_constraint(
        &self,
        entity_type: EntityType,
        label: &str,
        properties: &[&str],
    ) -> FalkorResult<redis::Value> {
        let entity_type = entity_type.to_string();
        let properties_count = properties.len().to_string();

        let mut params = Vec::with_capacity(5 + properties.len());
        params.extend([
            "MANDATORY",
            entity_type.as_str(),
            label,
            "PROPERTIES",
            properties_count.as_str(),
        ]);
        params.extend(properties);

        self.execute_command("GRAPH.CONSTRAINT", Some("CREATE"), Some(params.as_slice()))
    }

    /// Creates a new constraint for this graph, making the provided properties unique
    ///
    /// # Arguments
    /// * `entity_type`: Whether to apply this constraint on nodes or relationships.
    /// * `label`: Entities with this label will have this constraint applied to them.
    /// * `properties`: A slice of the names of properties this constraint will apply to.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Create Graph Unique Constraint", skip_all, level = "info")
    )]
    pub fn create_unique_constraint(
        &mut self,
        entity_type: EntityType,
        label: String,
        properties: &[&str],
    ) -> FalkorResult<redis::Value> {
        self.create_index(
            IndexType::Range,
            entity_type,
            label.as_str(),
            properties,
            None,
        )?;

        let entity_type = entity_type.to_string();
        let properties_count = properties.len().to_string();
        let mut params: Vec<&str> = Vec::with_capacity(5 + properties.len());
        params.extend([
            "UNIQUE",
            entity_type.as_str(),
            label.as_str(),
            "PROPERTIES",
            properties_count.as_str(),
        ]);
        params.extend(properties);

        // create constraint using index
        self.execute_command("GRAPH.CONSTRAINT", Some("CREATE"), Some(params.as_slice()))
    }

    /// Drop an existing constraint from the graph
    ///
    /// # Arguments
    /// * `constraint_type`: Which type of constraint to remove.
    /// * `entity_type`: Whether this constraint exists on nodes or relationships.
    /// * `label`: Remove the constraint from entities with this label.
    /// * `properties`: A slice of the names of properties to remove the constraint from.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Drop Graph Constraint", skip_all, level = "info")
    )]
    pub fn drop_constraint(
        &self,
        constraint_type: ConstraintType,
        entity_type: EntityType,
        label: &str,
        properties: &[&str],
    ) -> FalkorResult<redis::Value> {
        let constraint_type = constraint_type.to_string();
        let entity_type = entity_type.to_string();
        let properties_count = properties.len().to_string();

        let mut params = Vec::with_capacity(5 + properties.len());
        params.extend([
            constraint_type.as_str(),
            entity_type.as_str(),
            label,
            "PROPERTIES",
            properties_count.as_str(),
        ]);
        params.extend(properties);

        self.execute_command("GRAPH.CONSTRAINT", Some("DROP"), Some(params.as_slice()))
    }
}

impl HasGraphSchema for SyncGraph {
    fn get_graph_schema_mut(&mut self) -> &mut GraphSchema {
        &mut self.graph_schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_utils::{create_test_client, open_empty_test_graph},
        FalkorDBError, IndexType,
    };

    #[test]
    fn test_create_drop_index() {
        let mut graph = open_empty_test_graph("test_create_drop_index");

        let indices = graph
            .inner
            .create_index(
                IndexType::Fulltext,
                EntityType::Node,
                "actor",
                &["Hello"],
                None,
            )
            .expect("Could not create index");
        assert_eq!(indices.get_indices_created(), Some(1));

        let indices = graph.inner.list_indices().expect("Could not list indices");
        assert_eq!(indices.data.len(), 1);
        assert_eq!(
            indices.data[0].field_types["Hello"],
            vec![IndexType::Fulltext]
        );

        let indices = graph
            .inner
            .drop_index(IndexType::Fulltext, EntityType::Node, "actor", &["Hello"])
            .expect("Could not drop index");
        assert_eq!(indices.get_indices_deleted(), Some(1));
    }

    #[test]
    fn test_invalid_cypher_query_syntax_returns_error() {
        let mut graph = create_test_client().select_graph("imdb");
        let res = graph.query("not a cypher query").execute();
        assert!(res.is_err());
        assert!(matches!(res, Err(FalkorDBError::RedisError(_))));
    }
    #[test]
    fn test_list_indices() {
        let mut graph = create_test_client().select_graph("imdb");
        let indices = graph.list_indices().expect("Could not list indices");

        assert_eq!(indices.data.len(), 1);
        assert_eq!(indices.data[0].entity_type, EntityType::Node);
        assert_eq!(indices.data[0].index_label, "actor".to_string());
        assert_eq!(indices.data[0].field_types.len(), 2);
        assert_eq!(
            indices.data[0].field_types,
            HashMap::from([
                ("name".to_string(), vec![IndexType::Fulltext]),
                ("age".to_string(), vec![IndexType::Range])
            ])
        );
    }

    #[test]
    fn test_create_drop_mandatory_constraint() {
        let graph = open_empty_test_graph("test_mandatory_constraint");

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
        let mut graph = open_empty_test_graph("test_unique_constraint");

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
        let mut graph = open_empty_test_graph("test_list_constraints");

        graph
            .inner
            .create_unique_constraint(
                EntityType::Node,
                "actor".to_string(),
                &["first_name", "last_name"],
            )
            .expect("Could not create constraint");

        let res = graph
            .inner
            .list_constraints()
            .expect("Could not list constraints");
        assert_eq!(res.data.len(), 1);
    }

    #[test]
    fn test_slowlog() {
        let mut graph = open_empty_test_graph("test_slowlog");

        graph
            .inner
            .query("UNWIND range(0, 500) AS x RETURN x")
            .execute()
            .expect("Could not generate the fast query");
        graph
            .inner
            .query("UNWIND range(0, 100000) AS x RETURN x")
            .execute()
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
        let mut graph = create_test_client().select_graph("imdb");

        let execution_plan = graph.explain("MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 100").execute().expect("Could not create execution plan");
        assert_eq!(execution_plan.plan().len(), 7);
        assert!(execution_plan.operations().get("Aggregate").is_some());
        assert_eq!(execution_plan.operations()["Aggregate"].len(), 1);

        assert_eq!(
            execution_plan.string_representation(),
            "\nResults\n    Limit\n        Aggregate\n            Filter\n                Node By Index Scan | (b:actor)\n                    Project\n                        Node By Label Scan | (a:actor)"
        );
    }

    #[test]
    fn test_profile() {
        let mut graph = open_empty_test_graph("test_profile");

        let execution_plan = graph
            .inner
            .profile("UNWIND range(0, 1000) AS x RETURN x")
            .execute()
            .expect("Could not generate the query");

        assert_eq!(execution_plan.plan().len(), 3);

        let expected = vec!["Results", "Project", "Unwind"];
        let mut current_rc = execution_plan.operation_tree().clone();
        for step in expected {
            assert_eq!(current_rc.name, step);
            if step != "Unwind" {
                current_rc = current_rc.children[0].clone();
            }
        }
    }
}
