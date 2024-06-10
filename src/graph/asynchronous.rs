/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    client::asynchronous::FalkorAsyncClientInner, graph::HasGraphSchema, Constraint,
    ConstraintType, EntityType, ExecutionPlan, FalkorIndex, FalkorResponse, FalkorResult,
    FalkorValue, GraphSchema, IndexType, LazyResultSet, ProcedureQueryBuilder, QueryBuilder,
    SlowlogEntry,
};
use std::{collections::HashMap, fmt::Display, sync::Arc};

/// The main graph API, this allows the user to perform graph operations while exposing as little details as possible.
/// # Thread Safety
/// This struct is NOT thread safe, and synchronization is up to the user.
/// It does, however, allow the user to perform nonblocking operations
/// Graph schema is not shared between instances of AsyncGraph, even with the same name, but cloning will maintain the current schema
#[derive(Clone)]
pub struct AsyncGraph {
    pub(crate) client: Arc<FalkorAsyncClientInner>,
    graph_name: String,
    pub(crate) graph_schema: GraphSchema<FalkorAsyncClientInner>,
}

impl AsyncGraph {
    pub(crate) fn new<T: ToString>(
        client: Arc<FalkorAsyncClientInner>,
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

    async fn execute_command(
        &self,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[&str]>,
    ) -> FalkorResult<FalkorValue> {
        self.client
            .borrow_connection(self.client.clone())
            .await?
            .execute_command(Some(self.graph_name.as_str()), command, subcommand, params)
            .await
    }

    /// Deletes the graph stored in the database, and drop all the schema caches.
    /// NOTE: This still maintains the graph API, operations are still viable.
    pub async fn delete(&mut self) -> FalkorResult<()> {
        self.execute_command("GRAPH.DELETE", None, None).await?;
        self.graph_schema.clear();
        Ok(())
    }

    /// Retrieves the slowlog data, which contains info about the N slowest queries.
    ///
    /// # Returns
    /// A [`Vec`] of [`SlowlogEntry`], providing information about each query.
    pub async fn slowlog(&self) -> FalkorResult<Vec<SlowlogEntry>> {
        let res = self
            .execute_command("GRAPH.SLOWLOG", None, None)
            .await?
            .into_vec()?;

        Ok(res.into_iter().flat_map(SlowlogEntry::try_from).collect())
    }

    /// Resets the slowlog, all query time data will be cleared.
    pub async fn slowlog_reset(&self) -> FalkorResult<FalkorValue> {
        self.execute_command("GRAPH.SLOWLOG", None, Some(&["RESET"]))
            .await
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
    ) -> QueryBuilder<ExecutionPlan, &str, FalkorAsyncClientInner, Self> {
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
    ) -> QueryBuilder<ExecutionPlan, &str, FalkorAsyncClientInner, Self> {
        QueryBuilder::new(self, "GRAPH.EXPLAIN", query_string)
    }

    /// Creates a [`QueryBuilder`] for this graph
    /// This [`QueryBuilder`] has to be dropped or ran using [`QueryBuilder::execute`], before reusing the graph, as it takes a mutable reference to the graph for as long as it exists
    ///
    /// # Arguments
    /// * `query_string`: The query to run
    ///
    /// # Returns
    /// A [`QueryBuilder`] object, which when performed will return a [`FalkorResponse<FalkorResultSet>`]
    pub fn query<T: Display>(
        &mut self,
        query_string: T,
    ) -> QueryBuilder<
        FalkorResponse<LazyResultSet<FalkorAsyncClientInner>>,
        T,
        FalkorAsyncClientInner,
        Self,
    > {
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
    pub async fn ro_query<'a>(
        &'a mut self,
        query_string: &'a str,
    ) -> QueryBuilder<
        FalkorResponse<LazyResultSet<FalkorAsyncClientInner>>,
        &str,
        FalkorAsyncClientInner,
        Self,
    > {
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
    pub async fn list_indices(&mut self) -> FalkorResult<FalkorResponse<Vec<FalkorIndex>>> {
        ProcedureQueryBuilder::<FalkorResponse<Vec<FalkorIndex>>, Self>::new(self, "DB.INDEXES")
            .execute()
            .await
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
    pub async fn create_index<P: Display>(
        &mut self,
        index_field_type: IndexType,
        entity_type: EntityType,
        label: &str,
        properties: &[P],
        options: Option<&HashMap<String, String>>,
    ) -> FalkorResult<FalkorResponse<LazyResultSet<FalkorAsyncClientInner>>> {
        // Create index from these properties
        let properties_string = properties
            .iter()
            .map(|element| format!("l.{}", element))
            .collect::<Vec<_>>()
            .join(", ");

        let pattern = match entity_type {
            EntityType::Node => format!("(l:{})", label),
            EntityType::Edge => format!("()-[l:{}]->()", label),
        };

        let idx_type = match index_field_type {
            IndexType::Range => "",
            IndexType::Vector => "VECTOR ",
            IndexType::Fulltext => "FULLTEXT ",
        };

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

        let query_str = format!(
            "CREATE {idx_type}INDEX FOR {pattern} ON ({}){}",
            properties_string, options_string
        );
        QueryBuilder::<
            FalkorResponse<LazyResultSet<FalkorAsyncClientInner>>,
            String,
            FalkorAsyncClientInner,
            Self,
        >::new(self, "GRAPH.QUERY", query_str)
        .execute()
        .await
    }

    /// Drop an existing index, by specifying its type, entity, label and specific properties
    ///
    /// # Arguments
    /// * `index_field_type`
    pub async fn drop_index<L: ToString, P: ToString>(
        &mut self,
        index_field_type: IndexType,
        entity_type: EntityType,
        label: L,
        properties: &[P],
    ) -> FalkorResult<FalkorResponse<LazyResultSet<FalkorAsyncClientInner>>> {
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

        let query_str = format!(
            "DROP {idx_type} INDEX for {pattern} ON ({})",
            properties_string
        );
        self.query(query_str).execute().await
    }

    /// Calls the DB.CONSTRAINTS procedure on the graph, returning an array of the graph's constraints
    ///
    /// # Returns
    /// A tuple where the first element is a [`Vec`] of [`Constraint`]s, and the second element is a [`Vec`] of stats as [`String`]s
    pub async fn list_constraints(&mut self) -> FalkorResult<FalkorResponse<Vec<Constraint>>> {
        ProcedureQueryBuilder::<FalkorResponse<Vec<Constraint>>, AsyncGraph>::new(
            self,
            "DB.CONSTRAINTS",
        )
        .execute()
        .await
    }

    /// Creates a new constraint for this graph, making the provided properties mandatory
    ///
    /// # Arguments
    /// * `entity_type`: Whether to apply this constraint on nodes or relationships.
    /// * `label`: Entities with this label will have this constraint applied to them.
    /// * `properties`: A slice of the names of properties this constraint will apply to.
    pub async fn create_mandatory_constraint(
        &self,
        entity_type: EntityType,
        label: &str,
        properties: &[&str],
    ) -> FalkorResult<FalkorValue> {
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
            .await
    }

    /// Creates a new constraint for this graph, making the provided properties unique
    ///
    /// # Arguments
    /// * `entity_type`: Whether to apply this constraint on nodes or relationships.
    /// * `label`: Entities with this label will have this constraint applied to them.
    /// * `properties`: A slice of the names of properties this constraint will apply to.
    pub async fn create_unique_constraint(
        &mut self,
        entity_type: EntityType,
        label: String,
        properties: &[&str],
    ) -> FalkorResult<FalkorValue> {
        self.create_index(
            IndexType::Range,
            entity_type,
            label.as_str(),
            properties,
            None,
        )
        .await?;

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
            .await
    }

    /// Drop an existing constraint from the graph
    ///
    /// # Arguments
    /// * `constraint_type`: Which type of constraint to remove.
    /// * `entity_type`: Whether this constraint exists on nodes or relationships.
    /// * `label`: Remove the constraint from entities with this label.
    /// * `properties`: A slice of the names of properties to remove the constraint from.
    pub async fn drop_constraint(
        &self,
        constraint_type: ConstraintType,
        entity_type: EntityType,
        label: &str,
        properties: &[&str],
    ) -> FalkorResult<FalkorValue> {
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
            .await
    }
}

impl HasGraphSchema<FalkorAsyncClientInner> for AsyncGraph {
    fn get_graph_schema(&self) -> &GraphSchema<FalkorAsyncClientInner> {
        &self.graph_schema
    }

    fn get_graph_schema_mut(&mut self) -> &mut GraphSchema<FalkorAsyncClientInner> {
        &mut self.graph_schema
    }
}
