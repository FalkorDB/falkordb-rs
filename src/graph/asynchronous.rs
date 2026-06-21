/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    client::asynchronous::FalkorAsyncClientInner,
    graph::{
        generate_create_index_query, generate_drop_index_query, vector_index_options,
        VectorSimilarity,
    },
    Constraint, ConstraintType, EntityType, ExecutionPlan, FalkorIndex, FalkorResult, GraphSchema,
    IndexType, ProcedureQueryBuilder, QueryBuilder, QueryResult, RowStream, SlowlogEntry,
};
use parking_lot::RwLock;
use std::{collections::HashMap, fmt::Display, sync::Arc};

/// The main graph API, this allows the user to perform graph operations while exposing as little details as possible.
/// # Cloning &amp; sharing
/// `AsyncGraph` is a cheap `Send + Clone` handle: cloning bumps a few `Arc`s and **shares one
/// schema cache** between the clones, so it is safe to `clone()` per task instead of wrapping the
/// graph in an `Arc<Mutex<_>>`. Each clone still drives queries with `&mut self`.
#[derive(Clone)]
pub struct AsyncGraph {
    client: Arc<FalkorAsyncClientInner>,
    graph_name: String,
    graph_schema: Arc<RwLock<GraphSchema>>,
}

impl AsyncGraph {
    pub(crate) fn new<T: ToString>(
        client: Arc<FalkorAsyncClientInner>,
        graph_name: T,
    ) -> Self {
        Self {
            graph_name: graph_name.to_string(),
            graph_schema: Arc::new(RwLock::new(GraphSchema::new(graph_name, client.clone()))), // Required for requesting refreshes
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

    pub(crate) fn get_client(&self) -> &Arc<FalkorAsyncClientInner> {
        &self.client
    }

    /// A cheap clone of the shared schema-cache handle. Cloned `AsyncGraph`s share one cache, and
    /// each query takes the write lock to parse its reply (and refresh the schema on a cache miss).
    pub(crate) fn schema_handle(&self) -> Arc<RwLock<GraphSchema>> {
        self.graph_schema.clone()
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Graph Execute Command", skip_all, level = "info")
    )]
    async fn execute_command(
        &self,
        command: &str,
        subcommand: Option<&str>,
        params: Option<&[&str]>,
    ) -> FalkorResult<redis::Value> {
        self.client
            .borrow_connection(self.client.clone())
            .await?
            .execute_command(Some(self.graph_name.as_str()), command, subcommand, params)
            .await
    }

    /// Deletes the graph stored in the database, and drop all the schema caches.
    /// NOTE: This still maintains the graph API, operations are still viable.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Delete Graph", skip_all, level = "info")
    )]
    pub async fn delete(&mut self) -> FalkorResult<()> {
        self.execute_command("GRAPH.DELETE", None, None).await?;
        self.graph_schema.write().clear();
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
    pub async fn slowlog(&self) -> FalkorResult<Vec<SlowlogEntry>> {
        self.execute_command("GRAPH.SLOWLOG", None, None)
            .await
            .and_then(crate::response::slowlog_entry::parse_slowlog)
    }

    /// Resets the slowlog, all query time data will be cleared.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Reset Graph Slowlog", skip_all, level = "info")
    )]
    pub async fn slowlog_reset(&self) -> FalkorResult<redis::Value> {
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
    ) -> QueryBuilder<'a, ExecutionPlan, &'a str, Self> {
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
    ) -> QueryBuilder<'a, ExecutionPlan, &'a str, Self> {
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
    ) -> QueryBuilder<QueryResult<RowStream>, T, Self> {
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
    ) -> QueryBuilder<'a, QueryResult<RowStream>, &'a str, Self> {
        QueryBuilder::new(self, "GRAPH.RO_QUERY", query_string)
    }

    /// Starts a [`BatchBuilder`](crate::BatchBuilder) for this graph: queue several queries with
    /// `query`/`ro_query` (or `push`) and `execute().await` them over a single pipelined round-trip,
    /// getting one result per query in submission order.
    ///
    /// # Returns
    /// A [`BatchBuilder`](crate::BatchBuilder) borrowing this graph until executed.
    pub fn batch(&mut self) -> crate::BatchBuilder<'_, Self> {
        crate::BatchBuilder::new(self)
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
    ) -> ProcedureQueryBuilder<'a, P, Self> {
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
    ) -> ProcedureQueryBuilder<'a, P, Self> {
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
    pub async fn list_indices(&mut self) -> FalkorResult<QueryResult<Vec<FalkorIndex>>> {
        ProcedureQueryBuilder::<QueryResult<Vec<FalkorIndex>>, Self>::new(self, "DB.INDEXES")
            .read_only()
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
    /// A [`RowStream`](crate::RowStream) containing information on the created index
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Graph Create Index", skip_all, level = "info")
    )]
    pub async fn create_index<P: Display>(
        &mut self,
        index_field_type: IndexType,
        entity_type: EntityType,
        label: &str,
        properties: &[P],
        options: Option<&HashMap<String, String>>,
    ) -> FalkorResult<QueryResult<RowStream>> {
        // Create index from these properties
        let query_str =
            generate_create_index_query(index_field_type, entity_type, label, properties, options)?;

        QueryBuilder::<QueryResult<RowStream>, String, Self>::new(self, "GRAPH.QUERY", query_str)
            .execute()
            .await
    }

    /// Creates a vector index on a node label, for similarity search over the given properties.
    ///
    /// This is a typed convenience wrapper over [`create_index`](Self::create_index) that emits a
    /// valid `OPTIONS { dimension: N, similarityFunction: '…' }` clause.
    ///
    /// # Arguments
    /// * `label`: Nodes with this label will be indexed.
    /// * `properties`: The vector properties to index.
    /// * `dimension`: The dimensionality of the indexed vectors.
    /// * `similarity_function`: The [`VectorSimilarity`] function to compare vectors with.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Graph Create Node Vector Index", skip_all, level = "info")
    )]
    pub async fn create_node_vector_index<P: Display>(
        &mut self,
        label: &str,
        properties: &[P],
        dimension: u32,
        similarity_function: VectorSimilarity,
    ) -> FalkorResult<QueryResult<RowStream>> {
        let options = vector_index_options(dimension, similarity_function);
        self.create_index(
            IndexType::Vector,
            EntityType::Node,
            label,
            properties,
            Some(&options),
        )
        .await
    }

    /// Creates a vector index on a relationship type, for similarity search over the given properties.
    ///
    /// This is a typed convenience wrapper over [`create_index`](Self::create_index) that emits a
    /// valid `OPTIONS { dimension: N, similarityFunction: '…' }` clause.
    ///
    /// # Arguments
    /// * `relation`: Relationships of this type will be indexed.
    /// * `properties`: The vector properties to index.
    /// * `dimension`: The dimensionality of the indexed vectors.
    /// * `similarity_function`: The [`VectorSimilarity`] function to compare vectors with.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Graph Create Edge Vector Index", skip_all, level = "info")
    )]
    pub async fn create_edge_vector_index<P: Display>(
        &mut self,
        relation: &str,
        properties: &[P],
        dimension: u32,
        similarity_function: VectorSimilarity,
    ) -> FalkorResult<QueryResult<RowStream>> {
        let options = vector_index_options(dimension, similarity_function);
        self.create_index(
            IndexType::Vector,
            EntityType::Edge,
            relation,
            properties,
            Some(&options),
        )
        .await
    }

    /// Drop an existing index, by specifying its type, entity, label and specific properties
    ///
    /// # Arguments
    /// * `index_field_type`
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Graph Drop Index", skip_all, level = "info")
    )]
    pub async fn drop_index<P: Display>(
        &mut self,
        index_field_type: IndexType,
        entity_type: EntityType,
        label: &str,
        properties: &[P],
    ) -> FalkorResult<QueryResult<RowStream>> {
        let query_str = generate_drop_index_query(index_field_type, entity_type, label, properties);
        self.query(query_str).execute().await
    }

    /// Calls the DB.CONSTRAINTS procedure on the graph, returning an array of the graph's constraints
    ///
    /// # Returns
    /// A tuple where the first element is a [`Vec`] of [`Constraint`]s, and the second element is a [`Vec`] of stats as [`String`]s
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "List Graph Constraints", skip_all, level = "info")
    )]
    pub async fn list_constraints(&mut self) -> FalkorResult<QueryResult<Vec<Constraint>>> {
        ProcedureQueryBuilder::<QueryResult<Vec<Constraint>>, Self>::new(self, "DB.CONSTRAINTS")
            .read_only()
            .execute()
            .await
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
    pub async fn create_mandatory_constraint(
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
            .await
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
    pub async fn create_unique_constraint(
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
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Drop Graph Constraint", skip_all, level = "info")
    )]
    pub async fn drop_constraint(
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
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_utils::{
            create_async_test_client, imdb_async_test_client, open_empty_async_test_graph,
            retry_until_async,
        },
        ConstraintType, FalkorDBError, IndexStatus, IndexType, WaitOptions,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_call_procedure_ro_routes_read_only() {
        // `call_procedure_ro` must build a read-only procedure call (GRAPH.RO_QUERY)
        // and borrow from the read-only connection path. DB.INDEXES is a read-only
        // procedure, so this exercises the read-only borrow branch end-to-end.
        let mut graph = create_async_test_client().await.select_graph("imdb");
        let result = graph
            .call_procedure_ro::<QueryResult<Vec<FalkorIndex>>>("DB.INDEXES")
            .execute()
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_drop_index() {
        let mut graph = open_empty_async_test_graph("test_create_drop_index_async").await;

        graph
            .inner
            .create_index(
                IndexType::Fulltext,
                EntityType::Node,
                "actor",
                &["Hello"],
                None,
            )
            .await
            .expect("Could not create index");

        let indices = retry_until_async(
            &mut graph.inner,
            |g| Box::pin(async move { g.list_indices().await.expect("Could not list indices") }),
            |indices| indices.data.len() == 1,
        )
        .await;

        assert_eq!(indices.data.len(), 1);
        assert_eq!(
            indices.data[0].field_types["Hello"],
            vec![IndexType::Fulltext]
        );

        assert_eq!(indices.data[0].fields.len(), 1);
        assert_eq!(indices.data[0].fields[0], "Hello");

        graph
            .inner
            .drop_index(IndexType::Fulltext, EntityType::Node, "actor", &["Hello"])
            .await
            .expect("Could not drop index");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_list_indices() {
        let mut graph = imdb_async_test_client().await.select_graph("imdb");
        let indices = graph.list_indices().await.expect("Could not list indices");

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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_drop_mandatory_constraint() {
        let graph = open_empty_async_test_graph("test_mandatory_constraint_async").await;

        graph
            .inner
            .create_mandatory_constraint(EntityType::Edge, "act", &["hello", "goodbye"])
            .await
            .expect("Could not create constraint");

        graph
            .inner
            .drop_constraint(
                ConstraintType::Mandatory,
                EntityType::Edge,
                "act",
                &["hello", "goodbye"],
            )
            .await
            .expect("Could not drop constraint");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_drop_unique_constraint() {
        let mut graph = open_empty_async_test_graph("test_unique_constraint_async").await;

        graph
            .inner
            .create_unique_constraint(
                EntityType::Node,
                "actor".to_string(),
                &["first_name", "last_name"],
            )
            .await
            .expect("Could not create constraint");

        graph
            .inner
            .drop_constraint(
                ConstraintType::Unique,
                EntityType::Node,
                "actor",
                &["first_name", "last_name"],
            )
            .await
            .expect("Could not drop constraint");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_list_constraints() {
        let mut graph = open_empty_async_test_graph("test_list_constraint_async").await;

        graph
            .inner
            .create_unique_constraint(
                EntityType::Node,
                "actor".to_string(),
                &["first_name", "last_name"],
            )
            .await
            .expect("Could not create constraint");

        let res = retry_until_async(
            &mut graph.inner,
            |g| {
                Box::pin(async move {
                    g.list_constraints()
                        .await
                        .expect("Could not list constraints")
                })
            },
            |res| res.data.len() == 1,
        )
        .await;
        assert_eq!(res.data.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_index_op_execute_is_non_blocking() {
        let mut graph = open_empty_async_test_graph("test_create_index_op_execute_async").await;

        let res = graph
            .inner
            .create_index_op(
                IndexType::Fulltext,
                EntityType::Node,
                "actor",
                &["name"],
                None,
            )
            .execute()
            .await
            .expect("Could not create index");
        assert_eq!(res.get_indices_created(), Some(1));

        // `execute` is non-blocking, so the index may not be visible yet; wait for it
        // to appear before dropping so the drop is deterministic.
        retry_until_async(
            &mut graph.inner,
            |graph| {
                Box::pin(async move {
                    graph
                        .list_indices()
                        .await
                        .expect("Could not list indices")
                        .data
                })
            },
            |indices| {
                indices.iter().any(|index| {
                    index.index_label == "actor"
                        && index
                            .field_types
                            .get("name")
                            .is_some_and(|types| types.contains(&IndexType::Fulltext))
                })
            },
        )
        .await;

        let res = graph
            .inner
            .drop_index_op(IndexType::Fulltext, EntityType::Node, "actor", &["name"])
            .execute()
            .await
            .expect("Could not drop index");
        assert_eq!(res.get_indices_deleted(), Some(1));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_vector_index_node_and_edge() {
        let mut graph = open_empty_async_test_graph("test_vector_index_helpers_async").await;

        // With the OPTIONS-encoding fix the server accepts these; previously it rejected the
        // quoted `'dimension'` key with a parse error.
        graph
            .inner
            .create_node_vector_index("Item", &["embedding"], 4, VectorSimilarity::Euclidean)
            .await
            .expect("node vector index creation should succeed");
        graph
            .inner
            .create_edge_vector_index("SIMILAR", &["v"], 8, VectorSimilarity::Cosine)
            .await
            .expect("edge vector index creation should succeed");

        // Indices build asynchronously; wait until both vector indices are listed.
        let indices = retry_until_async(
            &mut graph.inner,
            |graph| {
                Box::pin(async move {
                    graph
                        .list_indices()
                        .await
                        .expect("Could not list indices")
                        .data
                })
            },
            |indices| {
                indices.iter().any(|index| {
                    index.entity_type == EntityType::Node
                        && index
                            .field_types
                            .get("embedding")
                            .is_some_and(|types| types.contains(&IndexType::Vector))
                }) && indices.iter().any(|index| {
                    index.entity_type == EntityType::Edge
                        && index
                            .field_types
                            .get("v")
                            .is_some_and(|types| types.contains(&IndexType::Vector))
                })
            },
        )
        .await;

        assert!(
            indices.iter().any(|index| {
                index.entity_type == EntityType::Node
                    && index
                        .field_types
                        .get("embedding")
                        .is_some_and(|types| types.contains(&IndexType::Vector))
            }),
            "node vector index should be listed"
        );
        assert!(
            indices.iter().any(|index| {
                index.entity_type == EntityType::Edge
                    && index
                        .field_types
                        .get("v")
                        .is_some_and(|types| types.contains(&IndexType::Vector))
            }),
            "edge vector index should be listed"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_vector_index_op_wait() {
        let mut graph = open_empty_async_test_graph("test_vector_index_op_wait_async").await;

        // The typed vector `_op` builders integrate with the wait ergonomics: `.wait()` awaits
        // until the vector index is actually operational.
        graph
            .inner
            .create_node_vector_index_op("Item", &["embedding"], 4, VectorSimilarity::Euclidean)
            .wait()
            .await
            .expect("node vector index did not become operational");
        graph
            .inner
            .create_edge_vector_index_op("SIMILAR", &["v"], 8, VectorSimilarity::Cosine)
            .wait()
            .await
            .expect("edge vector index did not become operational");

        // After `wait()` returns, both vector indices are listed and Active.
        let indices = graph
            .inner
            .list_indices()
            .await
            .expect("Could not list indices")
            .data;
        assert!(indices.iter().any(|index| {
            index.entity_type == EntityType::Node
                && index.status == IndexStatus::Active
                && index
                    .field_types
                    .get("embedding")
                    .is_some_and(|types| types.contains(&IndexType::Vector))
        }));
        assert!(indices.iter().any(|index| {
            index.entity_type == EntityType::Edge
                && index.status == IndexStatus::Active
                && index
                    .field_types
                    .get("v")
                    .is_some_and(|types| types.contains(&IndexType::Vector))
        }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_drop_index_op_wait() {
        let mut graph = open_empty_async_test_graph("test_create_index_op_wait_async").await;

        graph
            .inner
            .create_index_op(IndexType::Range, EntityType::Node, "person", &["age"], None)
            .wait()
            .await
            .expect("Index did not become operational");

        let indices = graph
            .inner
            .list_indices()
            .await
            .expect("Could not list indices")
            .data;
        assert!(indices.iter().any(|index| {
            index.index_label == "person"
                && index.status == IndexStatus::Active
                && index
                    .field_types
                    .get("age")
                    .is_some_and(|types| types.contains(&IndexType::Range))
        }));

        graph
            .inner
            .drop_index_op(IndexType::Range, EntityType::Node, "person", &["age"])
            .wait()
            .await
            .expect("Index was not dropped");

        let indices = graph
            .inner
            .list_indices()
            .await
            .expect("Could not list indices")
            .data;
        assert!(indices.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_mandatory_constraint_op_wait() {
        let mut graph =
            open_empty_async_test_graph("test_mandatory_constraint_op_wait_async").await;

        graph
            .inner
            .create_mandatory_constraint_op(EntityType::Node, "person", &["name"])
            .wait()
            .await
            .expect("Constraint did not become operational");

        graph
            .inner
            .drop_constraint_op(
                ConstraintType::Mandatory,
                EntityType::Node,
                "person",
                &["name"],
            )
            .wait()
            .await
            .expect("Constraint was not dropped");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_constraint_op_execute_is_non_blocking() {
        let mut graph = open_empty_async_test_graph("test_constraint_op_execute_async").await;

        graph
            .inner
            .create_mandatory_constraint_op(EntityType::Node, "person", &["name"])
            .execute()
            .await
            .expect("Could not create constraint");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_drop_index_op_wait_errors_when_missing() {
        let mut graph = open_empty_async_test_graph("test_drop_index_op_missing_async").await;

        let result = graph
            .inner
            .drop_index_op(IndexType::Range, EntityType::Node, "person", &["age"])
            .wait()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_unique_constraint_op_wait() {
        let mut graph = open_empty_async_test_graph("test_unique_constraint_op_wait_async").await;

        graph
            .inner
            .create_unique_constraint_op(EntityType::Node, "person", &["email"])
            .wait()
            .await
            .expect("Constraint did not become operational");

        graph
            .inner
            .drop_constraint_op(
                ConstraintType::Unique,
                EntityType::Node,
                "person",
                &["email"],
            )
            .wait()
            .await
            .expect("Constraint was not dropped");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_unique_constraint_op_wait_reports_failure() {
        let mut graph = open_empty_async_test_graph("test_unique_constraint_op_failed_async").await;

        graph
            .inner
            .query("CREATE (:person {email: 'dup'}), (:person {email: 'dup'})")
            .execute()
            .await
            .expect("Could not seed conflicting data");

        let result = graph
            .inner
            .create_unique_constraint_op(EntityType::Node, "person", &["email"])
            .wait_with(WaitOptions::with_timeout(std::time::Duration::from_secs(
                10,
            )))
            .await;

        assert_eq!(
            result,
            Err(FalkorDBError::ConstraintFailed {
                label: "person".to_string(),
                properties: vec!["email".to_string()],
                constraint_type: ConstraintType::Unique,
            })
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // Requires running FalkorDB server with slowlog configured
    async fn test_slowlog() {
        let mut graph = open_empty_async_test_graph("test_slowlog_async").await;

        graph
            .inner
            .query("UNWIND range(0, 500) AS x RETURN x")
            .execute()
            .await
            .expect("Could not generate the fast query");
        graph
            .inner
            .query("UNWIND range(0, 100000) AS x RETURN x")
            .execute()
            .await
            .expect("Could not generate the slow query");

        let slowlog = graph
            .inner
            .slowlog()
            .await
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
            .await
            .expect("Could not reset slowlog memory");
        let slowlog_after_reset = graph
            .inner
            .slowlog()
            .await
            .expect("Could not get slowlog entries after reset");
        assert!(slowlog_after_reset.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_explain() {
        let mut graph = imdb_async_test_client().await.select_graph("imdb");

        let execution_plan = graph.explain("MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 100").execute().await.expect("Could not create execution plan");
        assert_eq!(execution_plan.plan().len(), 7);
        assert!(execution_plan.operations().get("Aggregate").is_some());
        assert_eq!(execution_plan.operations()["Aggregate"].len(), 1);

        assert_eq!(
            execution_plan.string_representation(),
            "\nResults\n    Limit\n        Aggregate\n            Filter\n                Node By Index Scan | (b:actor)\n                    Project\n                        Node By Label Scan | (a:actor)"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_profile() {
        let mut graph = open_empty_async_test_graph("test_profile_async").await;

        let execution_plan = graph
            .inner
            .profile("UNWIND range(0, 1000) AS x RETURN x")
            .execute()
            .await
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
