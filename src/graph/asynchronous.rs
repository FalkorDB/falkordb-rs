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
    IndexType, ProcedureQueryBuilder, QueryBuilder, QueryResult, RowStream, SchemaType,
    SlowlogEntry,
};
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    fmt::Display,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

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
    /// Serializes schema fetch-and-apply across clones, so an older reply can never overwrite the
    /// map from a newer one.
    refresh_lock: Arc<tokio::sync::Mutex<()>>,
    /// Bumped per schema kind after each applied refresh, so tasks that queued behind a refresh
    /// of the same kind can tell it already happened and skip their own round-trip.
    refresh_generations: Arc<[AtomicU64; 3]>,
}

fn generation_index(schema_type: SchemaType) -> usize {
    match schema_type {
        SchemaType::Labels => 0,
        SchemaType::Properties => 1,
        SchemaType::Relationships => 2,
    }
}

impl AsyncGraph {
    pub(crate) fn new<T: ToString>(
        client: Arc<FalkorAsyncClientInner>,
        graph_name: T,
    ) -> Self {
        Self {
            graph_name: graph_name.to_string(),
            graph_schema: Arc::new(RwLock::new(GraphSchema::new_in_memory(graph_name))),
            refresh_lock: Arc::new(tokio::sync::Mutex::new(())),
            refresh_generations: Arc::new(Default::default()),
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

    /// Parses `raw` against the shared schema cache, refreshing the cache asynchronously on a miss.
    ///
    /// The schema lock is only ever held for the synchronous, CPU-bound `parse`, never across an
    /// await. When `parse` reports ids the cache does not know, the lock is released, the affected
    /// schema maps are re-fetched over a pooled async connection, and `parse` runs again on a copy
    /// of `raw`. A kind is fetched at most once per call, so ids the server really does not know
    /// surface as the parse error instead of looping.
    ///
    /// A task queued behind another task's refresh of the same kind skips its own fetch, but that
    /// does not count as its refresh: the other fetch may have started before this task's reply was
    /// produced and so may not know its ids. It re-parses, and fetches itself if ids are still
    /// missing (forced after a few skips, so churn can't starve it).
    ///
    /// A failed refresh does not discard what is already parsable: `parse` runs one last time, so
    /// rows and batch items that needed no unknown id keep their results, and only the ones that
    /// did carry the missing-id error. If that leaves nothing but an error, the refresh error is
    /// returned instead, as the more useful cause.
    pub(crate) async fn parse_with_schema<I: Clone, R>(
        &self,
        raw: I,
        parse: impl Fn(I, &mut GraphSchema) -> FalkorResult<R>,
    ) -> FalkorResult<R> {
        const MAX_SKIPS: usize = 8;
        let mut refreshed: Vec<SchemaType> = Vec::new();
        let mut skips = 0;
        loop {
            let seen = self.generations();
            let attempt = raw.clone();
            let (result, missing) = {
                // Acquire only a shared read lock and clone the Arc-backed schema snapshot so
                // concurrent readers never serialize behind an exclusive write lock.
                let mut schema = self.graph_schema.read().snapshot();
                let result = parse(attempt, &mut schema);
                (result, schema.take_missing())
            };

            let to_refresh: Vec<_> = missing
                .into_iter()
                .filter(|schema_type| !refreshed.contains(schema_type))
                .collect();
            if to_refresh.is_empty() {
                return result;
            }

            for schema_type in to_refresh {
                let seen = (skips < MAX_SKIPS).then(|| seen[generation_index(schema_type)]);
                let fetched = match self.refresh_schema(schema_type, seen).await {
                    Ok(fetched) => fetched,
                    Err(refresh_err) => {
                        // Parse once more with the failure recorded on a local snapshot, so the
                        // rows / batch items that needed the schema carry it, while the others
                        // keep their results.
                        let result = {
                            let mut schema = self.graph_schema.read().snapshot();
                            schema
                                .set_refresh_failure(Some((schema_type, refresh_err.to_string())));
                            parse(raw, &mut schema)
                        };
                        return result.map_err(|_| refresh_err);
                    }
                };
                if fetched {
                    refreshed.push(schema_type);
                } else {
                    skips += 1;
                }
            }
        }
    }

    fn generations(&self) -> [u64; 3] {
        std::array::from_fn(|i| self.refresh_generations[i].load(Ordering::Acquire))
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Refresh Schema Type", skip_all, level = "info")
    )]
    async fn refresh_schema(
        &self,
        schema_type: SchemaType,
        seen_generation: Option<u64>,
    ) -> FalkorResult<bool> {
        let _serialized = self.refresh_lock.lock().await;
        let generation = &self.refresh_generations[generation_index(schema_type)];
        if seen_generation.is_some_and(|seen| generation.load(Ordering::Acquire) != seen) {
            // Another task refreshed this kind while we waited for the lock.
            return Ok(false);
        }
        let res = self
            .client
            .borrow_connection(self.client.clone())
            .await?
            .execute_command(
                Some(self.graph_name.as_str()),
                "GRAPH.RO_QUERY",
                None,
                Some(&[GraphSchema::refresh_query(schema_type).as_str()]),
            )
            .await?;
        self.graph_schema
            .write()
            .apply_refresh_reply(schema_type, res)?;
        generation.fetch_add(1, Ordering::Release);
        Ok(true)
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
        // Held through the delete and the cache clear, so a refresh that fetched the old names
        // can't be applied after the clear and resurrect them.
        let _serialized = self.refresh_lock.lock().await;
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
        response::execution_plan::skip_results_root,
        test_utils::{
            create_async_test_client, imdb_async_test_client, open_empty_async_test_graph,
            retry_until_async,
        },
        ConstraintType, FalkorDBError, IndexStatus, IndexType, WaitOptions,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_parse_with_schema_refreshes_once_then_surfaces_unknown_id() {
        // An id the server has never issued: the schema is refreshed once, and the still-missing id
        // is returned as the parse error rather than looping.
        let mut handle = open_empty_async_test_graph("parse_with_schema_unknown_id").await;
        let graph = &mut handle.inner;
        graph
            .query("CREATE (:Seed)")
            .execute()
            .await
            .expect("seed the graph so it exists");

        let attempts = std::cell::Cell::new(0);
        let result = graph
            .parse_with_schema((), |_, schema| {
                attempts.set(attempts.get() + 1);
                schema.parse_single_id(i64::MAX, SchemaType::Labels)
            })
            .await;
        assert!(matches!(
            result,
            Err(FalkorDBError::MissingSchemaId(SchemaType::Labels))
        ));
        assert_eq!(attempts.get(), 2);
        let _ = graph.delete().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_parse_with_schema_refresh_failure_is_reported_per_item() {
        // A graph that does not exist makes the read-only refresh fail. Items that need an unknown
        // id carry that failure, the ones that do not keep their results.
        let handle = open_empty_async_test_graph("parse_with_schema_refresh_failure").await;
        let graph = &handle.inner;

        let items = graph
            .parse_with_schema((), |_, schema| {
                Ok(vec![
                    schema.parse_single_id(7, SchemaType::Labels),
                    Ok("unaffected".to_string()),
                ])
            })
            .await
            .expect("per-item failures do not fail the whole parse");
        assert!(items[0]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("schema refresh failed"));
        assert_eq!(items[1].as_ref().unwrap(), "unaffected");

        // With nothing parsable left, the refresh error itself is returned.
        let result = graph
            .parse_with_schema((), |_, schema| {
                schema.parse_single_id(7, SchemaType::Labels)
            })
            .await;
        assert!(!matches!(
            result,
            Err(FalkorDBError::MissingSchemaId(_)) | Ok(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_refresh_schema_skips_when_generation_changed() {
        // On a graph that does not exist the read-only refresh fails, so whether a call reached the
        // server is observable: a stale generation must skip the round-trip and succeed, a current
        // one must go to the server and fail.
        let handle = open_empty_async_test_graph("refresh_schema_generation_skip").await;
        let graph = &handle.inner;
        let index = generation_index(SchemaType::Labels);

        let seen = graph.generations()[index];
        graph.refresh_generations[index].fetch_add(1, Ordering::Release);
        assert!(!graph
            .refresh_schema(SchemaType::Labels, Some(seen))
            .await
            .expect("another task already refreshed this kind, nothing to fetch"));

        assert!(graph
            .refresh_schema(SchemaType::Labels, Some(graph.generations()[index]))
            .await
            .is_err());
        // A forced refresh ignores the generation.
        assert!(graph
            .refresh_schema(SchemaType::Labels, None)
            .await
            .is_err());
        // Other kinds are unaffected by the bumped generation.
        assert!(graph
            .refresh_schema(
                SchemaType::Properties,
                Some(graph.generations()[generation_index(SchemaType::Properties)])
            )
            .await
            .is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_skipped_refresh_does_not_count_as_refreshed() {
        // Another task finishes a refresh between our cache miss and our fetch, but that refresh
        // predates the id we need. Skipping our fetch must not give up: we re-parse and fetch.
        let mut handle = open_empty_async_test_graph("parse_with_schema_skip_not_refreshed").await;
        let graph = &mut handle.inner;
        graph
            .query("CREATE (:SkipSeed)")
            .execute()
            .await
            .expect("seed the graph");

        let attempts = std::cell::Cell::new(0);
        let result = graph
            .parse_with_schema((), |_, schema| {
                attempts.set(attempts.get() + 1);
                if attempts.get() == 1 {
                    // Simulate the other task's refresh landing right after this miss.
                    graph.refresh_generations[generation_index(SchemaType::Labels)]
                        .fetch_add(1, Ordering::Release);
                }
                schema.parse_single_id(0, SchemaType::Labels)
            })
            .await;
        assert_eq!(result.unwrap(), "SkipSeed");
        assert_eq!(attempts.get(), 3);
        let _ = graph.delete().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_refresh_failure_reaches_every_item_when_all_fail() {
        let handle = open_empty_async_test_graph("parse_with_schema_all_items_fail").await;
        let items = handle
            .inner
            .parse_with_schema((), |_, schema| {
                Ok(vec![
                    schema.parse_single_id(1, SchemaType::Labels),
                    schema.parse_single_id(2, SchemaType::Labels),
                ])
            })
            .await
            .expect("per-item failures do not fail the whole parse");
        assert_eq!(items.len(), 2);
        for item in items {
            assert!(item
                .unwrap_err()
                .to_string()
                .contains("schema refresh failed"));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_delete_waits_for_in_flight_refresh() {
        // `delete` must not clear the cache while a refresh may still apply an older reply.
        let handle = open_empty_async_test_graph("delete_waits_for_refresh").await;
        let mut deleting = handle.inner.clone();
        let guard = handle.inner.refresh_lock.lock().await;

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let delete = tokio::spawn(async move {
            let _ = started_tx.send(());
            deleting.delete().await
        });
        started_rx.await.expect("delete task started");
        // Give the task time to reach the lock; it must stay blocked however long it runs.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            !delete.is_finished(),
            "delete ran while a refresh held the lock"
        );

        drop(guard);
        // The graph never existed, so the server rejects the delete; it must still have run.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), delete)
            .await
            .expect("delete proceeds once the refresh lock is released");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_parse_with_schema_all_resolve_missing_ids() {
        // Many tasks on clones of one graph miss the same schema kind at once; all must end up
        // with the resolved label whether they fetched it or skipped behind another task's fetch.
        let mut handle = open_empty_async_test_graph("parse_with_schema_concurrent").await;
        handle
            .inner
            .query("CREATE (:ConcurrentSeed)")
            .execute()
            .await
            .expect("seed the graph");

        let tasks: Vec<_> = (0..16)
            .map(|_| {
                let graph = handle.inner.clone();
                tokio::spawn(async move {
                    graph
                        .parse_with_schema((), |_, schema| {
                            schema.parse_single_id(0, SchemaType::Labels)
                        })
                        .await
                })
            })
            .collect();
        for task in tasks {
            assert_eq!(task.await.unwrap().unwrap(), "ConcurrentSeed");
        }
        let _ = handle.inner.delete().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_parse_with_schema_uses_read_lock_when_cached() {
        // When all schema ids are already cached, `parse_with_schema` must only acquire a read
        // lock so concurrent readers never block each other.
        let mut handle = open_empty_async_test_graph("parse_with_schema_read_lock").await;
        handle
            .inner
            .query("CREATE (:CachedSeed)")
            .execute()
            .await
            .expect("seed the graph");
        // Warm the schema cache for label 0.
        let _ = handle
            .inner
            .parse_with_schema((), |_, schema| {
                schema.parse_single_id(0, SchemaType::Labels)
            })
            .await
            .unwrap();

        let graph = handle.inner.clone();
        let schema_lock = Arc::clone(&handle.inner.graph_schema);
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let _read_guard = schema_lock.read();
            locked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        locked_rx.recv().unwrap();

        let resolved = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            graph.parse_with_schema((), |_, schema| {
                schema.parse_single_id(0, SchemaType::Labels)
            }),
        )
        .await
        .expect("parse_with_schema must not block on a held read guard")
        .unwrap();
        assert_eq!(resolved, "CachedSeed");
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        let _ = handle.inner.delete().await;
    }

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
    async fn test_call_procedure_ro_read_preference() {
        // A read-only procedure call may opt into a replica (transparently falling back to the
        // primary on single-node) or be forced onto the primary.
        let client = create_async_test_client().await;
        #[allow(deprecated)]
        let legacy = client.reads_from_replicas();
        assert_eq!(legacy, client.replica_reads_available());
        assert_eq!(client.read_preference(), crate::ReadPreference::Primary);

        let mut graph = client.select_graph("imdb");
        assert!(graph
            .call_procedure_ro::<QueryResult<Vec<FalkorIndex>>>("DB.INDEXES")
            .prefer_replica()
            .execute()
            .await
            .is_ok());
        assert!(graph
            .call_procedure_ro::<QueryResult<Vec<FalkorIndex>>>("DB.INDEXES")
            .primary_only()
            .execute()
            .await
            .is_ok());

        // A replica preference on a writable procedure call is rejected before any round-trip.
        assert!(matches!(
            graph
                .call_procedure::<QueryResult<Vec<FalkorIndex>>>("DB.INDEXES")
                .prefer_replica()
                .execute()
                .await,
            Err(FalkorDBError::ReadPreferenceNotReadOnly { context: "query" })
        ));
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
        assert!(execution_plan.operations().get("Aggregate").is_some());
        assert_eq!(execution_plan.operations()["Aggregate"].len(), 1);

        // FalkorDB up to 4.2 roots the plan in a `Results` operation that newer servers no
        // longer plan, so assert on the operations below it to pass on both generations.
        let steps = skip_results_root(execution_plan.plan());
        assert_eq!(
            steps.iter().map(|step| step.trim()).collect::<Vec<_>>(),
            [
                "Limit",
                "Aggregate",
                "Filter",
                "Node By Index Scan | (b:actor)",
                "Project",
                "Node By Label Scan | (a:actor)",
            ]
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

        // As in `test_explain`, ignore the legacy `Results` root the server may still plan.
        assert_eq!(skip_results_root(execution_plan.plan()).len(), 2);

        let mut current_rc = execution_plan.operation_tree().clone();
        if current_rc.name == "Results" {
            current_rc = current_rc.children[0].clone();
        }

        let expected = vec!["Project", "Unwind"];
        for step in expected {
            assert_eq!(current_rc.name, step);
            if step != "Unwind" {
                current_rc = current_rc.children[0].clone();
            }
        }
    }
}
