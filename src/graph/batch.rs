/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Batch / pipelined execution: queue many queries and dispatch them over a single Redis pipeline
//! in one round-trip, with per-query results in submission order.

use crate::graph::query_builder::{
    build_vec_rows, construct_query, dispatch_query_response, resolve_use_replica,
    unwrap_query_response,
};
use crate::graph::HasGraphSchema;
use crate::graph_schema::{reply_schema, take_schema_element};
use crate::{
    FalkorDBError, FalkorParams, FalkorResult, GraphSchema, IntoFalkorParam, IntoFalkorParams,
    QueryResult, ReadPreference, Row, SyncGraph,
};

/// The result of one query in a batch: `Ok` with its [`QueryResult`] (rows eagerly parsed into a
/// `Vec<Row>`), or `Err` with that query's server / encoding / parse error. A failure here does
/// **not** affect the other queries in the batch.
pub type BatchItemResult = FalkorResult<QueryResult<Vec<Row>>>;

/// The result of [`BatchBuilder::execute`]: one [`BatchItemResult`] per query, in submission order.
///
/// The **outer** `FalkorResult` only fails if the batch could not be completed. If it fails *after*
/// the pipeline was sent (a transport error while reading replies), the server may have executed
/// some or all queries — the result state is unknown. A batch with nothing to send (empty, or every
/// query failed to encode) returns `Ok` without a round-trip.
pub type BatchResult = FalkorResult<Vec<BatchItemResult>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BatchCommand {
    Query,
    RoQuery,
}

impl BatchCommand {
    fn as_str(self) -> &'static str {
        match self {
            BatchCommand::Query => "GRAPH.QUERY",
            BatchCommand::RoQuery => "GRAPH.RO_QUERY",
        }
    }

    fn is_readonly(self) -> bool {
        matches!(self, BatchCommand::RoQuery)
    }
}

/// A single, graph-less query to run as part of a [`BatchBuilder`]. Owns its query text, parameters
/// and timeout, so queries can be built up front (e.g. in a loop) and `push`ed into a batch.
///
/// Build one with [`BatchQuery::write`] (a `GRAPH.QUERY`) or [`BatchQuery::read`] (a read-only
/// `GRAPH.RO_QUERY`), then attach parameters/timeout. Parameter encoding errors are reported per
/// query when the batch executes (the failing query becomes an `Err`, the rest still run).
pub struct BatchQuery {
    command: BatchCommand,
    query_string: String,
    params: FalkorParams,
    timeout: Option<i64>,
}

impl BatchQuery {
    fn new(
        command: BatchCommand,
        query_string: impl Into<String>,
    ) -> Self {
        Self {
            command,
            query_string: query_string.into(),
            params: FalkorParams::new(),
            timeout: None,
        }
    }

    /// A read-write query, dispatched as `GRAPH.QUERY`.
    pub fn write(query_string: impl Into<String>) -> Self {
        Self::new(BatchCommand::Query, query_string)
    }

    /// A read-only query, dispatched as `GRAPH.RO_QUERY`.
    pub fn read(query_string: impl Into<String>) -> Self {
        Self::new(BatchCommand::RoQuery, query_string)
    }

    /// Add a single typed parameter, referenced as `$key` in the query (escaped for you). Any
    /// encoding error surfaces when the batch executes, as this query's `Err`.
    pub fn with_param<V: IntoFalkorParam>(
        &mut self,
        key: &str,
        value: V,
    ) -> &mut Self {
        self.params.add_param(key, value);
        self
    }

    /// Add several typed parameters at once. See [`IntoFalkorParams`].
    pub fn with_params<P: IntoFalkorParams>(
        &mut self,
        params: P,
    ) -> &mut Self {
        self.params.merge(params.into_falkor_params());
        self
    }

    /// Escape hatch: insert a raw, already-valid Cypher expression as the parameter value (the name
    /// is still validated). Prefer [`with_param`](Self::with_param).
    pub fn with_raw_param(
        &mut self,
        key: &str,
        raw_cypher: impl Into<String>,
    ) -> &mut Self {
        self.params.add_raw(key, raw_cypher.into());
        self
    }

    /// Set a server-side timeout (milliseconds) for this query. This is per query, not a
    /// client-side deadline for the whole batch.
    pub fn with_timeout(
        &mut self,
        timeout: i64,
    ) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Accumulates queries to run as a single pipelined batch. Create one with `graph.batch()`.
///
/// `query`/`ro_query` push a query and return a `&mut BatchQuery` for chaining parameters; the
/// borrow ends at the statement, so a `for` loop works. To keep several queries around before
/// adding them, build owned [`BatchQuery`] values and [`push`](Self::push) them.
///
/// ```no_run
/// # fn main() -> Result<(), falkordb::FalkorDBError> {
/// use falkordb::FalkorClientBuilder;
/// # let info: falkordb::FalkorConnectionInfo = "falkor://127.0.0.1:6379".try_into()?;
/// let client = FalkorClientBuilder::new().with_connection_info(info).build()?;
/// let mut graph = client.select_graph("social");
///
/// let mut batch = graph.batch();
/// for name in ["alice", "bob"] {
///     batch.query("CREATE (:Person {name: $n})").with_param("n", name);
/// }
/// batch.ro_query("MATCH (p:Person) RETURN count(p) AS n");
///
/// let results = batch.execute()?; // one round-trip; one result per query, in order
/// assert_eq!(results.len(), 3);
/// # Ok(())
/// # }
/// ```
pub struct BatchBuilder<'a, G> {
    graph: &'a mut G,
    queries: Vec<BatchQuery>,
    read_preference: Option<ReadPreference>,
}

impl<'a, G> BatchBuilder<'a, G> {
    pub(crate) fn new(graph: &'a mut G) -> Self {
        Self {
            graph,
            queries: Vec::new(),
            read_preference: None,
        }
    }

    /// Queue a read-write query (`GRAPH.QUERY`) and return a handle to set its params/timeout.
    pub fn query(
        &mut self,
        query_string: impl Into<String>,
    ) -> &mut BatchQuery {
        self.queries.push(BatchQuery::write(query_string));
        self.queries.last_mut().expect("a query was just pushed")
    }

    /// Queue a read-only query (`GRAPH.RO_QUERY`) and return a handle to set its params/timeout.
    pub fn ro_query(
        &mut self,
        query_string: impl Into<String>,
    ) -> &mut BatchQuery {
        self.queries.push(BatchQuery::read(query_string));
        self.queries.last_mut().expect("a query was just pushed")
    }

    /// Append an already-built [`BatchQuery`].
    pub fn push(
        &mut self,
        query: BatchQuery,
    ) -> &mut Self {
        self.queries.push(query);
        self
    }

    /// The number of queries queued so far.
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    /// Whether no queries have been queued.
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }

    /// Override the [`ReadPreference`] for this batch, ignoring the client-wide default.
    ///
    /// Routing applies to the **whole** batch (one pipelined round-trip on one connection). A batch
    /// is eligible for a replica only when **every** queued query is read-only; setting
    /// [`ReadPreference::PreferReplica`] on a batch that contains a write makes `execute()` fail with
    /// [`FalkorDBError::ReadPreferenceNotReadOnly`](crate::FalkorDBError::ReadPreferenceNotReadOnly).
    ///
    /// # Arguments
    /// * `read_preference`: the [`ReadPreference`] to apply to this batch.
    pub fn with_read_preference(
        mut self,
        read_preference: ReadPreference,
    ) -> Self {
        self.read_preference = Some(read_preference);
        self
    }

    /// Route this all-read batch to a replica when available, else the primary. Shortcut for
    /// [`with_read_preference(ReadPreference::PreferReplica)`](Self::with_read_preference). Replicas
    /// can be slightly stale; fails at `execute()` if the batch contains a write.
    pub fn prefer_replica(self) -> Self {
        self.with_read_preference(ReadPreference::PreferReplica)
    }

    /// Serve this batch from the primary, overriding a client default of
    /// [`ReadPreference::PreferReplica`]. Shortcut for
    /// [`with_read_preference(ReadPreference::Primary)`](Self::with_read_preference).
    pub fn primary_only(self) -> Self {
        self.with_read_preference(ReadPreference::Primary)
    }
}

/// Builds the pipeline, pre-filling a result slot for every query that fails to encode (those are
/// never sent), and recording the original index of each *submitted* command so replies can be
/// woven back into submission order. Every query carries the same `SCHEMA_HASH`, when given.
fn prepare(
    graph_name: &str,
    queries: &[BatchQuery],
    schema_hash: Option<i64>,
) -> (redis::Pipeline, Vec<usize>, Vec<Option<BatchItemResult>>) {
    let mut pipe = redis::pipe();
    let mut submitted = Vec::with_capacity(queries.len());
    let mut slots: Vec<Option<BatchItemResult>> = (0..queries.len()).map(|_| None).collect();

    for (index, query) in queries.iter().enumerate() {
        match construct_query(&query.query_string, &query.params) {
            Ok(query_string) => {
                pipe.cmd(query.command.as_str())
                    .arg(graph_name)
                    .arg(query_string)
                    .arg("--compact");
                if let Some(timeout) = query.timeout {
                    pipe.arg("timeout").arg(timeout);
                }
                if let Some(hash) = schema_hash {
                    pipe.arg("SCHEMA_HASH").arg(hash);
                }
                submitted.push(index);
            }
            Err(err) => slots[index] = Some(Err(err)),
        }
    }

    (pipe, submitted, slots)
}

/// Returns whether any queued query is a write (so the batch needs a read-write connection).
fn has_write(queries: &[BatchQuery]) -> bool {
    queries.iter().any(|query| !query.command.is_readonly())
}

/// Errors if the server returned a different number of replies than commands submitted (a protocol
/// surprise), so the contract can never panic.
fn check_reply_count(
    submitted: &[usize],
    replies: &[redis::Value],
) -> FalkorResult<()> {
    if replies.len() != submitted.len() {
        return Err(FalkorDBError::RedisParsingError(format!(
            "batch expected {} replies but the server returned {}",
            submitted.len(),
            replies.len()
        )));
    }
    Ok(())
}

/// Parses each reply, in submission order. A reply with a schema element decodes with the names it
/// carries, or with `sent` (the schema whose hash the batch sent) when that hash is still current; a
/// reply without one decodes under `graph_schema`, refreshing it on a miss. Also returns the names
/// the replies carried, for the caller to offer to the cache.
fn parse_replies(
    replies: Vec<redis::Value>,
    sent: &GraphSchema,
    graph_schema: &mut GraphSchema,
) -> (Vec<BatchItemResult>, Vec<GraphSchema>) {
    let mut new_names = Vec::new();
    let items = replies
        .into_iter()
        .map(|reply| {
            let mut res = unwrap_query_response(reply)?;
            match take_schema_element(&mut res)? {
                Some(element) => {
                    let mut reply = reply_schema(sent, element)?;
                    let result = dispatch_query_response(res, &mut reply.schema, build_vec_rows);
                    if reply.new_names {
                        new_names.push(reply.schema);
                    }
                    result
                }
                None => dispatch_query_response(res, &mut *graph_schema, build_vec_rows),
            }
        })
        .collect();
    (items, new_names)
}

/// Puts the parsed replies into their original slots and returns the fully-populated, in-order
/// results.
fn fill_slots(
    mut slots: Vec<Option<BatchItemResult>>,
    submitted: &[usize],
    parsed: Vec<BatchItemResult>,
) -> BatchResult {
    for (item, &index) in parsed.into_iter().zip(submitted) {
        slots[index] = Some(item);
    }
    Ok(slots
        .into_iter()
        .map(|slot| slot.expect("every slot is filled: encode errors up front, replies by index"))
        .collect())
}

/// Parses each reply (in submission order) into its original slot under the graph schema, then
/// returns the fully-populated, in-order results.
fn weave_replies(
    slots: Vec<Option<BatchItemResult>>,
    submitted: &[usize],
    replies: Vec<redis::Value>,
    graph_schema: &mut GraphSchema,
) -> BatchResult {
    check_reply_count(submitted, &replies)?;
    let sent = graph_schema.snapshot();
    let (parsed, new_names) = parse_replies(replies, &sent, graph_schema);
    for names in &new_names {
        graph_schema.offer(names);
    }
    fill_slots(slots, submitted, parsed)
}

impl BatchBuilder<'_, SyncGraph> {
    /// Dispatch all queued queries over one pipelined round-trip and return their results in
    /// submission order. An empty batch (or one whose every query failed to encode) returns without
    /// a round-trip. On a Redis Cluster the queries are sent one after another instead, a round
    /// trip each, since the redis-rs sync cluster pipeline fails the whole batch when any one query
    /// fails.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Execute Batch", skip_all, level = "info")
    )]
    pub fn execute(self) -> BatchResult {
        let client = self.graph.get_client().clone();
        let use_replica = resolve_use_replica(
            !has_write(&self.queries),
            self.read_preference,
            client.read_preference(),
            "batch",
        )?;

        let schema_hash = client
            .schema_hash()
            .then(|| self.graph.get_graph_schema_mut().request_hash());
        let (pipe, submitted, slots) = prepare(self.graph.graph_name(), &self.queries, schema_hash);
        if submitted.is_empty() {
            return Ok(slots
                .into_iter()
                .map(|slot| slot.expect("all filled"))
                .collect());
        }

        let conn = if use_replica {
            client.borrow_readonly_connection(client.clone())
        } else {
            client.borrow_connection(client.clone())
        };
        let replies = conn?.execute_pipeline(&pipe)?;

        let graph_schema = self.graph.get_graph_schema_mut();
        weave_replies(slots, &submitted, replies, graph_schema)
    }
}

#[cfg(feature = "tokio")]
impl BatchBuilder<'_, crate::AsyncGraph> {
    /// Dispatch all queued queries over one pipelined round-trip and return their results in
    /// submission order. An empty batch (or one whose every query failed to encode) returns without
    /// a round-trip.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Execute Batch", skip_all, level = "info")
    )]
    pub async fn execute(self) -> BatchResult {
        let client = self.graph.get_client().clone();
        let use_replica = resolve_use_replica(
            !has_write(&self.queries),
            self.read_preference,
            client.read_preference(),
            "batch",
        )?;

        let sent = self.graph.schema_snapshot();
        let schema_hash = client.schema_hash().then(|| sent.request_hash());
        let (pipe, submitted, slots) = prepare(self.graph.graph_name(), &self.queries, schema_hash);
        if submitted.is_empty() {
            return Ok(slots
                .into_iter()
                .map(|slot| slot.expect("all filled"))
                .collect());
        }

        let conn = if use_replica {
            client.borrow_readonly_connection(client.clone()).await
        } else {
            client.borrow_connection(client.clone()).await
        };
        let replies = conn?.execute_pipeline(&pipe).await?;

        check_reply_count(&submitted, &replies)?;
        let (parsed, new_names) = self
            .graph
            .parse_with_schema(replies, |replies, schema| {
                Ok(parse_replies(replies, &sent, schema))
            })
            .await?;
        self.graph.offer_reply_schemas(&new_names);
        fill_slots(slots, &submitted, parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::blocking::create_empty_inner_sync_client;

    /// A minimal valid `GRAPH.QUERY` reply: a one-element response (stats only), which parses into
    /// an empty result without needing any schema lookup.
    fn stats_only_reply() -> redis::Value {
        redis::Value::Array(vec![redis::Value::Array(vec![])])
    }

    #[test]
    fn batch_command_strings_and_readonly() {
        assert_eq!(BatchCommand::Query.as_str(), "GRAPH.QUERY");
        assert_eq!(BatchCommand::RoQuery.as_str(), "GRAPH.RO_QUERY");
        assert!(!BatchCommand::Query.is_readonly());
        assert!(BatchCommand::RoQuery.is_readonly());
    }

    #[test]
    fn builder_mechanics_are_graph_agnostic() {
        let mut graph = (); // the queue-building methods work for any graph type
        let mut batch = BatchBuilder::new(&mut graph);
        assert!(batch.is_empty());
        batch.query("RETURN 1").with_param("k", 1).with_timeout(500);
        batch.ro_query("RETURN 2");
        batch.push(BatchQuery::write("RETURN 3"));
        assert_eq!(batch.len(), 3);
        assert!(!batch.is_empty());
    }

    #[test]
    fn batch_query_with_params_and_raw_param_encode() {
        // Exercise with_params (bulk typed params) and with_raw_param (raw Cypher value);
        // a successful `prepare` confirms all three params were merged and encoded.
        let mut q = BatchQuery::write("CREATE (n {a: $a, b: $b, c: $c})");
        q.with_params([("a", 1), ("b", 2)])
            .with_raw_param("c", "timestamp()");
        let queries = vec![q];
        let (pipe, submitted, slots) = prepare("g", &queries, None);
        assert_eq!(submitted, vec![0], "query with bulk + raw params encodes");
        assert_eq!(pipe.len(), 1);
        assert!(slots[0].is_none());
    }

    #[test]
    fn has_write_detects_any_write_query() {
        assert!(has_write(&[BatchQuery::write("x")]));
        assert!(has_write(&[BatchQuery::read("x"), BatchQuery::write("y")]));
        assert!(!has_write(&[BatchQuery::read("x"), BatchQuery::read("y")]));
        assert!(!has_write(&[]));
    }

    #[test]
    fn prepare_skips_encode_errors_and_records_submitted_indices() {
        let mut bad = BatchQuery::write("RETURN 2");
        bad.with_param("invalid name!", 1); // invalid Cypher identifier -> encode error
        let queries = vec![
            BatchQuery::write("RETURN 1"),
            bad,
            BatchQuery::read("RETURN 3"),
        ];

        let (pipe, submitted, slots) = prepare("g", &queries, None);

        assert_eq!(submitted, vec![0, 2], "the middle query must be skipped");
        assert_eq!(
            pipe.len(),
            2,
            "only the two encodable queries are pipelined"
        );
        assert!(slots[0].is_none());
        assert!(
            matches!(slots[1], Some(Err(_))),
            "skipped slot is pre-filled with its error"
        );
        assert!(slots[2].is_none());
    }

    /// The arguments of each command in `pipe`, as text.
    fn pipeline_args(pipe: &redis::Pipeline) -> Vec<Vec<String>> {
        pipe.cmd_iter()
            .map(|cmd| {
                cmd.args_iter()
                    .map(|arg| match arg {
                        redis::Arg::Simple(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                        _ => "<cursor>".to_string(),
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn prepare_sends_the_schema_hash_last_on_every_query() {
        let mut timed = BatchQuery::read("RETURN 2");
        timed.with_timeout(1000);
        let queries = vec![BatchQuery::write("RETURN 1"), timed];

        let (pipe, _, _) = prepare("g", &queries, Some(-5));
        assert_eq!(
            pipeline_args(&pipe),
            [
                vec![
                    "GRAPH.QUERY",
                    "g",
                    "RETURN 1",
                    "--compact",
                    "SCHEMA_HASH",
                    "-5"
                ],
                vec![
                    "GRAPH.RO_QUERY",
                    "g",
                    "RETURN 2",
                    "--compact",
                    "timeout",
                    "1000",
                    "SCHEMA_HASH",
                    "-5"
                ],
            ]
        );

        let (pipe, _, _) = prepare("g", &queries, None);
        assert!(pipeline_args(&pipe)
            .iter()
            .all(|args| !args.iter().any(|arg| arg == "SCHEMA_HASH")));
    }

    #[test]
    fn weave_replies_decodes_each_reply_with_its_own_names() {
        // One hash went out for the whole batch. The first reply brings new names and decodes with
        // them; the second reply's hash matches what was sent, so it decodes with the names sent.
        // Afterwards the cache holds the newest names.
        let bulk = |text: &str| redis::Value::BulkString(text.as_bytes().to_vec());
        let header = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::Int(1),
            bulk("n"),
        ])]);
        let node_reply = |label: i64, element: redis::Value| {
            redis::Value::Array(vec![
                header.clone(),
                redis::Value::Array(vec![redis::Value::Array(vec![redis::Value::Array(vec![
                    redis::Value::Int(8),
                    redis::Value::Array(vec![
                        redis::Value::Int(1),
                        redis::Value::Array(vec![redis::Value::Int(label)]),
                        redis::Value::Array(vec![]),
                    ]),
                ])])]),
                redis::Value::Array(vec![]),
                element,
            ])
        };
        let element = |hash: i64, labels: Option<&[&str]>| {
            let mut pairs = vec![bulk("hash"), redis::Value::Int(hash)];
            if let Some(labels) = labels {
                pairs.extend([
                    bulk("labels"),
                    redis::Value::Array(labels.iter().map(|label| bulk(label)).collect()),
                    bulk("relationshipTypes"),
                    redis::Value::Array(vec![]),
                    bulk("propertyKeys"),
                    redis::Value::Array(vec![]),
                ]);
            }
            redis::Value::Array(pairs)
        };

        let mut schema = GraphSchema::new("test", create_empty_inner_sync_client());
        // Fill the cache with hash 1 = [Sent].
        let (_, new_names) = parse_replies(
            vec![node_reply(0, element(1, Some(&["Sent"])))],
            &schema.snapshot(),
            &mut schema,
        );
        schema.offer(&new_names[0]);
        assert_eq!(schema.request_hash(), 1);

        let replies = vec![
            node_reply(1, element(2, Some(&["Sent", "Added"]))),
            node_reply(0, element(1, None)),
        ];
        let out = weave_replies(vec![None, None], &[0, 1], replies, &mut schema).expect("woven");
        let label = |item: &BatchItemResult| {
            item.as_ref().expect("decodes").data[0]
                .get_at(0)
                .and_then(crate::FalkorValue::as_node)
                .expect("a node")
                .labels
                .clone()
        };
        assert_eq!(label(&out[0]), vec!["Added"]);
        assert_eq!(label(&out[1]), vec!["Sent"]);
        assert_eq!(schema.request_hash(), 2);
    }

    #[test]
    fn prepare_all_encode_errors_pipelines_nothing() {
        let mut q = BatchQuery::write("RETURN 1");
        q.with_param("invalid name!", 1);
        let (pipe, submitted, slots) = prepare("g", &[q], None);
        assert!(submitted.is_empty());
        assert_eq!(pipe.len(), 0);
        assert!(matches!(slots[0], Some(Err(_))));
    }

    #[test]
    fn weave_replies_aligns_replies_around_a_skipped_slot() {
        let mut schema = GraphSchema::new("test", create_empty_inner_sync_client());
        // Original order: [submitted, skipped(Err), submitted].
        // Include a 3-element reply with one scalar row so `build_vec_rows` and `parse_rows`
        // are exercised without needing a server round-trip.
        let one_row_reply = redis::Value::Array(vec![
            redis::Value::Array(vec![redis::Value::Array(vec![redis::Value::BulkString(
                b"n".to_vec(),
            )])]),
            redis::Value::Array(vec![redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::Int(3),
                redis::Value::Int(7),
            ])])]),
            redis::Value::Array(vec![]),
        ]);
        let slots: Vec<Option<BatchItemResult>> = vec![
            None,
            Some(Err(FalkorDBError::RedisError("encode".to_string()))),
            None,
        ];
        let submitted = vec![0usize, 2usize];
        let replies = vec![stats_only_reply(), one_row_reply];

        let out = weave_replies(slots, &submitted, replies, &mut schema).expect("woven");

        assert_eq!(out.len(), 3);
        assert!(out[0].is_ok(), "first submitted query parsed");
        assert!(
            matches!(&out[1], Err(FalkorDBError::RedisError(m)) if m == "encode"),
            "the pre-filled encode error is preserved in its original slot"
        );
        assert_eq!(
            out[2].as_ref().unwrap().data.len(),
            1,
            "second submitted query parsed into slot 2, not slot 1"
        );
    }

    #[test]
    fn weave_replies_errors_on_reply_count_mismatch() {
        let mut schema = GraphSchema::new("test", create_empty_inner_sync_client());
        // Two commands submitted but only one reply -> a protocol surprise, surfaced as Err
        // (never a panic).
        let too_few = weave_replies(
            vec![None, None],
            &[0, 1],
            vec![stats_only_reply()],
            &mut schema,
        );
        assert!(matches!(too_few, Err(FalkorDBError::RedisParsingError(_))));
        // More replies than commands also errors.
        let too_many = weave_replies(
            vec![None],
            &[0],
            vec![stats_only_reply(), stats_only_reply()],
            &mut schema,
        );
        assert!(matches!(too_many, Err(FalkorDBError::RedisParsingError(_))));
    }
}

#[cfg(test)]
mod proptest {
    use super::*;
    use crate::client::blocking::create_empty_inner_sync_client;
    use ::proptest::prelude::*;

    fn stats_only_reply() -> redis::Value {
        redis::Value::Array(vec![redis::Value::Array(vec![])])
    }

    /// `true` = an encodable (good) query, `false` = a query whose param fails to encode (skipped).
    fn batch_of(good_flags: &[bool]) -> Vec<BatchQuery> {
        good_flags
            .iter()
            .map(|&good| {
                let mut q = BatchQuery::write("RETURN 1");
                if !good {
                    q.with_param("invalid name!", 1); // never encodes
                }
                q
            })
            .collect()
    }

    proptest! {
        /// For any mix of encodable and non-encodable queries, `prepare` + `weave_replies` keep the
        /// results 1:1 with submission order: every good query maps to its reply, every skipped
        /// query keeps its pre-filled error, and lengths never drift (the off-by-one guard).
        #[test]
        fn skip_and_weave_preserves_submission_order(good_flags in prop::collection::vec(any::<bool>(), 0..16)) {
            let queries = batch_of(&good_flags);
            let (pipe, submitted, slots) = prepare("g", &queries, None);

            let expected_submitted: Vec<usize> = good_flags
                .iter()
                .enumerate()
                .filter_map(|(i, &good)| good.then_some(i))
                .collect();
            prop_assert_eq!(&submitted, &expected_submitted);
            prop_assert_eq!(pipe.len(), submitted.len());
            for (i, &good) in good_flags.iter().enumerate() {
                prop_assert_eq!(slots[i].is_some(), !good);
            }

            // Simulate a successful dispatch: one reply per submitted command.
            let replies = vec![stats_only_reply(); submitted.len()];
            let mut schema = GraphSchema::new("test", create_empty_inner_sync_client());
            let out = weave_replies(slots, &submitted, replies, &mut schema).expect("woven");

            prop_assert_eq!(out.len(), good_flags.len());
            for (i, &good) in good_flags.iter().enumerate() {
                prop_assert_eq!(out[i].is_ok(), good);
            }
        }
    }
}
