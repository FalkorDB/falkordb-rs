/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Batch / pipelined execution: queue many queries and dispatch them over a single Redis pipeline
//! in one round-trip, with per-query results in submission order.

use crate::graph::query_builder::{
    build_vec_rows, construct_query, dispatch_query_response, unwrap_query_response,
};
use crate::graph::HasGraphSchema;
use crate::{
    FalkorDBError, FalkorParams, FalkorResult, GraphSchema, IntoFalkorParam, IntoFalkorParams,
    QueryResult, Row, SyncGraph,
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
}

impl<'a, G> BatchBuilder<'a, G> {
    pub(crate) fn new(graph: &'a mut G) -> Self {
        Self {
            graph,
            queries: Vec::new(),
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
}

/// Builds the pipeline, pre-filling a result slot for every query that fails to encode (those are
/// never sent), and recording the original index of each *submitted* command so replies can be
/// woven back into submission order.
fn prepare(
    graph_name: &str,
    queries: &[BatchQuery],
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
                    pipe.arg(format!("timeout {timeout}"));
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

/// Parses each reply (in submission order) into its original slot under the graph schema, then
/// returns the fully-populated, in-order results. Errors if the server returned a different number
/// of replies than commands submitted (a protocol surprise), so the contract can never panic.
fn weave_replies(
    mut slots: Vec<Option<BatchItemResult>>,
    submitted: &[usize],
    replies: Vec<redis::Value>,
    graph_schema: &mut GraphSchema,
) -> BatchResult {
    if replies.len() != submitted.len() {
        return Err(FalkorDBError::RedisParsingError(format!(
            "batch expected {} replies but the server returned {}",
            submitted.len(),
            replies.len()
        )));
    }
    for (reply, &index) in replies.into_iter().zip(submitted) {
        slots[index] = Some(
            unwrap_query_response(reply)
                .and_then(|res| dispatch_query_response(res, &mut *graph_schema, build_vec_rows)),
        );
    }
    Ok(slots
        .into_iter()
        .map(|slot| slot.expect("every slot is filled: encode errors up front, replies by index"))
        .collect())
}

impl BatchBuilder<'_, SyncGraph> {
    /// Dispatch all queued queries over one pipelined round-trip and return their results in
    /// submission order. An empty batch (or one whose every query failed to encode) returns without
    /// a round-trip.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Execute Batch", skip_all, level = "info")
    )]
    pub fn execute(self) -> BatchResult {
        let (pipe, submitted, slots) = prepare(self.graph.graph_name(), &self.queries);
        if submitted.is_empty() {
            return Ok(slots
                .into_iter()
                .map(|slot| slot.expect("all filled"))
                .collect());
        }

        let client = self.graph.get_client().clone();
        let conn = if has_write(&self.queries) {
            client.borrow_connection(client.clone())
        } else {
            client.borrow_readonly_connection(client.clone())
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
        let (pipe, submitted, slots) = prepare(self.graph.graph_name(), &self.queries);
        if submitted.is_empty() {
            return Ok(slots
                .into_iter()
                .map(|slot| slot.expect("all filled"))
                .collect());
        }

        let client = self.graph.get_client().clone();
        let conn = if has_write(&self.queries) {
            client.borrow_connection(client.clone()).await
        } else {
            client.borrow_readonly_connection(client.clone()).await
        };
        let replies = conn?.execute_pipeline(&pipe).await?;

        let schema = self.graph.schema_handle();
        let mut guard = schema.write();
        weave_replies(slots, &submitted, replies, &mut guard)
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

        let (pipe, submitted, slots) = prepare("g", &queries);

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

    #[test]
    fn prepare_all_encode_errors_pipelines_nothing() {
        let mut q = BatchQuery::write("RETURN 1");
        q.with_param("invalid name!", 1);
        let (pipe, submitted, slots) = prepare("g", &[q]);
        assert!(submitted.is_empty());
        assert_eq!(pipe.len(), 0);
        assert!(matches!(slots[0], Some(Err(_))));
    }

    #[test]
    fn weave_replies_aligns_replies_around_a_skipped_slot() {
        let mut schema = GraphSchema::new("test", create_empty_inner_sync_client());
        // Original order: [submitted, skipped(Err), submitted].
        let slots: Vec<Option<BatchItemResult>> = vec![
            None,
            Some(Err(FalkorDBError::RedisError("encode".to_string()))),
            None,
        ];
        let submitted = vec![0usize, 2usize];
        let replies = vec![stats_only_reply(), stats_only_reply()];

        let out = weave_replies(slots, &submitted, replies, &mut schema).expect("woven");

        assert_eq!(out.len(), 3);
        assert!(out[0].is_ok(), "first submitted query parsed");
        assert!(
            matches!(&out[1], Err(FalkorDBError::RedisError(m)) if m == "encode"),
            "the pre-filled encode error is preserved in its original slot"
        );
        assert!(
            out[2].is_ok(),
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
            let (pipe, submitted, slots) = prepare("g", &queries);

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
