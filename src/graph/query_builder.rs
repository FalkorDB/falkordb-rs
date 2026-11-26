/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    connection::reject_server_error,
    graph::HasGraphSchema,
    graph_schema::SchemaHandle,
    parser::{parse_header, redis_value_as_vec, SchemaParsable},
    retry::{op_kind_for_command, run_with_retry_blocking, OpKind},
    value::param::is_bare_identifier,
    Constraint, ExecutionPlan, FalkorDBError, FalkorIndex, FalkorParams, FalkorResult, GraphSchema,
    IntoFalkorParam, IntoFalkorParams, LazyResultSet, QueryResult, ReadPreference, SyncGraph,
};
use std::sync::Arc;
use std::{
    fmt::{Display, Write},
    marker::PhantomData,
};

#[cfg(feature = "tokio")]
use crate::{retry::run_with_retry_async, AsyncGraph, RowStream};

#[cfg(feature = "serde")]
use crate::TypedLazyResultSet;
#[cfg(all(feature = "serde", feature = "tokio"))]
use crate::TypedRowStream;

/// Builds a [`QueryResult`] over a [`LazyResultSet`] from an already-parsed header and the raw row
/// values, sharing one `Arc` header with the result set.
fn build_lazy_query_result<'g>(
    header: Arc<[String]>,
    rows: Vec<redis::Value>,
    stats: redis::Value,
    graph_schema: impl Into<SchemaHandle<'g>>,
) -> FalkorResult<QueryResult<LazyResultSet<'g>>> {
    let data = LazyResultSet::new(Arc::clone(&header), rows, graph_schema);
    QueryResult::from_response(header, data, stats)
}

/// Builds a [`QueryResult`] over an owned [`RowStream`] by pre-scanning compact schema ids against
/// `graph_schema` and capturing a schema snapshot, so rows decode lazily on `poll_next` while the
/// stream remains decoupled from later changes to the graph's schema.
#[cfg(feature = "tokio")]
fn build_row_stream_result(
    header: Arc<[String]>,
    rows: Vec<redis::Value>,
    stats: redis::Value,
    graph_schema: &mut GraphSchema,
) -> FalkorResult<QueryResult<crate::RowStream>> {
    let data = crate::RowStream::parse(Arc::clone(&header), rows, graph_schema);
    QueryResult::from_response(header, data, stats)
}

/// Builds a [`QueryResult`] over an eagerly-parsed `Vec<Row>`, used by the batch API where N result
/// sets coexist (so a borrowing [`LazyResultSet`] is not an option). A row that fails to parse
/// collapses the whole result to that `Err`.
pub(crate) fn build_vec_rows(
    header: Arc<[String]>,
    rows: Vec<redis::Value>,
    stats: redis::Value,
    graph_schema: &mut GraphSchema,
) -> FalkorResult<QueryResult<Vec<crate::Row>>> {
    let data = crate::response::row::parse_rows(Arc::clone(&header), rows, graph_schema)
        .into_iter()
        .collect::<FalkorResult<Vec<_>>>()?;
    QueryResult::from_response(header, data, stats)
}

/// The `SCHEMA_HASH` argument for `command`, if the request should carry it: only `GRAPH.QUERY` and
/// `GRAPH.RO_QUERY` get the schema element back (`GRAPH.EXPLAIN` and `GRAPH.PROFILE` don't), and
/// only when the client has it enabled. Older servers ignore the argument.
pub(crate) fn schema_hash_arg(
    enabled: bool,
    command: &str,
    hash: impl FnOnce() -> i64,
) -> Option<String> {
    (enabled && matches!(command, "GRAPH.QUERY" | "GRAPH.RO_QUERY")).then(|| hash().to_string())
}

/// The arguments after the graph name: the query, `--compact`, then the optional timeout and
/// schema hash. At most 7, so with the command the request stays within the server's limit of 8.
fn query_args<'s>(
    query: &'s str,
    timeout: Option<&'s str>,
    schema_hash: Option<&'s str>,
) -> Vec<&'s str> {
    let mut args = vec![query, "--compact"];
    if let Some(timeout) = timeout {
        args.extend(["timeout", timeout]);
    }
    if let Some(hash) = schema_hash {
        args.extend(["SCHEMA_HASH", hash]);
    }
    args
}

/// Unwraps a top-level query reply: a server error becomes an `Err` (as for any command, see
/// [`reject_server_error`]), otherwise the reply is read as the response array. Batch replies come
/// through here per command, since a pipeline keeps each command's error in its own slot.
pub(crate) fn unwrap_query_response(value: redis::Value) -> FalkorResult<Vec<redis::Value>> {
    reject_server_error(value).and_then(redis_value_as_vec)
}

/// Turns the top-level query response array into a [`QueryResult`]: one element is stats only, two
/// is a header with no rows, three is header + rows + stats. Any other length is malformed. The
/// `build` closure constructs the concrete result set (sync [`LazyResultSet`] or async
/// [`RowStream`](crate::RowStream)) from the parsed header, rows, stats, and `schema` handle.
pub(crate) fn dispatch_query_response<S, D>(
    res: Vec<redis::Value>,
    schema: S,
    build: impl FnOnce(
        Arc<[String]>,
        Vec<redis::Value>,
        redis::Value,
        S,
    ) -> FalkorResult<QueryResult<D>>,
) -> FalkorResult<QueryResult<D>> {
    match res.len() {
        1 => {
            let stats =
                res.into_iter()
                    .next()
                    .ok_or(FalkorDBError::ParsingArrayToStructElementCount(
                        "One element exists but using next() failed",
                    ))?;

            build(Vec::new().into(), Vec::new(), stats, schema)
        }
        2 => {
            // The match arm guarantees the length, so the fixed-size conversion cannot fail.
            let [header, stats]: [redis::Value; 2] =
                res.try_into().expect("length checked to be exactly two");

            let header: Arc<[String]> = parse_header(header)?.into();
            build(header, Vec::new(), stats, schema)
        }
        3 => {
            let [header, data, stats]: [redis::Value; 3] =
                res.try_into().expect("length checked to be exactly three");

            let header: Arc<[String]> = parse_header(header)?.into();
            let rows = redis_value_as_vec(data)?;
            build(header, rows, stats, schema)
        }
        _ => Err(FalkorDBError::ParsingArrayToStructElementCount(
            "Invalid number of elements returned from query",
        )),
    }
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Construct Query", skip_all, level = "trace")
)]
pub(crate) fn construct_query<Q: Display>(
    query_str: Q,
    params: &FalkorParams,
) -> FalkorResult<String> {
    let mut query = String::new();
    params.encode_preamble(&mut query)?;
    let _ = write!(query, "{query_str}");
    Ok(query)
}

/// Resolve whether a read-only-capable command should be served from a replica connection.
///
/// `requested` is the per-request override (`None` means "inherit the client default"). The three
/// concepts are kept separate: `is_ro_command` is the command's read-only semantics, the resolved
/// [`ReadPreference`] is the requested policy, and the returned `bool` is the actual route. A
/// replica route is only taken for a read-only command whose effective preference is
/// [`ReadPreference::PreferReplica`]; the borrow path still falls back to the primary when no
/// replica connection exists.
///
/// Returns [`FalkorDBError::ReadPreferenceNotReadOnly`] when a replica preference was explicitly
/// requested for a command that can write — a write can never be routed to a replica.
pub(crate) fn resolve_use_replica(
    is_ro_command: bool,
    requested: Option<ReadPreference>,
    client_default: ReadPreference,
    context: &'static str,
) -> FalkorResult<bool> {
    if requested == Some(ReadPreference::PreferReplica) && !is_ro_command {
        return Err(FalkorDBError::ReadPreferenceNotReadOnly { context });
    }
    let effective = requested.unwrap_or(client_default);
    Ok(is_ro_command && effective == ReadPreference::PreferReplica)
}

/// A Builder-pattern struct that allows creating and executing queries on a graph
pub struct QueryBuilder<'a, Output, T: Display, G> {
    _unused: PhantomData<Output>,
    graph: &'a mut G,
    command: &'a str,
    query_string: T,
    params: FalkorParams,
    timeout: Option<i64>,
    read_preference: Option<ReadPreference>,
}

impl<'a, Output, T: Display, G> QueryBuilder<'a, Output, T, G> {
    pub(crate) fn new(
        graph: &'a mut G,
        command: &'a str,
        query_string: T,
    ) -> Self {
        Self {
            _unused: PhantomData,
            graph,
            command,
            query_string,
            params: FalkorParams::new(),
            timeout: None,
            read_preference: None,
        }
    }

    /// Add a single typed parameter, referenced as `$key` in the query.
    ///
    /// The value is encoded as a Cypher literal and escaped for you, so strings, lists and maps
    /// are safe from Cypher injection. Any encoding error (for example a non-finite float, or an
    /// invalid parameter name) is reported when the query is executed; use
    /// [`try_with_param`](Self::try_with_param) to fail eagerly instead.
    ///
    /// The query string itself must not already start with a manual `CYPHER name=value` preamble;
    /// the preamble is generated from the parameters added here.
    ///
    /// # Arguments
    /// * `key`: the parameter name (a Cypher identifier), referenced as `$key`
    /// * `value`: any value implementing [`IntoFalkorParam`]
    pub fn with_param<V: IntoFalkorParam>(
        mut self,
        key: &str,
        value: V,
    ) -> Self {
        self.params.add_param(key, value);
        self
    }

    /// Like [`with_param`](Self::with_param), but returns an error immediately if the parameter
    /// name or value cannot be encoded, rather than deferring it to execution.
    pub fn try_with_param<V: IntoFalkorParam>(
        mut self,
        key: &str,
        value: V,
    ) -> FalkorResult<Self> {
        self.params.add_param(key, value);
        match self.params.first_error() {
            Some(err) => Err(err),
            None => Ok(self),
        }
    }

    /// Add several typed parameters at once, e.g. from a `Vec<(key, value)>`, an array of pairs,
    /// or a `HashMap`/`BTreeMap`. See [`IntoFalkorParams`].
    pub fn with_params<P: IntoFalkorParams>(
        mut self,
        params: P,
    ) -> Self {
        self.params.merge(params.into_falkor_params());
        self
    }

    /// Escape hatch: insert a raw, **already-valid** Cypher expression as the parameter value.
    ///
    /// No escaping is performed on the value (the name is still validated), so a malformed
    /// expression can reintroduce Cypher injection — prefer [`with_param`](Self::with_param).
    pub fn with_raw_param(
        mut self,
        key: &str,
        raw_cypher: impl Into<String>,
    ) -> Self {
        self.params.add_raw(key, raw_cypher.into());
        self
    }

    /// Specify a timeout after which to abort the query
    ///
    /// # Arguments
    /// * `timeout`: the timeout after which the server is allowed to abort or throw this request,
    ///   in milliseconds, when that happens the server will return a timeout error
    pub fn with_timeout(
        self,
        timeout: i64,
    ) -> Self {
        Self {
            timeout: Some(timeout),
            ..self
        }
    }

    /// Override the [`ReadPreference`] for this single query, ignoring the client-wide default.
    ///
    /// Only read-only queries (`ro_query`) can be routed to a replica. Setting
    /// [`ReadPreference::PreferReplica`] on a writable `query` makes `execute()` fail with
    /// [`FalkorDBError::ReadPreferenceNotReadOnly`](crate::FalkorDBError::ReadPreferenceNotReadOnly),
    /// since a write can never run on a replica. See [`prefer_replica`](Self::prefer_replica) and
    /// [`primary_only`](Self::primary_only) for the common shortcuts.
    ///
    /// # Arguments
    /// * `read_preference`: the [`ReadPreference`] to apply to this query.
    pub fn with_read_preference(
        self,
        read_preference: ReadPreference,
    ) -> Self {
        Self {
            read_preference: Some(read_preference),
            ..self
        }
    }

    /// Route this read-only query to a replica when one is available, falling back to the primary
    /// otherwise. Shortcut for [`with_read_preference(ReadPreference::PreferReplica)`](Self::with_read_preference).
    ///
    /// Replicas can be slightly stale, so only use this when a marginally out-of-date read is
    /// acceptable. Has effect only on `ro_query`; on a writable `query` it makes `execute()` fail.
    pub fn prefer_replica(self) -> Self {
        self.with_read_preference(ReadPreference::PreferReplica)
    }

    /// Serve this query from the primary, overriding a client default of
    /// [`ReadPreference::PreferReplica`]. Shortcut for
    /// [`with_read_preference(ReadPreference::Primary)`](Self::with_read_preference). Use this for
    /// read-your-writes paths that must not observe replication lag.
    pub fn primary_only(self) -> Self {
        self.with_read_preference(ReadPreference::Primary)
    }
}

impl<'a, Output, T: Display, G: HasGraphSchema> QueryBuilder<'a, Output, T, G> {
    fn generate_query_result_set(
        self,
        value: redis::Value,
    ) -> FalkorResult<QueryResult<LazyResultSet<'a>>> {
        let mut res = unwrap_query_response(value)?;
        let schema = self.graph.get_graph_schema_mut().for_reply(&mut res)?;
        dispatch_query_response(res, schema, build_lazy_query_result)
    }
}

impl<Out, T: Display> QueryBuilder<'_, Out, T, SyncGraph> {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Common Query Execution Steps",
            skip_all,
            fields(
                db.system.name = "falkordb",
                db.namespace = %self.graph.graph_name(),
                db.operation.name = %self.command,
                db.falkordb.read_only = tracing::field::Empty,
                db.falkordb.strategy = tracing::field::Empty,
                db.query.fingerprint = tracing::field::Empty,
                db.query.text = tracing::field::Empty,
                error.type = tracing::field::Empty,
            ),
            level = "trace"
        )
    )]
    fn common_execute_steps(&mut self) -> FalkorResult<redis::Value> {
        let query = construct_query(&self.query_string, &self.params)?;

        let command = self.command;
        let timeout = self.timeout.map(|timeout| timeout.to_string());
        let schema_hash = schema_hash_arg(self.graph.get_client().schema_hash(), command, || {
            self.graph.get_graph_schema_mut().request_hash()
        });
        let params = query_args(&query, timeout.as_deref(), schema_hash.as_deref());

        let client = self.graph.get_client();
        let op_kind = op_kind_for_command(command);

        #[cfg(any(feature = "tracing", feature = "metrics"))]
        let strategy = crate::observability::SYNC_STRATEGY;
        #[cfg(feature = "tracing")]
        crate::observability::record_request(
            strategy,
            matches!(op_kind, OpKind::ReadOnly),
            &self.query_string.to_string(),
            client.query_logging(),
        );

        let graph_name = self.graph.graph_name();
        let use_replica = resolve_use_replica(
            command == "GRAPH.RO_QUERY",
            self.read_preference,
            client.read_preference(),
            "query",
        )?;
        let params_ref = params.as_slice();
        let policy = client.retry_policy();

        #[cfg(feature = "metrics")]
        let metrics_start = std::time::Instant::now();
        let result = run_with_retry_blocking(&policy, op_kind, || {
            let conn = if use_replica {
                client.borrow_readonly_connection(client.clone())
            } else {
                client.borrow_connection(client.clone())
            };
            conn.and_then(|mut conn| {
                conn.execute_command(Some(graph_name), command, None, Some(params_ref))
            })
        });

        #[cfg(feature = "tracing")]
        if let Err(ref err) = result {
            crate::observability::record_error(err);
        }
        #[cfg(feature = "metrics")]
        crate::observability::record_query_metrics(
            command,
            matches!(op_kind, OpKind::ReadOnly),
            strategy,
            metrics_start.elapsed(),
            result.as_ref().err(),
        );
        result
    }
}

#[cfg(feature = "tokio")]
impl<'a, Out, T: Display> QueryBuilder<'a, Out, T, AsyncGraph> {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Common Query Execution Steps",
            skip_all,
            fields(
                db.system.name = "falkordb",
                db.namespace = %self.graph.graph_name(),
                db.operation.name = %self.command,
                db.falkordb.read_only = tracing::field::Empty,
                db.falkordb.strategy = tracing::field::Empty,
                db.query.fingerprint = tracing::field::Empty,
                db.query.text = tracing::field::Empty,
                error.type = tracing::field::Empty,
            ),
            level = "trace"
        )
    )]
    /// Sends the query, returning the reply and the schema whose hash it carried.
    async fn common_execute_steps(&mut self) -> FalkorResult<(redis::Value, GraphSchema)> {
        let query = construct_query(&self.query_string, &self.params)?;

        let command = self.command;
        let timeout = self.timeout.map(|timeout| timeout.to_string());
        let sent = self.graph.schema_snapshot();
        let schema_hash = schema_hash_arg(self.graph.get_client().schema_hash(), command, || {
            sent.request_hash()
        });
        let params = query_args(&query, timeout.as_deref(), schema_hash.as_deref());

        let client = self.graph.get_client();
        let op_kind = op_kind_for_command(command);

        #[cfg(any(feature = "tracing", feature = "metrics"))]
        let strategy = crate::observability::strategy_label(&client.strategy());
        #[cfg(feature = "tracing")]
        crate::observability::record_request(
            strategy,
            matches!(op_kind, OpKind::ReadOnly),
            &self.query_string.to_string(),
            client.query_logging(),
        );

        let graph_name = self.graph.graph_name();
        let use_replica = resolve_use_replica(
            command == "GRAPH.RO_QUERY",
            self.read_preference,
            client.read_preference(),
            "query",
        )?;
        let params_ref = params.as_slice();
        let policy = client.retry_policy();

        #[cfg(feature = "metrics")]
        let metrics_start = std::time::Instant::now();
        let result = run_with_retry_async(&policy, op_kind, || async move {
            let conn = if use_replica {
                client.borrow_readonly_connection(client.clone()).await
            } else {
                client.borrow_connection(client.clone()).await
            };
            conn?
                .execute_command(Some(graph_name), command, None, Some(params_ref))
                .await
        })
        .await;

        #[cfg(feature = "tracing")]
        if let Err(ref err) = result {
            crate::observability::record_error(err);
        }
        #[cfg(feature = "metrics")]
        crate::observability::record_query_metrics(
            command,
            matches!(op_kind, OpKind::ReadOnly),
            strategy,
            metrics_start.elapsed(),
            result.as_ref().err(),
        );
        result.map(|value| (value, sent))
    }

    /// Builds an owned [`RowStream`] from the reply, pre-resolving any unknown compact schema ids
    /// via an async refresh (see [`AsyncGraph::parse_with_schema`]) and capturing a schema
    /// snapshot so rows decode lazily on `poll_next` while outliving the borrow of the graph,
    /// staying `Send + 'static`, and remaining decoupled from later changes to the graph's schema.
    async fn generate_async_result_set(
        self,
        value: redis::Value,
        sent: &GraphSchema,
    ) -> FalkorResult<QueryResult<RowStream>> {
        self.graph
            .decode_reply(unwrap_query_response(value)?, sent, |res, schema| {
                dispatch_query_response(res, schema, build_row_stream_result)
            })
            .await
    }
}

impl<'a, T: Display> QueryBuilder<'a, QueryResult<LazyResultSet<'a>>, T, SyncGraph> {
    /// Executes the query, returning a [`QueryResult`], with a [`LazyResultSet`] as its `data` member
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Execute Lazy Result Set Query",
            skip_all,
            fields(
                db.response.returned_rows = tracing::field::Empty,
                db.falkordb.server_time_ms = tracing::field::Empty,
            ),
            level = "info"
        )
    )]
    pub fn execute(mut self) -> FalkorResult<QueryResult<LazyResultSet<'a>>> {
        let result = self
            .common_execute_steps()
            .and_then(|res| self.generate_query_result_set(res))?;
        #[cfg(feature = "tracing")]
        crate::observability::record_result(
            result.data.len(),
            result.get_internal_execution_time(),
        );
        Ok(result)
    }
}

#[cfg(feature = "tokio")]
impl<'a, T: Display> QueryBuilder<'a, QueryResult<RowStream>, T, AsyncGraph> {
    /// Executes the query, returning a [`QueryResult`] whose `data` is an owned [`RowStream`] — a
    /// `Send + 'static` [`futures_core::Stream`] of `FalkorResult<Row>`.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Execute Row Stream Query",
            skip_all,
            fields(
                db.response.returned_rows = tracing::field::Empty,
                db.falkordb.server_time_ms = tracing::field::Empty,
            ),
            level = "info"
        )
    )]
    pub async fn execute(mut self) -> FalkorResult<QueryResult<RowStream>> {
        let (res, sent) = self.common_execute_steps().await?;
        let result = self.generate_async_result_set(res, &sent).await?;
        #[cfg(feature = "tracing")]
        crate::observability::record_result(
            result.data.len(),
            result.get_internal_execution_time(),
        );
        Ok(result)
    }
}

#[cfg(feature = "serde")]
impl<'a, T: Display, G: HasGraphSchema> QueryBuilder<'a, QueryResult<LazyResultSet<'a>>, T, G> {
    /// Map each result row into `U`, where `U` implements [`serde::Deserialize`].
    ///
    /// This changes the type produced by [`execute`](Self::execute) from
    /// `QueryResult<LazyResultSet>` to `QueryResult<TypedLazyResultSet<U>>`, whose `data` is an
    /// iterator yielding one `FalkorResult<U>` per row. The [`header`](QueryResult::header) and
    /// [`stats`](QueryResult::stats) are preserved.
    ///
    /// A single-column row maps the column's value (so `RETURN m` maps the node and
    /// `RETURN n.name` maps the scalar); a multi-column row maps column name to value for
    /// structs and maps, or the values in order for tuples and [`Vec`]. See
    /// [`from_falkor_row`](crate::from_falkor_row) for the full mapping rules.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let result = graph.query("MATCH (m:Movie) RETURN m").query_as::<Movie>().execute()?;
    /// let movies: Vec<Movie> = result.data.collect::<Result<_, _>>()?;
    /// ```
    pub fn query_as<U>(self) -> QueryBuilder<'a, QueryResult<TypedLazyResultSet<'a, U>>, T, G>
    where
        U: serde::de::DeserializeOwned,
    {
        QueryBuilder {
            _unused: PhantomData,
            graph: self.graph,
            command: self.command,
            query_string: self.query_string,
            params: self.params,
            timeout: self.timeout,
            read_preference: self.read_preference,
        }
    }
}

#[cfg(feature = "serde")]
impl<'a, T: Display, U> QueryBuilder<'a, QueryResult<TypedLazyResultSet<'a, U>>, T, SyncGraph>
where
    U: serde::de::DeserializeOwned,
{
    /// Executes the query, returning a [`QueryResult`] whose `data` is a
    /// [`TypedLazyResultSet`] that deserializes each row into `U`.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Execute Typed Query",
            skip_all,
            fields(
                db.response.returned_rows = tracing::field::Empty,
                db.falkordb.server_time_ms = tracing::field::Empty,
            ),
            level = "info"
        )
    )]
    pub fn execute(mut self) -> FalkorResult<QueryResult<TypedLazyResultSet<'a, U>>> {
        let result = self
            .common_execute_steps()
            .and_then(|res| self.generate_query_result_set(res))
            .map(|result| result.into_typed::<U>())?;
        #[cfg(feature = "tracing")]
        crate::observability::record_result(
            result.data.len(),
            result.get_internal_execution_time(),
        );
        Ok(result)
    }
}

#[cfg(all(feature = "serde", feature = "tokio"))]
impl<'a, T: Display> QueryBuilder<'a, QueryResult<RowStream>, T, AsyncGraph> {
    /// Map each result row into `U` (the async counterpart of the sync `query_as`): the result of
    /// [`execute`](Self::execute) becomes a `QueryResult<TypedRowStream<U>>`, a `Send + 'static`
    /// stream of `FalkorResult<U>`. See [`from_falkor_row`](crate::from_falkor_row) for the mapping.
    pub fn query_as<U>(self) -> QueryBuilder<'a, QueryResult<TypedRowStream<U>>, T, AsyncGraph>
    where
        U: serde::de::DeserializeOwned,
    {
        QueryBuilder {
            _unused: PhantomData,
            graph: self.graph,
            command: self.command,
            query_string: self.query_string,
            params: self.params,
            timeout: self.timeout,
            read_preference: self.read_preference,
        }
    }
}

#[cfg(all(feature = "serde", feature = "tokio"))]
impl<'a, T: Display, U> QueryBuilder<'a, QueryResult<TypedRowStream<U>>, T, AsyncGraph>
where
    U: serde::de::DeserializeOwned,
{
    /// Executes the query, returning a [`QueryResult`] whose `data` is a [`TypedRowStream`] that
    /// deserializes each row into `U`.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Execute Typed Row Stream Query",
            skip_all,
            fields(
                db.response.returned_rows = tracing::field::Empty,
                db.falkordb.server_time_ms = tracing::field::Empty,
            ),
            level = "info"
        )
    )]
    pub async fn execute(mut self) -> FalkorResult<QueryResult<TypedRowStream<U>>> {
        let (res, sent) = self.common_execute_steps().await?;
        let result = self
            .generate_async_result_set(res, &sent)
            .await?
            .into_typed::<U>();
        #[cfg(feature = "tracing")]
        crate::observability::record_result(
            result.data.len(),
            result.get_internal_execution_time(),
        );
        Ok(result)
    }
}

impl<T: Display> QueryBuilder<'_, ExecutionPlan, T, SyncGraph> {
    /// Executes the query, returning an [`ExecutionPlan`] from the data returned
    pub fn execute(mut self) -> FalkorResult<ExecutionPlan> {
        self.common_execute_steps().and_then(ExecutionPlan::parse)
    }
}

#[cfg(feature = "tokio")]
impl<'a, T: Display> QueryBuilder<'a, ExecutionPlan, T, AsyncGraph> {
    /// Executes the query, returning an [`ExecutionPlan`] from the data returned
    pub async fn execute(mut self) -> FalkorResult<ExecutionPlan> {
        self.common_execute_steps()
            .await
            .and_then(|(value, _)| ExecutionPlan::parse(value))
    }
}

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Generate Procedure Call", skip_all, level = "trace")
)]
pub(crate) fn generate_procedure_call(
    procedure: &str,
    args: Option<&[&str]>,
    yields: Option<&[&str]>,
) -> FalkorResult<(String, FalkorParams)> {
    // The procedure and its yield columns are written into the query as they are, so a name that
    // isn't an identifier could add clauses to the call.
    if !procedure.split('.').all(is_bare_identifier) {
        return Err(FalkorDBError::InvalidIdentifier {
            kind: "procedure",
            name: procedure.to_string(),
            message: "must be Cypher identifiers ([A-Za-z_][A-Za-z0-9_]*) joined by dots",
        });
    }
    if let Some(column) = yields
        .unwrap_or_default()
        .iter()
        .find(|column| !is_bare_identifier(column))
    {
        return Err(FalkorDBError::InvalidIdentifier {
            kind: "yield column",
            name: column.to_string(),
            message: "must be a Cypher identifier ([A-Za-z_][A-Za-z0-9_]*)",
        });
    }

    let mut params = FalkorParams::new();
    let args_str = args
        .unwrap_or_default()
        .iter()
        .enumerate()
        .map(|(idx, arg)| {
            let name = format!("param{idx}");
            params.add_param(&name, *arg);
            format!("${name}")
        })
        .collect::<Vec<_>>()
        .join(",");
    let mut query_string = format!("CALL {procedure}({args_str})");

    if let Some(yields) = yields {
        query_string += format!(" YIELD {}", yields.join(",")).as_str();
    }

    Ok((query_string, params))
}

/// A Builder-pattern struct that allows creating and executing procedure call on a graph
pub struct ProcedureQueryBuilder<'a, Output, G> {
    _unused: PhantomData<Output>,
    graph: &'a mut G,
    readonly: bool,
    op_kind: OpKind,
    procedure_name: &'a str,
    args: Option<&'a [&'a str]>,
    yields: Option<&'a [&'a str]>,
    read_preference: Option<ReadPreference>,
}

impl<'a, Out, G> ProcedureQueryBuilder<'a, Out, G> {
    pub(crate) fn new(
        graph: &'a mut G,
        procedure_name: &'a str,
    ) -> Self {
        Self {
            _unused: PhantomData,

            graph,
            readonly: false,
            op_kind: OpKind::Write,
            procedure_name,
            args: None,
            yields: None,
            read_preference: None,
        }
    }

    pub(crate) fn new_readonly(
        graph: &'a mut G,
        procedure_name: &'a str,
    ) -> Self {
        Self {
            _unused: PhantomData,
            graph,
            readonly: true,
            op_kind: OpKind::ReadOnly,
            procedure_name,
            args: None,
            yields: None,
            read_preference: None,
        }
    }

    /// Mark this procedure call as read-only / idempotent for retry purposes, independent of the
    /// wire command. Used by internal listing procedures (`DB.INDEXES`, `DB.CONSTRAINTS`) which are
    /// dispatched over `GRAPH.QUERY` yet only read, so they remain safe to re-issue.
    pub(crate) fn read_only(mut self) -> Self {
        self.op_kind = OpKind::ReadOnly;
        self
    }

    /// Pass arguments to the procedure call
    ///
    /// # Arguments
    /// * `args`: The arguments to pass
    pub fn with_args(
        self,
        args: &'a [&str],
    ) -> Self {
        Self {
            args: Some(args),
            ..self
        }
    }

    /// Tell the procedure call it should yield the following results
    ///
    /// Each column is written into the query as it is, so it must be a Cypher identifier
    /// (`[A-Za-z_][A-Za-z0-9_]*`); otherwise `execute()` fails with
    /// [`FalkorDBError::InvalidIdentifier`](crate::FalkorDBError::InvalidIdentifier).
    ///
    /// # Arguments
    /// * `yields`: The values to yield
    pub fn with_yields(
        self,
        yields: &'a [&str],
    ) -> Self {
        Self {
            yields: Some(yields),
            ..self
        }
    }

    /// Override the [`ReadPreference`] for this single procedure call, ignoring the client default.
    ///
    /// Only read-only procedure calls (`call_procedure_ro`) can be routed to a replica. Setting
    /// [`ReadPreference::PreferReplica`] on a writable `call_procedure` makes `execute()` fail with
    /// [`FalkorDBError::ReadPreferenceNotReadOnly`](crate::FalkorDBError::ReadPreferenceNotReadOnly).
    ///
    /// # Arguments
    /// * `read_preference`: the [`ReadPreference`] to apply to this call.
    pub fn with_read_preference(
        self,
        read_preference: ReadPreference,
    ) -> Self {
        Self {
            read_preference: Some(read_preference),
            ..self
        }
    }

    /// Route this read-only procedure call to a replica when available, else the primary. Shortcut
    /// for [`with_read_preference(ReadPreference::PreferReplica)`](Self::with_read_preference).
    /// Replicas can be slightly stale. Has effect only on `call_procedure_ro`.
    pub fn prefer_replica(self) -> Self {
        self.with_read_preference(ReadPreference::PreferReplica)
    }

    /// Serve this procedure call from the primary, overriding a client default of
    /// [`ReadPreference::PreferReplica`]. Shortcut for
    /// [`with_read_preference(ReadPreference::Primary)`](Self::with_read_preference).
    pub fn primary_only(self) -> Self {
        self.with_read_preference(ReadPreference::Primary)
    }
}

/// Parses a 3-element `[header, rows, stats]` procedure reply, its schema element already removed,
/// into a `QueryResult<Vec<T>>`, using `graph_schema` to resolve compact ids. Shared by the sync and
/// async procedure builders.
fn parse_procedure_result<T: SchemaParsable>(
    res: Vec<redis::Value>,
    graph_schema: &mut GraphSchema,
) -> FalkorResult<QueryResult<Vec<T>>> {
    let [header, indices, stats]: [redis::Value; 3] = res.try_into().map_err(|_| {
        FalkorDBError::ParsingArrayToStructElementCount(
            "Expected exactly 3 elements in query response",
        )
    })?;

    let header: Arc<[String]> = parse_header(header)?.into();
    let data = redis_value_as_vec(indices)?
        .into_iter()
        .map(|res| T::parse(res, graph_schema))
        .collect::<FalkorResult<Vec<T>>>()?;
    QueryResult::from_response(header, data, stats)
}

impl<'a, Out, G: HasGraphSchema> ProcedureQueryBuilder<'a, Out, G> {
    fn parse_query_result_of_type<T: SchemaParsable>(
        &mut self,
        res: redis::Value,
    ) -> FalkorResult<QueryResult<Vec<T>>> {
        let mut res = redis_value_as_vec(res)?;
        let mut schema = self.graph.get_graph_schema_mut().for_reply(&mut res)?;
        parse_procedure_result(res, schema.get())
    }
}

impl<Out> ProcedureQueryBuilder<'_, Out, SyncGraph> {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Common Procedure Call Execution Steps",
            skip_all,
            fields(
                db.system.name = "falkordb",
                db.namespace = %self.graph.graph_name(),
                db.operation.name = %self.procedure_name,
                db.falkordb.read_only = tracing::field::Empty,
                db.falkordb.strategy = tracing::field::Empty,
                db.query.fingerprint = tracing::field::Empty,
                db.query.text = tracing::field::Empty,
                error.type = tracing::field::Empty,
            ),
            level = "trace"
        )
    )]
    fn common_execute_steps(&mut self) -> FalkorResult<redis::Value> {
        let command = match self.readonly {
            true => "GRAPH.RO_QUERY",
            false => "GRAPH.QUERY",
        };

        let (query_string, params) =
            generate_procedure_call(self.procedure_name, self.args, self.yields)?;

        #[cfg(any(feature = "tracing", feature = "metrics"))]
        let strategy = crate::observability::SYNC_STRATEGY;
        #[cfg(feature = "tracing")]
        crate::observability::record_request(
            strategy,
            matches!(self.op_kind, OpKind::ReadOnly),
            &query_string,
            self.graph.get_client().query_logging(),
        );

        let query = construct_query(query_string, &params)?;
        let schema_hash = schema_hash_arg(self.graph.get_client().schema_hash(), command, || {
            self.graph.get_graph_schema_mut().request_hash()
        });
        let exec_params = query_args(&query, None, schema_hash.as_deref());

        let client = self.graph.get_client();
        let graph_name = self.graph.graph_name();
        let use_replica = resolve_use_replica(
            self.readonly,
            self.read_preference,
            client.read_preference(),
            "query",
        )?;
        let op_kind = self.op_kind;
        let policy = client.retry_policy();

        #[cfg(feature = "metrics")]
        let metrics_start = std::time::Instant::now();
        let result = run_with_retry_blocking(&policy, op_kind, || {
            let conn = if use_replica {
                client.borrow_readonly_connection(client.clone())
            } else {
                client.borrow_connection(client.clone())
            };
            conn.and_then(|mut conn| {
                conn.execute_command(
                    Some(graph_name),
                    command,
                    None,
                    Some(exec_params.as_slice()),
                )
            })
        });

        #[cfg(feature = "tracing")]
        if let Err(ref err) = result {
            crate::observability::record_error(err);
        }
        #[cfg(feature = "metrics")]
        crate::observability::record_query_metrics(
            command,
            matches!(op_kind, OpKind::ReadOnly),
            strategy,
            metrics_start.elapsed(),
            result.as_ref().err(),
        );
        result
    }
}

#[cfg(feature = "tokio")]
impl<'a, Out> ProcedureQueryBuilder<'a, Out, AsyncGraph> {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Common Procedure Call Execution Steps",
            skip_all,
            fields(
                db.system.name = "falkordb",
                db.namespace = %self.graph.graph_name(),
                db.operation.name = %self.procedure_name,
                db.falkordb.read_only = tracing::field::Empty,
                db.falkordb.strategy = tracing::field::Empty,
                db.query.fingerprint = tracing::field::Empty,
                db.query.text = tracing::field::Empty,
                error.type = tracing::field::Empty,
            ),
            level = "trace"
        )
    )]
    /// Sends the procedure call, returning the reply and the schema whose hash it carried.
    async fn common_execute_steps(&mut self) -> FalkorResult<(redis::Value, GraphSchema)> {
        let command = match self.readonly {
            true => "GRAPH.RO_QUERY",
            false => "GRAPH.QUERY",
        };

        let (query_string, params) =
            generate_procedure_call(self.procedure_name, self.args, self.yields)?;

        let client = self.graph.get_client();

        #[cfg(any(feature = "tracing", feature = "metrics"))]
        let strategy = crate::observability::strategy_label(&client.strategy());
        #[cfg(feature = "tracing")]
        crate::observability::record_request(
            strategy,
            matches!(self.op_kind, OpKind::ReadOnly),
            &query_string,
            client.query_logging(),
        );

        let query = construct_query(query_string, &params)?;

        let graph_name = self.graph.graph_name();
        let use_replica = resolve_use_replica(
            self.readonly,
            self.read_preference,
            client.read_preference(),
            "query",
        )?;
        let op_kind = self.op_kind;
        let sent = self.graph.schema_snapshot();
        let schema_hash = schema_hash_arg(client.schema_hash(), command, || sent.request_hash());
        let exec_params = query_args(&query, None, schema_hash.as_deref());
        let exec_params = exec_params.as_slice();
        let policy = client.retry_policy();

        #[cfg(feature = "metrics")]
        let metrics_start = std::time::Instant::now();
        let result = run_with_retry_async(&policy, op_kind, || async move {
            let conn = if use_replica {
                client.borrow_readonly_connection(client.clone()).await
            } else {
                client.borrow_connection(client.clone()).await
            };
            conn?
                .execute_command(Some(graph_name), command, None, Some(exec_params))
                .await
        })
        .await;

        #[cfg(feature = "tracing")]
        if let Err(ref err) = result {
            crate::observability::record_error(err);
        }
        #[cfg(feature = "metrics")]
        crate::observability::record_query_metrics(
            command,
            matches!(op_kind, OpKind::ReadOnly),
            strategy,
            metrics_start.elapsed(),
            result.as_ref().err(),
        );
        result.map(|value| (value, sent))
    }
}

impl ProcedureQueryBuilder<'_, QueryResult<Vec<FalkorIndex>>, SyncGraph> {
    /// Executes the procedure call and returns a [`QueryResult`] type containing a result set of [`FalkorIndex`]s
    /// This function consumes self.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Execute FalkorIndex Query",
            skip_all,
            fields(
                db.response.returned_rows = tracing::field::Empty,
                db.falkordb.server_time_ms = tracing::field::Empty,
            ),
            level = "info"
        )
    )]
    pub fn execute(mut self) -> FalkorResult<QueryResult<Vec<FalkorIndex>>> {
        let result = self
            .common_execute_steps()
            .and_then(|res| self.parse_query_result_of_type(res))?;
        #[cfg(feature = "tracing")]
        crate::observability::record_result(
            result.data.len(),
            result.get_internal_execution_time(),
        );
        Ok(result)
    }
}

#[cfg(feature = "tokio")]
impl<'a> ProcedureQueryBuilder<'a, QueryResult<Vec<FalkorIndex>>, AsyncGraph> {
    /// Executes the procedure call and returns a [`QueryResult`] type containing a result set of [`FalkorIndex`]s
    /// This function consumes self.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Execute FalkorIndex Query",
            skip_all,
            fields(
                db.response.returned_rows = tracing::field::Empty,
                db.falkordb.server_time_ms = tracing::field::Empty,
            ),
            level = "info"
        )
    )]
    pub async fn execute(mut self) -> FalkorResult<QueryResult<Vec<FalkorIndex>>> {
        let (res, sent) = self.common_execute_steps().await?;
        let result = self
            .graph
            .decode_reply(redis_value_as_vec(res)?, &sent, parse_procedure_result)
            .await?;
        #[cfg(feature = "tracing")]
        crate::observability::record_result(
            result.data.len(),
            result.get_internal_execution_time(),
        );
        Ok(result)
    }
}

impl ProcedureQueryBuilder<'_, QueryResult<Vec<Constraint>>, SyncGraph> {
    /// Executes the procedure call and returns a [`QueryResult`] type containing a result set of [`Constraint`]s
    /// This function consumes self.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Execute Constraint Procedure Call",
            skip_all,
            fields(
                db.response.returned_rows = tracing::field::Empty,
                db.falkordb.server_time_ms = tracing::field::Empty,
            ),
            level = "info"
        )
    )]
    pub fn execute(mut self) -> FalkorResult<QueryResult<Vec<Constraint>>> {
        let result = self
            .common_execute_steps()
            .and_then(|res| self.parse_query_result_of_type(res))?;
        #[cfg(feature = "tracing")]
        crate::observability::record_result(
            result.data.len(),
            result.get_internal_execution_time(),
        );
        Ok(result)
    }
}

#[cfg(feature = "tokio")]
impl<'a> ProcedureQueryBuilder<'a, QueryResult<Vec<Constraint>>, AsyncGraph> {
    /// Executes the procedure call and returns a [`QueryResult`] type containing a result set of [`Constraint`]s
    /// This function consumes self.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Execute Constraint Procedure Call",
            skip_all,
            fields(
                db.response.returned_rows = tracing::field::Empty,
                db.falkordb.server_time_ms = tracing::field::Empty,
            ),
            level = "info"
        )
    )]
    pub async fn execute(mut self) -> FalkorResult<QueryResult<Vec<Constraint>>> {
        let (res, sent) = self.common_execute_steps().await?;
        let result = self
            .graph
            .decode_reply(redis_value_as_vec(res)?, &sent, parse_procedure_result)
            .await?;
        #[cfg(feature = "tracing")]
        crate::observability::record_result(
            result.data.len(),
            result.get_internal_execution_time(),
        );
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::blocking::create_empty_inner_sync_client;

    #[test]
    fn resolve_use_replica_defaults_to_primary() {
        // A read-only command with no per-request override and a Primary client default stays on
        // the primary.
        assert!(!resolve_use_replica(true, None, ReadPreference::Primary, "query").unwrap());
    }

    #[test]
    fn resolve_use_replica_inherits_client_prefer_replica() {
        // A read-only command inherits a PreferReplica client default.
        assert!(resolve_use_replica(true, None, ReadPreference::PreferReplica, "query").unwrap());
    }

    #[test]
    fn resolve_use_replica_request_override_wins() {
        // A per-request PreferReplica overrides a Primary client default…
        assert!(resolve_use_replica(
            true,
            Some(ReadPreference::PreferReplica),
            ReadPreference::Primary,
            "query"
        )
        .unwrap());
        // …and a per-request Primary overrides a PreferReplica client default.
        assert!(!resolve_use_replica(
            true,
            Some(ReadPreference::Primary),
            ReadPreference::PreferReplica,
            "query"
        )
        .unwrap());
    }

    #[test]
    fn resolve_use_replica_write_never_uses_replica_by_default() {
        // A writable command never routes to a replica, even under a PreferReplica client default,
        // and does not error as long as no replica was explicitly requested.
        assert!(!resolve_use_replica(false, None, ReadPreference::PreferReplica, "query").unwrap());
    }

    #[test]
    fn resolve_use_replica_explicit_replica_on_write_errors() {
        // Explicitly requesting a replica for a writable command is rejected.
        let err = resolve_use_replica(
            false,
            Some(ReadPreference::PreferReplica),
            ReadPreference::Primary,
            "query",
        )
        .unwrap_err();
        assert!(matches!(
            err,
            FalkorDBError::ReadPreferenceNotReadOnly { context: "query" }
        ));
    }

    #[test]
    fn resolve_use_replica_explicit_primary_on_write_is_ok() {
        // Explicitly forcing the primary on a write is fine (it is a no-op).
        assert!(!resolve_use_replica(
            false,
            Some(ReadPreference::Primary),
            ReadPreference::PreferReplica,
            "batch"
        )
        .unwrap());
    }

    /// A header `redis::Value` describing a single column named `name`.
    fn header_with_one_column(name: &str) -> redis::Value {
        redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::Int(1),
            redis::Value::BulkString(name.as_bytes().to_vec()),
        ])])
    }

    #[test]
    fn dispatch_one_element_is_stats_only() {
        let mut schema = GraphSchema::new("test_dispatch_one", create_empty_inner_sync_client());
        let mut result = dispatch_query_response(
            vec![redis::Value::Array(vec![])],
            &mut schema,
            build_lazy_query_result,
        )
        .unwrap();
        assert!(result.header.is_empty());
        assert!(result.data.next().is_none());
    }

    #[test]
    fn dispatch_two_elements_is_header_without_rows() {
        let mut schema = GraphSchema::new("test_dispatch_two", create_empty_inner_sync_client());
        let res = vec![header_with_one_column("col"), redis::Value::Array(vec![])];
        let mut result =
            dispatch_query_response(res, &mut schema, build_lazy_query_result).unwrap();
        assert_eq!(result.header.len(), 1);
        assert_eq!(result.header[0], "col");
        assert!(result.data.next().is_none());
    }

    #[test]
    fn dispatch_three_elements_has_header_and_rows() {
        let mut schema = GraphSchema::new("test_dispatch_three", create_empty_inner_sync_client());
        let res = vec![
            header_with_one_column("col"),
            redis::Value::Array(vec![]), // zero data rows
            redis::Value::Array(vec![]), // stats
        ];
        let mut result =
            dispatch_query_response(res, &mut schema, build_lazy_query_result).unwrap();
        assert_eq!(result.header.len(), 1);
        assert_eq!(result.header[0], "col");
        assert!(result.data.next().is_none());
    }

    #[test]
    fn dispatch_invalid_length_is_error() {
        let mut schema = GraphSchema::new("test_dispatch_bad", create_empty_inner_sync_client());
        assert!(dispatch_query_response(
            vec![redis::Value::Nil; 4],
            &mut schema,
            build_lazy_query_result
        )
        .is_err());
    }

    fn bulk(text: &str) -> redis::Value {
        redis::Value::BulkString(text.as_bytes().to_vec())
    }

    /// One row holding a compact node: a label id and one string property.
    fn node_row(
        label: i64,
        key: i64,
        value: &str,
    ) -> redis::Value {
        redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::Int(8),
            redis::Value::Array(vec![
                redis::Value::Int(1),
                redis::Value::Array(vec![redis::Value::Int(label)]),
                redis::Value::Array(vec![redis::Value::Array(vec![
                    redis::Value::Int(key),
                    redis::Value::Int(2),
                    bulk(value),
                ])]),
            ]),
        ])])
    }

    /// A RESP2 schema element: the hash, and a label and a property key when given.
    fn element(
        hash: i64,
        names: Option<(&str, &str)>,
    ) -> redis::Value {
        let mut pairs = vec![bulk("hash"), redis::Value::Int(hash)];
        if let Some((label, key)) = names {
            pairs.extend([
                bulk("labels"),
                redis::Value::Array(vec![bulk(label)]),
                bulk("relationshipTypes"),
                redis::Value::Array(vec![]),
                bulk("propertyKeys"),
                redis::Value::Array(vec![bulk(key)]),
            ]);
        }
        redis::Value::Array(pairs)
    }

    /// Decodes a `[header, rows, stats, ...]` reply through the sync path and returns its one node.
    fn decode_node(
        schema: &mut GraphSchema,
        mut res: Vec<redis::Value>,
    ) -> FalkorResult<crate::Node> {
        let handle = schema.for_reply(&mut res)?;
        let mut result = dispatch_query_response(res, handle, build_lazy_query_result)?;
        let row = result.data.next().expect("one row")?;
        match row.into_values().remove(0) {
            crate::FalkorValue::Node(node) => Ok(node),
            other => panic!("expected a node, got {other:?}"),
        }
    }

    #[test]
    fn replies_decode_with_the_names_their_schema_element_calls_for() {
        // The client provider is empty, so any refresh would fail: every decode below uses names
        // that came with a reply.
        let mut schema = GraphSchema::new("test_reply_names", create_empty_inner_sync_client());
        let reply = |value: &str, element: redis::Value| {
            vec![
                header_with_one_column("n"),
                redis::Value::Array(vec![node_row(0, 0, value)]),
                redis::Value::Array(vec![]),
                element,
            ]
        };

        let node = decode_node(
            &mut schema,
            reply("Alice", element(1, Some(("Person", "name")))),
        )
        .expect("names in the reply");
        assert_eq!(node.labels, vec!["Person"]);
        assert!(node.properties.contains_key("name"));

        // The graph was recreated: ids 0 now mean other names.
        let node = decode_node(
            &mut schema,
            reply("Heat", element(2, Some(("Movie", "title")))),
        )
        .expect("new names in the reply");
        assert_eq!(node.labels, vec!["Movie"]);
        assert!(node.properties.contains_key("title"));

        // The hash is current: the cached names decode it.
        let node = decode_node(&mut schema, reply("Dune", element(2, None))).expect("cached names");
        assert_eq!(node.labels, vec!["Movie"]);

        // A write without RETURN: `[stats, element]`.
        let mut res = vec![
            redis::Value::Array(vec![bulk("Nodes created: 1")]),
            element(2, None),
        ];
        let handle = schema.for_reply(&mut res).expect("element");
        let result =
            dispatch_query_response(res, handle, build_lazy_query_result).expect("stats only");
        assert!(result.header.is_empty());
        assert_eq!(result.stats.len(), 1);

        // A reply without an element (an older server) decodes with the cache as before.
        let node = decode_node(
            &mut schema,
            vec![
                header_with_one_column("n"),
                redis::Value::Array(vec![node_row(0, 0, "Old")]),
                redis::Value::Array(vec![]),
            ],
        )
        .expect("cached names");
        assert_eq!(node.labels, vec!["Movie"]);
    }

    #[test]
    fn an_id_missing_from_the_reply_names_is_an_error_not_a_refresh() {
        let mut schema = GraphSchema::new("test_reply_missing", create_empty_inner_sync_client());
        let err = decode_node(
            &mut schema,
            vec![
                header_with_one_column("n"),
                redis::Value::Array(vec![node_row(1, 0, "x")]),
                redis::Value::Array(vec![]),
                element(1, Some(("Only", "k"))),
            ],
        )
        .unwrap_err();
        // A refresh would have tried the (empty) client and failed with a connection error.
        assert!(matches!(
            err,
            FalkorDBError::MissingSchemaId(crate::SchemaType::Labels)
        ));
    }

    #[test]
    fn schema_hash_goes_only_on_queries_when_enabled() {
        let hash = || -1_146_456_808_972_410_223_i64;
        for command in ["GRAPH.QUERY", "GRAPH.RO_QUERY"] {
            // The negative form the server sent comes back byte for byte.
            assert_eq!(
                schema_hash_arg(true, command, hash).as_deref(),
                Some("-1146456808972410223")
            );
            assert_eq!(schema_hash_arg(false, command, hash), None);
        }
        for command in ["GRAPH.EXPLAIN", "GRAPH.PROFILE"] {
            assert_eq!(schema_hash_arg(true, command, hash), None);
        }
    }

    #[test]
    fn query_args_end_with_the_schema_hash_and_fit_the_argument_limit() {
        assert_eq!(query_args("q", None, None), ["q", "--compact"]);
        let longest = query_args("q", Some("1000"), Some("-5"));
        assert_eq!(
            longest,
            ["q", "--compact", "timeout", "1000", "SCHEMA_HASH", "-5"]
        );
        // With the command and the graph name, within the server's limit of 8.
        assert!(2 + longest.len() <= 8);
    }

    /// The schema element at the end of a reply that `builder` gets from the server.
    fn raw_schema_element(
        mut builder: QueryBuilder<'_, QueryResult<LazyResultSet<'_>>, &str, SyncGraph>
    ) -> Option<crate::graph_schema::SchemaElement> {
        let raw = builder.common_execute_steps().expect("query");
        let mut res = unwrap_query_response(raw).expect("reply");
        crate::graph_schema::take_schema_element(&mut res).expect("element parses")
    }

    #[test]
    fn a_current_hash_gets_the_hash_back_without_names() {
        let mut handle = crate::test_utils::open_empty_test_graph("schema_hash_reply_shapes");
        let graph = &mut handle.inner;
        let _ = graph.delete();
        let supported = crate::test_utils::server_has_schema_hash();

        // Nothing cached yet: the reply brings the names, including the ones the write added.
        let element = raw_schema_element(graph.query("CREATE (:A {k: 1}) RETURN 1"));
        if !supported {
            // An older server ignores SCHEMA_HASH and sends no element.
            assert_eq!(element, None);
            return;
        }
        let element = element.expect("an element");
        let names = element.names.expect("names");
        assert_eq!(names.labels, vec!["A"]);
        assert_eq!(names.property_keys, vec!["k"]);

        // Decoding a reply takes its names into the cache.
        graph
            .query("MATCH (n) RETURN n")
            .execute()
            .expect("query")
            .data
            .for_each(|row| assert!(row.is_ok()));
        assert_eq!(graph.get_graph_schema_mut().request_hash(), element.hash);

        // Now the hash sent is current, so only the hash comes back.
        assert_eq!(
            raw_schema_element(graph.query("MATCH (n) RETURN n")),
            Some(crate::graph_schema::SchemaElement {
                hash: element.hash,
                names: None
            })
        );
        let _ = graph.delete();
    }

    #[test]
    fn parse_procedure_result_rejects_wrong_element_count() {
        use crate::FalkorIndex;
        let mut schema = GraphSchema::new("test_proc_bad_len", create_empty_inner_sync_client());
        // Two elements instead of the required three.
        let res = vec![redis::Value::Array(vec![]), redis::Value::Array(vec![])];
        let result = parse_procedure_result::<FalkorIndex>(res, &mut schema);
        assert!(matches!(
            result,
            Err(FalkorDBError::ParsingArrayToStructElementCount(_))
        ));
    }

    #[test]
    fn parse_procedure_result_propagates_row_parse_error() {
        use crate::FalkorIndex;
        let mut schema = GraphSchema::new("test_proc_bad_row", create_empty_inner_sync_client());
        // A valid 3-element reply shape, but the single index row is malformed (not the 9-element
        // array `FalkorIndex` expects), so its parse error must propagate instead of being dropped.
        let res = vec![
            header_with_one_column("idx"),
            redis::Value::Array(vec![redis::Value::Int(5)]),
            redis::Value::Array(vec![]),
        ];
        let result = parse_procedure_result::<FalkorIndex>(res, &mut schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_procedure_call_no_args_no_yields() {
        let (query, params) = generate_procedure_call("my_procedure", None, None).unwrap();
        assert_eq!(query, "CALL my_procedure()");
        assert!(params.is_empty());
    }

    #[test]
    fn test_generate_procedure_call_with_args_no_yields() {
        let args = ["arg1", "arg2"];
        let (query, params) = generate_procedure_call("my_procedure", Some(&args), None).unwrap();
        assert_eq!(query, "CALL my_procedure($param0,$param1)");
        // String args are encoded as safe, quoted Cypher string literals.
        let mut preamble = String::new();
        params.encode_preamble(&mut preamble).unwrap();
        assert_eq!(preamble, "CYPHER param0='arg1' param1='arg2' ");
    }

    #[test]
    fn test_generate_procedure_call_no_args_with_yields() {
        let yields = ["yield1", "yield2"];
        let (query, params) = generate_procedure_call("my_procedure", None, Some(&yields)).unwrap();
        assert_eq!(query, "CALL my_procedure() YIELD yield1,yield2");
        assert!(params.is_empty());
    }

    #[test]
    fn test_generate_procedure_call_with_args_and_yields() {
        let args = ["arg1", "arg2"];
        let yields = ["yield1", "yield2"];
        let (query, params) =
            generate_procedure_call("my_procedure", Some(&args), Some(&yields)).unwrap();
        assert_eq!(
            query,
            "CALL my_procedure($param0,$param1) YIELD yield1,yield2"
        );
        let mut preamble = String::new();
        params.encode_preamble(&mut preamble).unwrap();
        assert_eq!(preamble, "CYPHER param0='arg1' param1='arg2' ");
    }

    #[test]
    fn test_construct_query_without_params() {
        let result = construct_query("MATCH (n) RETURN n", &FalkorParams::new()).unwrap();
        assert_eq!(result, "MATCH (n) RETURN n");
    }

    #[test]
    fn test_construct_query_single_param() {
        let mut params = FalkorParams::new();
        params.add_param("name", "Alice");
        let result = construct_query("MATCH (n) RETURN n", &params).unwrap();
        assert_eq!(result, "CYPHER name='Alice' MATCH (n) RETURN n");
    }

    #[test]
    fn test_construct_query_multiple_params() {
        let mut params = FalkorParams::new();
        params.add_param("name", "Alice");
        params.add_param("age", 30i64);
        let result = construct_query("MATCH (n) RETURN n", &params).unwrap();
        assert!(result.starts_with("CYPHER "));
        assert!(result.contains("name='Alice'"));
        assert!(result.contains("age=30"));
        assert!(result.ends_with("MATCH (n) RETURN n"));
    }

    #[test]
    fn test_construct_query_param_error_propagates() {
        let mut params = FalkorParams::new();
        params.add_param("bad name", 1i64);
        assert!(construct_query("RETURN 1", &params).is_err());
    }

    #[test]
    fn test_generate_procedure_call_arg_injection_is_escaped() {
        let args = ["'; MATCH (n) DELETE n //"];
        let (_query, params) = generate_procedure_call("p", Some(&args), None).unwrap();
        let mut preamble = String::new();
        params.encode_preamble(&mut preamble).unwrap();
        assert_eq!(preamble, "CYPHER param0='\\'; MATCH (n) DELETE n //' ");
    }

    #[test]
    fn test_generate_procedure_call_nul_arg_errors() {
        let args = ["a\0b"];
        let (_query, params) = generate_procedure_call("p", Some(&args), None).unwrap();
        assert!(params.encode_preamble(&mut String::new()).is_err());
    }

    #[test]
    fn test_construct_query_procedure_end_to_end() {
        let args = ["Label"];
        let (query, params) =
            generate_procedure_call("db.idx", Some(&args), Some(&["node"])).unwrap();
        let full = construct_query(query, &params).unwrap();
        assert_eq!(
            full,
            "CYPHER param0='Label' CALL db.idx($param0) YIELD node"
        );
    }

    #[test]
    fn test_generate_procedure_call_accepts_dotted_names() {
        let (query, _params) = generate_procedure_call(
            "db.idx.fulltext.queryNodes",
            Some(&["Person", "Alice"]),
            Some(&["node", "score"]),
        )
        .unwrap();
        assert_eq!(
            query,
            "CALL db.idx.fulltext.queryNodes($param0,$param1) YIELD node,score"
        );
        let (query, _params) =
            generate_procedure_call("DB.INDEXES", None, Some(&["_label2"])).unwrap();
        assert_eq!(query, "CALL DB.INDEXES() YIELD _label2");
    }

    #[test]
    fn test_generate_procedure_call_rejects_names_that_are_not_identifiers() {
        // The first name of each list is the issue's payload, which would add clauses to the call.
        for procedure in [
            "db.labels() YIELD label DETACH DELETE n //",
            "",
            "db..labels",
            ".db",
            "db.",
            "db labels",
            "`db.labels`",
            "1db",
        ] {
            assert_eq!(
                generate_procedure_call(procedure, None, None).unwrap_err(),
                FalkorDBError::InvalidIdentifier {
                    kind: "procedure",
                    name: procedure.to_string(),
                    message: "must be Cypher identifiers ([A-Za-z_][A-Za-z0-9_]*) joined by dots",
                }
            );
        }
        for column in ["node, (n) DETACH DELETE n", "label AS l", "", "`node`"] {
            assert_eq!(
                generate_procedure_call("db.labels", None, Some(&["label", column])).unwrap_err(),
                FalkorDBError::InvalidIdentifier {
                    kind: "yield column",
                    name: column.to_string(),
                    message: "must be a Cypher identifier ([A-Za-z_][A-Za-z0-9_]*)",
                }
            );
        }
        assert_eq!(
            generate_procedure_call("db.labels", None, Some(&["label AS l"]))
                .unwrap_err()
                .to_string(),
            "invalid yield column name 'label AS l': must be a Cypher identifier ([A-Za-z_][A-Za-z0-9_]*)"
        );
    }
}
