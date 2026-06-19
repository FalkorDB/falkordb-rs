/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    graph::HasGraphSchema,
    parser::{parse_header, redis_value_as_vec, SchemaParsable},
    retry::{op_kind_for_command, run_with_retry_blocking, OpKind},
    Constraint, ExecutionPlan, FalkorDBError, FalkorIndex, FalkorParams, FalkorResult, GraphSchema,
    IntoFalkorParam, IntoFalkorParams, LazyResultSet, QueryResult, SyncGraph,
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
    graph_schema: &'g mut GraphSchema,
) -> FalkorResult<QueryResult<LazyResultSet<'g>>> {
    let data = LazyResultSet::new(Arc::clone(&header), rows, graph_schema);
    QueryResult::from_response(header, data, stats)
}

/// Builds a [`QueryResult`] over an owned [`RowStream`] by eagerly parsing every row against
/// `graph_schema`, so the result is decoupled from later changes to the graph's schema.
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

/// Unwraps a top-level query reply: a `ServerError` becomes a [`FalkorDBError::RedisError`],
/// otherwise the reply is read as the response array.
pub(crate) fn unwrap_query_response(value: redis::Value) -> FalkorResult<Vec<redis::Value>> {
    if let redis::Value::ServerError(e) = value {
        return Err(FalkorDBError::RedisError(
            e.details().unwrap_or("Unknown error").to_string(),
        ));
    }
    redis_value_as_vec(value)
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

/// A Builder-pattern struct that allows creating and executing queries on a graph
pub struct QueryBuilder<'a, Output, T: Display, G> {
    _unused: PhantomData<Output>,
    graph: &'a mut G,
    command: &'a str,
    query_string: T,
    params: FalkorParams,
    timeout: Option<i64>,
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
}

impl<'a, Output, T: Display, G: HasGraphSchema> QueryBuilder<'a, Output, T, G> {
    fn generate_query_result_set(
        self,
        value: redis::Value,
    ) -> FalkorResult<QueryResult<LazyResultSet<'a>>> {
        dispatch_query_response(
            unwrap_query_response(value)?,
            self.graph.get_graph_schema_mut(),
            build_lazy_query_result,
        )
    }
}

impl<Out, T: Display> QueryBuilder<'_, Out, T, SyncGraph> {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Common Query Execution Steps",
            skip_all,
            fields(readonly = (self.command == "GRAPH.RO_QUERY")),
            level = "trace"
        )
    )]
    fn common_execute_steps(&mut self) -> FalkorResult<redis::Value> {
        let query = construct_query(&self.query_string, &self.params)?;

        let timeout = self.timeout.map(|timeout| timeout.to_string());
        let mut params = vec![query.as_str(), "--compact"];
        if let Some(timeout) = timeout.as_deref() {
            params.push("timeout");
            params.push(timeout);
        }

        let client = self.graph.get_client();
        let graph_name = self.graph.graph_name();
        let command = self.command;
        let readonly_conn = command == "GRAPH.RO_QUERY";
        let params_ref = params.as_slice();
        let policy = client.retry_policy();

        run_with_retry_blocking(&policy, op_kind_for_command(command), || {
            let conn = if readonly_conn {
                client.borrow_readonly_connection(client.clone())
            } else {
                client.borrow_connection(client.clone())
            };
            conn.and_then(|mut conn| {
                conn.execute_command(Some(graph_name), command, None, Some(params_ref))
            })
        })
    }
}

#[cfg(feature = "tokio")]
impl<'a, Out, T: Display> QueryBuilder<'a, Out, T, AsyncGraph> {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Common Query Execution Steps",
            skip_all,
            fields(readonly = (self.command == "GRAPH.RO_QUERY")),
            level = "trace"
        )
    )]
    async fn common_execute_steps(&mut self) -> FalkorResult<redis::Value> {
        let query = construct_query(&self.query_string, &self.params)?;

        let timeout = self.timeout.map(|timeout| timeout.to_string());
        let mut params = vec![query.as_str(), "--compact"];
        if let Some(timeout) = timeout.as_deref() {
            params.push("timeout");
            params.push(timeout);
        }

        let client = self.graph.get_client();
        let graph_name = self.graph.graph_name();
        let command = self.command;
        let readonly_conn = command == "GRAPH.RO_QUERY";
        let params_ref = params.as_slice();
        let policy = client.retry_policy();

        run_with_retry_async(&policy, op_kind_for_command(command), || async move {
            let conn = if readonly_conn {
                client.borrow_readonly_connection(client.clone()).await
            } else {
                client.borrow_connection(client.clone()).await
            };
            conn?
                .execute_command(Some(graph_name), command, None, Some(params_ref))
                .await
        })
        .await
    }

    /// Eagerly parses the reply into an owned [`RowStream`] under the schema write lock, so the
    /// result outlives the borrow of the graph, is `Send + 'static`, and is decoupled from any
    /// later change to the graph's schema.
    fn generate_async_result_set(
        self,
        value: redis::Value,
    ) -> FalkorResult<QueryResult<RowStream>> {
        let schema = self.graph.schema_handle();
        let mut guard = schema.write();
        dispatch_query_response(
            unwrap_query_response(value)?,
            &mut *guard,
            build_row_stream_result,
        )
    }
}

impl<'a, T: Display> QueryBuilder<'a, QueryResult<LazyResultSet<'a>>, T, SyncGraph> {
    /// Executes the query, retuning a [`QueryResult`], with a [`LazyResultSet`] as its `data` member
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Execute Lazy Result Set Query", skip_all, level = "info")
    )]
    pub fn execute(mut self) -> FalkorResult<QueryResult<LazyResultSet<'a>>> {
        self.common_execute_steps()
            .and_then(|res| self.generate_query_result_set(res))
    }
}

#[cfg(feature = "tokio")]
impl<'a, T: Display> QueryBuilder<'a, QueryResult<RowStream>, T, AsyncGraph> {
    /// Executes the query, returning a [`QueryResult`] whose `data` is an owned [`RowStream`] — a
    /// `Send + 'static` [`futures_core::Stream`] of `FalkorResult<Row>`.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Execute Row Stream Query", skip_all, level = "info")
    )]
    pub async fn execute(mut self) -> FalkorResult<QueryResult<RowStream>> {
        self.common_execute_steps()
            .await
            .and_then(|res| self.generate_async_result_set(res))
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
        tracing::instrument(name = "Execute Typed Query", skip_all, level = "info")
    )]
    pub fn execute(mut self) -> FalkorResult<QueryResult<TypedLazyResultSet<'a, U>>> {
        self.common_execute_steps()
            .and_then(|res| self.generate_query_result_set(res))
            .map(|result| result.into_typed::<U>())
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
        tracing::instrument(name = "Execute Typed Row Stream Query", skip_all, level = "info")
    )]
    pub async fn execute(mut self) -> FalkorResult<QueryResult<TypedRowStream<U>>> {
        self.common_execute_steps()
            .await
            .and_then(|res| self.generate_async_result_set(res))
            .map(|result| result.into_typed::<U>())
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
            .and_then(ExecutionPlan::parse)
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
) -> (String, FalkorParams) {
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

    (query_string, params)
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
}

/// Parses a 3-element `[header, rows, stats]` procedure reply into a `QueryResult<Vec<T>>`, using
/// `graph_schema` to resolve compact ids. Shared by the sync and async procedure builders.
fn parse_procedure_result<T: SchemaParsable>(
    res: redis::Value,
    graph_schema: &mut GraphSchema,
) -> FalkorResult<QueryResult<Vec<T>>> {
    let [header, indices, stats]: [redis::Value; 3] =
        redis_value_as_vec(res).and_then(|res_vec| {
            res_vec.try_into().map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 3 elements in query response",
                )
            })
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
        parse_procedure_result(res, self.graph.get_graph_schema_mut())
    }
}

impl<Out> ProcedureQueryBuilder<'_, Out, SyncGraph> {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Common Procedure Call Execution Steps",
            skip_all,
            fields(readonly = self.readonly),
            level = "trace"
        )
    )]
    fn common_execute_steps(&mut self) -> FalkorResult<redis::Value> {
        let command = match self.readonly {
            true => "GRAPH.RO_QUERY",
            false => "GRAPH.QUERY",
        };

        let (query_string, params) =
            generate_procedure_call(self.procedure_name, self.args, self.yields);
        let query = construct_query(query_string, &params)?;

        let client = self.graph.get_client();
        let graph_name = self.graph.graph_name();
        let readonly_conn = self.readonly;
        let op_kind = self.op_kind;
        let exec_params = [query.as_str(), "--compact"];
        let policy = client.retry_policy();

        run_with_retry_blocking(&policy, op_kind, || {
            let conn = if readonly_conn {
                client.borrow_readonly_connection(client.clone())
            } else {
                client.borrow_connection(client.clone())
            };
            conn.and_then(|mut conn| {
                conn.execute_command(Some(graph_name), command, None, Some(&exec_params))
            })
        })
    }
}

#[cfg(feature = "tokio")]
impl<'a, Out> ProcedureQueryBuilder<'a, Out, AsyncGraph> {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "Common Procedure Call Execution Steps",
            skip_all,
            fields(readonly = self.readonly),
            level = "trace"
        )
    )]
    async fn common_execute_steps(&mut self) -> FalkorResult<redis::Value> {
        let command = match self.readonly {
            true => "GRAPH.RO_QUERY",
            false => "GRAPH.QUERY",
        };

        let (query_string, params) =
            generate_procedure_call(self.procedure_name, self.args, self.yields);
        let query = construct_query(query_string, &params)?;

        let client = self.graph.get_client();
        let graph_name = self.graph.graph_name();
        let readonly_conn = self.readonly;
        let op_kind = self.op_kind;
        let exec_params = [query.as_str(), "--compact"];
        let policy = client.retry_policy();

        run_with_retry_async(&policy, op_kind, || async move {
            let conn = if readonly_conn {
                client.borrow_readonly_connection(client.clone()).await
            } else {
                client.borrow_connection(client.clone()).await
            };
            conn?
                .execute_command(Some(graph_name), command, None, Some(&exec_params))
                .await
        })
        .await
    }
}

impl ProcedureQueryBuilder<'_, QueryResult<Vec<FalkorIndex>>, SyncGraph> {
    /// Executes the procedure call and return a [`QueryResult`] type containing a result set of [`FalkorIndex`]s
    /// This functions consumes self
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Execute FalkorIndex Query", skip_all, level = "info")
    )]
    pub fn execute(mut self) -> FalkorResult<QueryResult<Vec<FalkorIndex>>> {
        self.common_execute_steps()
            .and_then(|res| self.parse_query_result_of_type(res))
    }
}

#[cfg(feature = "tokio")]
impl<'a> ProcedureQueryBuilder<'a, QueryResult<Vec<FalkorIndex>>, AsyncGraph> {
    /// Executes the procedure call and return a [`QueryResult`] type containing a result set of [`FalkorIndex`]s
    /// This functions consumes self
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Execute FalkorIndex Query", skip_all, level = "info")
    )]
    pub async fn execute(mut self) -> FalkorResult<QueryResult<Vec<FalkorIndex>>> {
        let res = self.common_execute_steps().await?;
        let schema = self.graph.schema_handle();
        let mut guard = schema.write();
        parse_procedure_result(res, &mut guard)
    }
}

impl ProcedureQueryBuilder<'_, QueryResult<Vec<Constraint>>, SyncGraph> {
    /// Executes the procedure call and return a [`QueryResult`] type containing a result set of [`Constraint`]s
    /// This functions consumes self
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Execute Constraint Procedure Call", skip_all, level = "info")
    )]
    pub fn execute(mut self) -> FalkorResult<QueryResult<Vec<Constraint>>> {
        self.common_execute_steps()
            .and_then(|res| self.parse_query_result_of_type(res))
    }
}

#[cfg(feature = "tokio")]
impl<'a> ProcedureQueryBuilder<'a, QueryResult<Vec<Constraint>>, AsyncGraph> {
    /// Executes the procedure call and return a [`QueryResult`] type containing a result set of [`Constraint`]s
    /// This functions consumes self
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Execute Constraint Procedure Call", skip_all, level = "info")
    )]
    pub async fn execute(mut self) -> FalkorResult<QueryResult<Vec<Constraint>>> {
        let res = self.common_execute_steps().await?;
        let schema = self.graph.schema_handle();
        let mut guard = schema.write();
        parse_procedure_result(res, &mut guard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::blocking::create_empty_inner_sync_client;

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

    #[test]
    fn parse_procedure_result_rejects_wrong_element_count() {
        use crate::FalkorIndex;
        let mut schema = GraphSchema::new("test_proc_bad_len", create_empty_inner_sync_client());
        // Two elements instead of the required three.
        let res = redis::Value::Array(vec![
            redis::Value::Array(vec![]),
            redis::Value::Array(vec![]),
        ]);
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
        let res = redis::Value::Array(vec![
            header_with_one_column("idx"),
            redis::Value::Array(vec![redis::Value::Int(5)]),
            redis::Value::Array(vec![]),
        ]);
        let result = parse_procedure_result::<FalkorIndex>(res, &mut schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_procedure_call_no_args_no_yields() {
        let (query, params) = generate_procedure_call("my_procedure", None, None);
        assert_eq!(query, "CALL my_procedure()");
        assert!(params.is_empty());
    }

    #[test]
    fn test_generate_procedure_call_with_args_no_yields() {
        let args = ["arg1", "arg2"];
        let (query, params) = generate_procedure_call("my_procedure", Some(&args), None);
        assert_eq!(query, "CALL my_procedure($param0,$param1)");
        // String args are encoded as safe, quoted Cypher string literals.
        let mut preamble = String::new();
        params.encode_preamble(&mut preamble).unwrap();
        assert_eq!(preamble, "CYPHER param0='arg1' param1='arg2' ");
    }

    #[test]
    fn test_generate_procedure_call_no_args_with_yields() {
        let yields = ["yield1", "yield2"];
        let (query, params) = generate_procedure_call("my_procedure", None, Some(&yields));
        assert_eq!(query, "CALL my_procedure() YIELD yield1,yield2");
        assert!(params.is_empty());
    }

    #[test]
    fn test_generate_procedure_call_with_args_and_yields() {
        let args = ["arg1", "arg2"];
        let yields = ["yield1", "yield2"];
        let (query, params) = generate_procedure_call("my_procedure", Some(&args), Some(&yields));
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
        let (_query, params) = generate_procedure_call("p", Some(&args), None);
        let mut preamble = String::new();
        params.encode_preamble(&mut preamble).unwrap();
        assert_eq!(preamble, "CYPHER param0='\\'; MATCH (n) DELETE n //' ");
    }

    #[test]
    fn test_generate_procedure_call_nul_arg_errors() {
        let args = ["a\0b"];
        let (_query, params) = generate_procedure_call("p", Some(&args), None);
        assert!(params.encode_preamble(&mut String::new()).is_err());
    }

    #[test]
    fn test_construct_query_procedure_end_to_end() {
        let args = ["Label"];
        let (query, params) = generate_procedure_call("db.idx", Some(&args), Some(&["node"]));
        let full = construct_query(query, &params).unwrap();
        assert_eq!(
            full,
            "CYPHER param0='Label' CALL db.idx($param0) YIELD node"
        );
    }
}
