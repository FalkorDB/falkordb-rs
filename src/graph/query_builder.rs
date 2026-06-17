/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    graph::HasGraphSchema,
    parser::{redis_value_as_vec, SchemaParsable},
    Constraint, ExecutionPlan, FalkorDBError, FalkorIndex, FalkorParams, FalkorResult,
    IntoFalkorParam, IntoFalkorParams, LazyResultSet, QueryResult, SyncGraph,
};
use std::{
    fmt::{Display, Write},
    marker::PhantomData,
};

#[cfg(feature = "tokio")]
use crate::AsyncGraph;

#[cfg(feature = "serde")]
use crate::TypedLazyResultSet;

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
pub struct QueryBuilder<'a, Output, T: Display, G: HasGraphSchema> {
    _unused: PhantomData<Output>,
    graph: &'a mut G,
    command: &'a str,
    query_string: T,
    params: FalkorParams,
    timeout: Option<i64>,
}

impl<'a, Output, T: Display, G: HasGraphSchema> QueryBuilder<'a, Output, T, G> {
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

    fn generate_query_result_set(
        self,
        value: redis::Value,
    ) -> FalkorResult<QueryResult<LazyResultSet<'a>>> {
        if let redis::Value::ServerError(e) = value {
            return Err(FalkorDBError::RedisError(
                e.details().unwrap_or("Unknown error").to_string(),
            ));
        }

        let res = redis_value_as_vec(value)?;

        match res.len() {
            1 => {
                let stats = res.into_iter().next().ok_or(
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "One element exist but using next() failed",
                    ),
                )?;

                QueryResult::from_response(
                    None,
                    LazyResultSet::new(Default::default(), self.graph.get_graph_schema_mut()),
                    stats,
                )
            }
            2 => {
                let [header, stats]: [redis::Value; 2] = res.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "Two elements exist but couldn't be parsed to an array",
                    )
                })?;

                QueryResult::from_response(
                    Some(header),
                    LazyResultSet::new(Default::default(), self.graph.get_graph_schema_mut()),
                    stats,
                )
            }
            3 => {
                let [header, data, stats]: [redis::Value; 3] = res.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "3 elements exist but couldn't be parsed to an array",
                    )
                })?;

                QueryResult::from_response(
                    Some(header),
                    LazyResultSet::new(
                        redis_value_as_vec(data)?,
                        self.graph.get_graph_schema_mut(),
                    ),
                    stats,
                )
            }
            _ => Err(FalkorDBError::ParsingArrayToStructElementCount(
                "Invalid number of elements returned from query",
            ))?,
        }
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

        let timeout = self.timeout.map(|timeout| format!("timeout {timeout}"));
        let mut params = vec![query.as_str(), "--compact"];
        params.extend(timeout.as_deref());

        let client = self.graph.get_client();
        let conn = if self.command == "GRAPH.RO_QUERY" {
            client.borrow_readonly_connection(client.clone())
        } else {
            client.borrow_connection(client.clone())
        };

        conn.and_then(|mut conn| {
            conn.execute_command(
                Some(self.graph.graph_name()),
                self.command,
                None,
                Some(params.as_slice()),
            )
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

        let timeout = self.timeout.map(|timeout| format!("timeout {timeout}"));
        let mut params = vec![query.as_str(), "--compact"];
        params.extend(timeout.as_deref());

        let client = self.graph.get_client();
        let conn = if self.command == "GRAPH.RO_QUERY" {
            client.borrow_readonly_connection(client.clone()).await
        } else {
            client.borrow_connection(client.clone()).await
        };

        conn?
            .execute_command(
                Some(self.graph.graph_name()),
                self.command,
                None,
                Some(params.as_slice()),
            )
            .await
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
impl<'a, T: Display> QueryBuilder<'a, QueryResult<LazyResultSet<'a>>, T, AsyncGraph> {
    /// Executes the query, retuning a [`QueryResult`], with a [`LazyResultSet`] as its `data` member
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Execute Lazy Result Set Query", skip_all, level = "info")
    )]
    pub async fn execute(mut self) -> FalkorResult<QueryResult<LazyResultSet<'a>>> {
        self.common_execute_steps()
            .await
            .and_then(|res| self.generate_query_result_set(res))
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
impl<'a, T: Display, U> QueryBuilder<'a, QueryResult<TypedLazyResultSet<'a, U>>, T, AsyncGraph>
where
    U: serde::de::DeserializeOwned,
{
    /// Executes the query, returning a [`QueryResult`] whose `data` is a
    /// [`TypedLazyResultSet`] that deserializes each row into `U`.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Execute Typed Query", skip_all, level = "info")
    )]
    pub async fn execute(mut self) -> FalkorResult<QueryResult<TypedLazyResultSet<'a, U>>> {
        self.common_execute_steps()
            .await
            .and_then(|res| self.generate_query_result_set(res))
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
pub struct ProcedureQueryBuilder<'a, Output, G: HasGraphSchema> {
    _unused: PhantomData<Output>,
    graph: &'a mut G,
    readonly: bool,
    procedure_name: &'a str,
    args: Option<&'a [&'a str]>,
    yields: Option<&'a [&'a str]>,
}

impl<'a, Out, G: HasGraphSchema> ProcedureQueryBuilder<'a, Out, G> {
    pub(crate) fn new(
        graph: &'a mut G,
        procedure_name: &'a str,
    ) -> Self {
        Self {
            _unused: PhantomData,

            graph,
            readonly: false,
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
            procedure_name,
            args: None,
            yields: None,
        }
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

    fn parse_query_result_of_type<T: SchemaParsable>(
        &mut self,
        res: redis::Value,
    ) -> FalkorResult<QueryResult<Vec<T>>> {
        let [header, indices, stats]: [redis::Value; 3] =
            redis_value_as_vec(res).and_then(|res_vec| {
                res_vec.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "Expected exactly 3 elements in query response",
                    )
                })
            })?;

        QueryResult::from_response(
            Some(header),
            redis_value_as_vec(indices).map(|indices| {
                indices
                    .into_iter()
                    .flat_map(|res| T::parse(res, self.graph.get_graph_schema_mut()))
                    .collect()
            })?,
            stats,
        )
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
        let conn = if self.readonly {
            client.borrow_readonly_connection(client.clone())
        } else {
            client.borrow_connection(client.clone())
        };

        conn.and_then(|mut conn| {
            conn.execute_command(
                Some(self.graph.graph_name()),
                command,
                None,
                Some(&[query.as_str(), "--compact"]),
            )
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
        let conn = if self.readonly {
            client.borrow_readonly_connection(client.clone()).await
        } else {
            client.borrow_connection(client.clone()).await
        };

        conn?
            .execute_command(
                Some(self.graph.graph_name()),
                command,
                None,
                Some(&[query.as_str(), "--compact"]),
            )
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
        self.common_execute_steps()
            .await
            .and_then(|res| self.parse_query_result_of_type(res))
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
        self.common_execute_steps()
            .await
            .and_then(|res| self.parse_query_result_of_type(res))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
