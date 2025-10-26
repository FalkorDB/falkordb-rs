/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    graph::HasGraphSchema,
    parser::{redis_value_as_vec, SchemaParsable},
    Constraint, ExecutionPlan, FalkorDBError, FalkorIndex, FalkorResult, LazyResultSet,
    QueryResult, SyncGraph,
};
use std::{collections::HashMap, fmt::Display, marker::PhantomData, ops::Not};

#[cfg(feature = "tokio")]
use crate::AsyncGraph;

#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Construct Query", skip_all, level = "trace")
)]
pub(crate) fn construct_query<Q: Display, T: Display, Z: Display>(
    query_str: Q,
    params: Option<&HashMap<T, Z>>,
) -> String {
    let params_str = params
        .map(|p| {
            p.iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .and_then(|params_str| {
            params_str
                .is_empty()
                .not()
                .then_some(format!("CYPHER {params_str} "))
        })
        .unwrap_or_default();
    format!("{params_str}{query_str}")
}

/// Convert serde_json::Value to Cypher literal syntax
/// Cypher uses unquoted keys in maps: {key: value} not {"key": "value"}
fn json_value_to_cypher_literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => {
            // Escape single quotes and backslashes, wrap in single quotes
            format!("'{}'", s.replace("\\", "\\\\").replace("'", "\\'"))
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_value_to_cypher_literal).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(map) => {
            let items: Vec<String> = map
                .iter()
                .map(|(k, v)| {
                    // Escape backticks in keys by doubling them, then wrap in backticks
                    let escaped_key = k.replace("`", "``");
                    format!("`{}`: {}", escaped_key, json_value_to_cypher_literal(v))
                })
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

/// Construct query with JSON parameters (for complex data structures like UNWIND batches)
/// This replaces parameter placeholders like $batch with Cypher literal syntax
#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Construct Query with JSON Params", skip_all, level = "trace")
)]
pub(crate) fn construct_query_with_json_params<Q: Display>(
    query_str: Q,
    json_params: Option<&HashMap<String, serde_json::Value>>,
) -> String {
    let mut query = query_str.to_string();

    if let Some(params) = json_params {
        // Sort params by key length descending to handle substring collisions
        // e.g., replace $id_type before $id to avoid $id replacing part of $id_type
        let mut sorted_params: Vec<_> = params.iter().collect();
        sorted_params.sort_by(|(k1, _), (k2, _)| k2.len().cmp(&k1.len()));

        for (key, value) in sorted_params {
            // Use regex with word boundary to match exact parameter names
            // The pattern matches $ followed by the exact key and a word boundary
            let pattern = format!(r"\${}\b", regex::escape(key));
            let regex = regex::Regex::new(&pattern)
                .expect("Failed to create regex for parameter replacement");
            let cypher_literal = json_value_to_cypher_literal(value);
            query = regex
                .replace_all(&query, cypher_literal.as_str())
                .to_string();
        }
    }

    query
}

/// A Builder-pattern struct that allows creating and executing queries on a graph
pub struct QueryBuilder<'a, Output, T: Display, G: HasGraphSchema> {
    _unused: PhantomData<Output>,
    graph: &'a mut G,
    command: &'a str,
    query_string: T,
    params: Option<&'a HashMap<String, String>>,
    json_params: Option<&'a HashMap<String, serde_json::Value>>,
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
            params: None,
            json_params: None,
            timeout: None,
        }
    }

    /// Pass the following params to the query as "CYPHER {param_key}={param_val}"
    ///
    /// # Arguments
    /// * `params`: A [`HashMap`] of params in key-val format
    pub fn with_params(
        self,
        params: &'a HashMap<String, String>,
    ) -> Self {
        Self {
            params: Some(params),
            ..self
        }
    }

    /// Pass JSON parameters to the query for complex data structures (e.g., UNWIND batches)
    ///
    /// This method allows passing complex parameters like arrays of maps which are common
    /// in UNWIND operations. The parameters are converted to Cypher literal syntax.
    ///
    /// # Arguments
    /// * `json_params`: A [`HashMap`] of parameter names to JSON values
    ///
    /// # Example
    /// ```ignore
    /// let mut batch_data = Vec::new();
    /// // ... populate batch_data ...
    /// let mut params = HashMap::new();
    /// params.insert("batch".to_string(), serde_json::Value::Array(batch_data));
    ///
    /// graph.query("UNWIND $batch AS row CREATE (n:Node) SET n = row")
    ///     .with_json_params(&params)
    ///     .execute()
    ///     .await?;
    /// ```
    pub fn with_json_params(
        self,
        json_params: &'a HashMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            json_params: Some(json_params),
            ..self
        }
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
        tracing::instrument(name = "Common Query Execution Steps", skip_all, level = "trace")
    )]
    fn common_execute_steps(&mut self) -> FalkorResult<redis::Value> {
        // json_params takes precedence over params when both are provided
        debug_assert!(
            self.json_params.is_none() || self.params.is_none(),
            "Cannot use both json_params and params simultaneously - json_params will be used"
        );

        let query = if self.json_params.is_some() {
            construct_query_with_json_params(&self.query_string, self.json_params)
        } else {
            construct_query(&self.query_string, self.params)
        };

        let timeout = self.timeout.map(|timeout| format!("timeout {timeout}"));
        let mut params = vec![query.as_str(), "--compact"];
        params.extend(timeout.as_deref());

        self.graph
            .get_client()
            .borrow_connection(self.graph.get_client().clone())
            .and_then(|mut conn| {
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
        tracing::instrument(name = "Common Query Execution Steps", skip_all, level = "trace")
    )]
    async fn common_execute_steps(&mut self) -> FalkorResult<redis::Value> {
        // json_params takes precedence over params when both are provided
        debug_assert!(
            self.json_params.is_none() || self.params.is_none(),
            "Cannot use both json_params and params simultaneously - json_params will be used"
        );

        let query = if self.json_params.is_some() {
            construct_query_with_json_params(&self.query_string, self.json_params)
        } else {
            construct_query(&self.query_string, self.params)
        };

        let timeout = self.timeout.map(|timeout| format!("timeout {timeout}"));
        let mut params = vec![query.as_str(), "--compact"];
        params.extend(timeout.as_deref());

        self.graph
            .get_client()
            .borrow_connection(self.graph.get_client().clone())
            .await?
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
pub(crate) fn generate_procedure_call<P: Display, T: Display, Z: Display>(
    procedure: P,
    args: Option<&[T]>,
    yields: Option<&[Z]>,
) -> (String, Option<HashMap<String, String>>) {
    let args_str = args
        .unwrap_or_default()
        .iter()
        .enumerate()
        .map(|(idx, _)| format!("$param{idx}"))
        .collect::<Vec<_>>()
        .join(",");
    let mut query_string = format!("CALL {procedure}({args_str})");

    let params = args.map(|args| {
        args.iter()
            .enumerate()
            .fold(HashMap::new(), |mut acc, (idx, param)| {
                acc.insert(format!("param{idx}"), param.to_string());
                acc
            })
    });

    if let Some(yields) = yields {
        query_string += format!(
            " YIELD {}",
            yields
                .iter()
                .map(|element| element.to_string())
                .collect::<Vec<_>>()
                .join(",")
        )
        .as_str();
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
        let query = construct_query(query_string, params.as_ref());

        self.graph
            .get_client()
            .borrow_connection(self.graph.get_client().clone())
            .and_then(|mut conn| {
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
        let query = construct_query(query_string, params.as_ref());

        self.graph
            .get_client()
            .borrow_connection(self.graph.get_client().clone())
            .await?
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
        let procedure = "my_procedure";
        let args: Option<&[String]> = None;
        let yields: Option<&[String]> = None;

        let expected_query = "CALL my_procedure()".to_string();
        let expected_params: Option<HashMap<String, String>> = None;

        let result = generate_procedure_call(procedure, args, yields);

        assert_eq!(result, (expected_query, expected_params));
    }

    #[test]
    fn test_generate_procedure_call_with_args_no_yields() {
        let procedure = "my_procedure";
        let args = &["arg1".to_string(), "arg2".to_string()];
        let yields: Option<&[String]> = None;

        let expected_query = "CALL my_procedure($param0,$param1)".to_string();
        let mut expected_params = HashMap::new();
        expected_params.insert("param0".to_string(), "arg1".to_string());
        expected_params.insert("param1".to_string(), "arg2".to_string());

        let result = generate_procedure_call(procedure, Some(args), yields);

        assert_eq!(result, (expected_query, Some(expected_params)));
    }

    #[test]
    fn test_generate_procedure_call_no_args_with_yields() {
        let procedure = "my_procedure";
        let args: Option<&[String]> = None;
        let yields = &["yield1".to_string(), "yield2".to_string()];

        let expected_query = "CALL my_procedure() YIELD yield1,yield2".to_string();
        let expected_params: Option<HashMap<String, String>> = None;

        let result = generate_procedure_call(procedure, args, Some(yields));

        assert_eq!(result, (expected_query, expected_params));
    }

    #[test]
    fn test_generate_procedure_call_with_args_and_yields() {
        let procedure = "my_procedure";
        let args = &["arg1".to_string(), "arg2".to_string()];
        let yields = &["yield1".to_string(), "yield2".to_string()];

        let expected_query = "CALL my_procedure($param0,$param1) YIELD yield1,yield2".to_string();
        let mut expected_params = HashMap::new();
        expected_params.insert("param0".to_string(), "arg1".to_string());
        expected_params.insert("param1".to_string(), "arg2".to_string());

        let result = generate_procedure_call(procedure, Some(args), Some(yields));

        assert_eq!(result, (expected_query, Some(expected_params)));
    }

    #[test]
    fn test_construct_query_with_params() {
        let query_str = "MATCH (n) RETURN n";
        let mut params = HashMap::new();
        params.insert("name", "Alice");
        params.insert("age", "30");

        let result = construct_query(query_str, Some(&params));
        assert!(result.starts_with("CYPHER "));
        assert!(result.ends_with(" RETURN n"));
        assert!(result.contains(" name=Alice "));
        assert!(result.contains(" age=30 "));
    }

    #[test]
    fn test_construct_query_without_params() {
        let query_str = "MATCH (n) RETURN n";
        let result = construct_query::<&str, &str, &str>(query_str, None);
        assert_eq!(result, "MATCH (n) RETURN n");
    }

    #[test]
    fn test_construct_query_empty_params() {
        let query_str = "MATCH (n) RETURN n";
        let params: HashMap<&str, &str> = HashMap::new();
        let result = construct_query(query_str, Some(&params));
        assert_eq!(result, "MATCH (n) RETURN n");
    }

    #[test]
    fn test_construct_query_single_param() {
        let query_str = "MATCH (n) RETURN n";
        let mut params = HashMap::new();
        params.insert("name", "Alice");

        let result = construct_query(query_str, Some(&params));
        assert_eq!(result, "CYPHER name=Alice MATCH (n) RETURN n");
    }

    #[test]
    fn test_construct_query_multiple_params() {
        let query_str = "MATCH (n) RETURN n";
        let mut params = HashMap::new();
        params.insert("name", "Alice");
        params.insert("age", "30");
        params.insert("city", "Wonderland");

        let result = construct_query(query_str, Some(&params));
        assert!(result.starts_with("CYPHER "));
        assert!(result.contains(" name=Alice "));
        assert!(result.contains(" age=30 "));
        assert!(result.contains(" city=Wonderland "));
        assert!(result.ends_with("MATCH (n) RETURN n"));
    }

    #[test]
    fn test_json_value_to_cypher_literal_primitives() {
        assert_eq!(
            json_value_to_cypher_literal(&serde_json::json!(null)),
            "null"
        );
        assert_eq!(
            json_value_to_cypher_literal(&serde_json::json!(true)),
            "true"
        );
        assert_eq!(
            json_value_to_cypher_literal(&serde_json::json!(false)),
            "false"
        );
        assert_eq!(json_value_to_cypher_literal(&serde_json::json!(42)), "42");
        assert_eq!(
            json_value_to_cypher_literal(&serde_json::json!(3.14)),
            "3.14"
        );
        assert_eq!(
            json_value_to_cypher_literal(&serde_json::json!("hello")),
            "'hello'"
        );
    }

    #[test]
    fn test_json_value_to_cypher_literal_string_escaping() {
        assert_eq!(
            json_value_to_cypher_literal(&serde_json::json!("it's")),
            "'it\\'s'"
        );
        assert_eq!(
            json_value_to_cypher_literal(&serde_json::json!("back\\slash")),
            "'back\\\\slash'"
        );
    }

    #[test]
    fn test_json_value_to_cypher_literal_array() {
        let arr = serde_json::json!([1, 2, 3]);
        assert_eq!(json_value_to_cypher_literal(&arr), "[1, 2, 3]");

        let mixed = serde_json::json!(["a", 42, true]);
        assert_eq!(json_value_to_cypher_literal(&mixed), "['a', 42, true]");
    }

    #[test]
    fn test_json_value_to_cypher_literal_object() {
        let obj = serde_json::json!({"name": "Alice", "age": 30});
        let result = json_value_to_cypher_literal(&obj);

        // HashMap iteration order is not guaranteed, so check both components
        assert!(result.starts_with("{"));
        assert!(result.ends_with("}"));
        assert!(result.contains("`name`: 'Alice'"));
        assert!(result.contains("`age`: 30"));
    }

    #[test]
    fn test_json_value_to_cypher_literal_nested() {
        let nested = serde_json::json!({
            "id": 1,
            "props": {
                "name": "Alice",
                "tags": ["tag1", "tag2"]
            }
        });
        let result = json_value_to_cypher_literal(&nested);

        assert!(result.contains("`id`: 1"));
        assert!(result.contains("`name`: 'Alice'"));
        assert!(result.contains("`tags`: ['tag1', 'tag2']"));
    }

    #[test]
    fn test_construct_query_with_json_params_simple() {
        let query_str = "MATCH (n) WHERE n.id = $id RETURN n";
        let mut params = HashMap::new();
        params.insert("id".to_string(), serde_json::json!(42));

        let result = construct_query_with_json_params(query_str, Some(&params));
        assert_eq!(result, "MATCH (n) WHERE n.id = 42 RETURN n");
    }

    #[test]
    fn test_construct_query_with_json_params_array() {
        let query_str = "UNWIND $batch AS row CREATE (n) SET n = row";
        let batch_data = serde_json::json!([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]);
        let mut params = HashMap::new();
        params.insert("batch".to_string(), batch_data);

        let result = construct_query_with_json_params(query_str, Some(&params));
        assert!(result.contains("UNWIND [{`id`: 1, `name`: 'Alice'}, {`id`: 2, `name`: 'Bob'}]"));
        assert!(result.contains("AS row CREATE (n) SET n = row"));
    }

    #[test]
    fn test_construct_query_with_json_params_no_params() {
        let query_str = "MATCH (n) RETURN n";
        let result = construct_query_with_json_params(query_str, None);
        assert_eq!(result, "MATCH (n) RETURN n");
    }

    #[test]
    fn test_construct_query_with_json_params_substring_collision() {
        let query_str = "MATCH (n) WHERE n.id = $id AND n.id_type = $id_type";
        let mut params = HashMap::new();
        params.insert("id".to_string(), serde_json::json!(42));
        params.insert("id_type".to_string(), serde_json::json!("user"));

        let result = construct_query_with_json_params(query_str, Some(&params));
        // Should not incorrectly replace $id within $id_type
        assert!(result.contains("n.id = 42"));
        assert!(result.contains("n.id_type = 'user'"));
        assert!(!result.contains("n.42_type"));
    }

    #[test]
    fn test_json_value_to_cypher_literal_special_keys() {
        // Test keys with spaces
        let obj = serde_json::json!({"key with spaces": "value"});
        let result = json_value_to_cypher_literal(&obj);
        assert!(result.contains("`key with spaces`: 'value'"));

        // Test keys with hyphens
        let obj = serde_json::json!({"key-with-hyphens": "value"});
        let result = json_value_to_cypher_literal(&obj);
        assert!(result.contains("`key-with-hyphens`: 'value'"));

        // Test keys with colons
        let obj = serde_json::json!({"key:with:colons": "value"});
        let result = json_value_to_cypher_literal(&obj);
        assert!(result.contains("`key:with:colons`: 'value'"));

        // Test keys that are Cypher keywords
        let obj = serde_json::json!({"match": "value"});
        let result = json_value_to_cypher_literal(&obj);
        assert!(result.contains("`match`: 'value'"));

        // Test keys starting with digits
        let obj = serde_json::json!({"1st": "value"});
        let result = json_value_to_cypher_literal(&obj);
        assert!(result.contains("`1st`: 'value'"));

        // Test keys with backticks (should be escaped)
        let obj = serde_json::json!({"key`with`backticks": "value"});
        let result = json_value_to_cypher_literal(&obj);
        assert!(result.contains("`key``with``backticks`: 'value'"));
    }
}
