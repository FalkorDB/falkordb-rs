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

/// Replace parameter placeholders in a Cypher query, respecting quoted regions.
///
/// This tokenizer-based parser respects:
/// - Single-quoted strings: `'$param'` (not replaced)
/// - Backtick-quoted identifiers: `` `$param` `` (not replaced)
/// - Escaped quotes: `''` and `` `` `` (doubled quotes)
/// - Backslash escapes: `\'` in strings
///
/// # Arguments
/// * `query` - The Cypher query string
/// * `replacer` - Function that takes a parameter name and returns its replacement string
///
/// # Returns
/// The query with placeholders replaced
fn replace_cypher_parameters<F>(
    query: &str,
    mut replacer: F,
) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    let mut result = String::with_capacity(query.len());
    let chars: Vec<char> = query.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];

        // Handle single-quoted strings
        if ch == '\'' {
            result.push(ch);
            i += 1;
            // Skip everything until closing quote, handling escaped quotes
            while i < chars.len() {
                let inner = chars[i];
                result.push(inner);
                if inner == '\'' {
                    // Check for doubled single quote (escape in Cypher)
                    if i + 1 < chars.len() && chars[i + 1] == '\'' {
                        result.push(chars[i + 1]);
                        i += 2;
                    } else {
                        i += 1;
                        break; // End of string
                    }
                } else if inner == '\\' && i + 1 < chars.len() {
                    // Handle backslash escape
                    result.push(chars[i + 1]);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            continue;
        }

        // Handle backtick-quoted identifiers
        if ch == '`' {
            result.push(ch);
            i += 1;
            // Skip everything until closing backtick, handling doubled backticks
            while i < chars.len() {
                let inner = chars[i];
                result.push(inner);
                if inner == '`' {
                    // Check for doubled backtick (escape)
                    if i + 1 < chars.len() && chars[i + 1] == '`' {
                        result.push(chars[i + 1]);
                        i += 2;
                    } else {
                        i += 1;
                        break; // End of identifier
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }

        // Check for parameter placeholder outside quoted regions
        if ch == '$' {
            // Extract parameter name (alphanumeric + underscore)
            let start = i + 1;
            let mut end = start;
            while end < chars.len() && (chars[end].is_alphanumeric() || chars[end] == '_') {
                end += 1;
            }

            if end > start {
                let param_name: String = chars[start..end].iter().collect();
                if let Some(replacement) = replacer(&param_name) {
                    result.push_str(&replacement);
                    i = end;
                    continue;
                }
            }

            // No match, keep the $ character
            result.push(ch);
            i += 1;
        } else {
            result.push(ch);
            i += 1;
        }
    }

    result
}

/// Construct query with JSON parameters (for complex data structures like UNWIND batches)
/// This replaces parameter placeholders like $batch with Cypher literal syntax.
/// Placeholders inside single-quoted strings or backtick-quoted identifiers are NOT replaced.
#[cfg_attr(
    feature = "tracing",
    tracing::instrument(name = "Construct Query with JSON Params", skip_all, level = "trace")
)]
pub(crate) fn construct_query_with_json_params<Q: Display>(
    query_str: Q,
    json_params: &HashMap<String, serde_json::Value>,
) -> String {
    let query = query_str.to_string();

    // Sort params by key length descending to handle substring collisions
    let mut sorted_keys: Vec<_> = json_params.keys().collect();
    sorted_keys.sort_by(|k1, k2| k2.len().cmp(&k1.len()));

    replace_cypher_parameters(&query, |param_name| {
        // Try to match longest key first (already sorted)
        for key in &sorted_keys {
            if param_name.starts_with(key.as_str()) {
                // Ensure exact match with word boundary
                if param_name.len() == key.len() {
                    return Some(json_value_to_cypher_literal(&json_params[*key]));
                }
            }
        }
        None
    })
}

/// Parameter types for Cypher queries
#[derive(Debug, Clone)]
pub enum QueryParams<'a> {
    /// Simple string parameters formatted as "CYPHER key=value"
    Simple(&'a HashMap<String, String>),
    /// JSON parameters converted to Cypher literal syntax (for complex data structures)
    Json(&'a HashMap<String, serde_json::Value>),
}

/// A Builder-pattern struct that allows creating and executing queries on a graph
pub struct QueryBuilder<'a, Output, T: Display, G: HasGraphSchema> {
    _unused: PhantomData<Output>,
    graph: &'a mut G,
    command: &'a str,
    query_string: T,
    params: Option<QueryParams<'a>>,
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
            timeout: None,
        }
    }

    /// Pass parameters to the query
    ///
    /// Accepts either:
    /// - Simple string parameters: formatted as "CYPHER {param_key}={param_val}"
    /// - JSON parameters: converted to Cypher literal syntax for complex data structures
    ///
    /// # Arguments
    /// * `params`: [`QueryParams`] enum containing either simple or JSON parameters
    ///
    /// # Examples
    /// ```ignore
    /// // Simple string parameters
    /// let mut params = HashMap::new();
    /// params.insert("name".to_string(), "Alice".to_string());
    /// graph.query("MATCH (n {name: $name}) RETURN n")
    ///     .with_params(QueryParams::Simple(&params))
    ///     .execute()?;
    ///
    /// // JSON parameters for complex data
    /// let mut json_params = HashMap::new();
    /// json_params.insert("batch".to_string(), serde_json::json!([
    ///     {"id": 1, "name": "Alice"},
    ///     {"id": 2, "name": "Bob"}
    /// ]));
    /// graph.query("UNWIND $batch AS row CREATE (n:Node) SET n = row")
    ///     .with_params(QueryParams::Json(&json_params))
    ///     .execute()?;
    /// ```
    pub fn with_params(
        self,
        params: QueryParams<'a>,
    ) -> Self {
        Self {
            params: Some(params),
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
        let query = match &self.params {
            Some(QueryParams::Json(json_params)) => {
                construct_query_with_json_params(&self.query_string, json_params)
            }
            Some(QueryParams::Simple(params)) => {
                construct_query(&self.query_string, Some(params))
            }
            None => construct_query(&self.query_string, None::<&HashMap<&str, &str>>),
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
        let query = match &self.params {
            Some(QueryParams::Json(json_params)) => {
                construct_query_with_json_params(&self.query_string, json_params)
            }
            Some(QueryParams::Simple(params)) => {
                construct_query(&self.query_string, Some(params))
            }
            None => construct_query(&self.query_string, None::<&HashMap<&str, &str>>),
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

        let result = construct_query_with_json_params(query_str, &params);
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

        let result = construct_query_with_json_params(query_str, &params);
        assert!(result.contains("UNWIND [{`id`: 1, `name`: 'Alice'}, {`id`: 2, `name`: 'Bob'}]"));
        assert!(result.contains("AS row CREATE (n) SET n = row"));
    }

    #[test]
    fn test_construct_query_with_json_params_no_params() {
        let query_str = "MATCH (n) RETURN n";
        let params = HashMap::new();
        let result = construct_query_with_json_params(query_str, &params);
        assert_eq!(result, "MATCH (n) RETURN n");
    }

    #[test]
    fn test_construct_query_with_json_params_substring_collision() {
        let query_str = "MATCH (n) WHERE n.id = $id AND n.id_type = $id_type";
        let mut params = HashMap::new();
        params.insert("id".to_string(), serde_json::json!(42));
        params.insert("id_type".to_string(), serde_json::json!("user"));

        let result = construct_query_with_json_params(query_str, &params);
        // Should not incorrectly replace $id within $id_type
        assert!(result.contains("n.id = 42"));
        assert!(result.contains("n.id_type = 'user'"));
        assert!(!result.contains("n.42_type"));
    }

    #[test]
    fn test_construct_query_with_json_params_quoted_string() {
        let query_str = "RETURN '$id' AS s, $id AS x";
        let mut params = HashMap::new();
        params.insert("id".to_string(), serde_json::json!(42));

        let result = construct_query_with_json_params(query_str, &params);
        // Only unquoted $id should be replaced
        assert!(result.contains("'$id' AS s"));
        assert!(result.contains("42 AS x"));
    }

    #[test]
    fn test_construct_query_with_json_params_backtick_identifier() {
        let query_str = "MATCH (n) WHERE n.`$id` = $id";
        let mut params = HashMap::new();
        params.insert("id".to_string(), serde_json::json!(42));

        let result = construct_query_with_json_params(query_str, &params);
        // Placeholder in backtick identifier should not be replaced
        assert!(result.contains("n.`$id` = 42"));
    }

    #[test]
    fn test_construct_query_with_json_params_escaped_quotes() {
        // Test doubled single quotes (Cypher escape)
        let query_str = "RETURN '$id''s value' AS s, $id AS x";
        let mut params = HashMap::new();
        params.insert("id".to_string(), serde_json::json!(42));

        let result = construct_query_with_json_params(query_str, &params);
        assert!(result.contains("'$id''s value' AS s"));
        assert!(result.contains("42 AS x"));
    }

    #[test]
    fn test_construct_query_with_json_params_backslash_escape() {
        // Test backslash escapes in strings
        let query_str = "RETURN '$id\\'s value' AS s, $id AS x";
        let mut params = HashMap::new();
        params.insert("id".to_string(), serde_json::json!(42));

        let result = construct_query_with_json_params(query_str, &params);
        assert!(result.contains("'$id\\'s value' AS s"));
        assert!(result.contains("42 AS x"));
    }

    #[test]
    fn test_construct_query_with_json_params_doubled_backticks() {
        // Test doubled backticks (escape in identifiers)
        let query_str = "MATCH (n) WHERE n.`$id``name` = $id";
        let mut params = HashMap::new();
        params.insert("id".to_string(), serde_json::json!(42));

        let result = construct_query_with_json_params(query_str, &params);
        assert!(result.contains("n.`$id``name` = 42"));
    }

    #[test]
    fn test_construct_query_with_json_params_multiple_quotes() {
        // Test multiple quoted regions
        let query_str = "MATCH (n) WHERE n.name = '$name' AND n.id = $id AND n.type = '$type'";
        let mut params = HashMap::new();
        params.insert("name".to_string(), serde_json::json!("Alice"));
        params.insert("id".to_string(), serde_json::json!(42));
        params.insert("type".to_string(), serde_json::json!("user"));

        let result = construct_query_with_json_params(query_str, &params);
        assert!(result.contains("n.name = '$name'"));
        assert!(result.contains("n.id = 42"));
        assert!(result.contains("n.type = '$type'"));
    }

    #[test]
    fn test_construct_query_with_json_params_no_word_boundary() {
        // Test that partial matches don't replace (word boundary check)
        let query_str = "MATCH (n) WHERE n.$id_extra = $id";
        let mut params = HashMap::new();
        params.insert("id".to_string(), serde_json::json!(42));

        let result = construct_query_with_json_params(query_str, &params);
        // $id_extra should not be replaced because underscore is not a word boundary
        assert!(result.contains("n.$id_extra = 42"));
    }

    #[test]
    fn test_construct_query_with_json_params_end_of_string() {
        // Test placeholder at end of string
        let query_str = "RETURN $id";
        let mut params = HashMap::new();
        params.insert("id".to_string(), serde_json::json!(42));

        let result = construct_query_with_json_params(query_str, &params);
        assert_eq!(result, "RETURN 42");
    }

    #[test]
    fn test_construct_query_with_json_params_unclosed_quote() {
        // Test query with unclosed quote (edge case - should still work)
        let query_str = "RETURN '$id";
        let mut params = HashMap::new();
        params.insert("id".to_string(), serde_json::json!(42));

        let result = construct_query_with_json_params(query_str, &params);
        // Should preserve the unclosed quote and not replace inside it
        assert_eq!(result, "RETURN '$id");
    }

    #[test]
    fn test_replace_cypher_parameters_basic() {
        let query = "MATCH (n) WHERE n.id = $id RETURN n";
        let result = replace_cypher_parameters(query, |param| {
            if param == "id" {
                Some("42".to_string())
            } else {
                None
            }
        });
        assert_eq!(result, "MATCH (n) WHERE n.id = 42 RETURN n");
    }

    #[test]
    fn test_replace_cypher_parameters_in_quotes() {
        let query = "RETURN '$id' AS literal, $id AS value";
        let result = replace_cypher_parameters(query, |param| {
            if param == "id" {
                Some("42".to_string())
            } else {
                None
            }
        });
        assert_eq!(result, "RETURN '$id' AS literal, 42 AS value");
    }

    #[test]
    fn test_replace_cypher_parameters_in_backticks() {
        let query = "MATCH (n) WHERE n.`$id` = $id";
        let result = replace_cypher_parameters(query, |param| {
            if param == "id" {
                Some("42".to_string())
            } else {
                None
            }
        });
        assert_eq!(result, "MATCH (n) WHERE n.`$id` = 42");
    }

    #[test]
    fn test_replace_cypher_parameters_escaped_quotes() {
        let query = "RETURN '$id''s' AS s, $id AS x";
        let result = replace_cypher_parameters(query, |param| {
            if param == "id" {
                Some("42".to_string())
            } else {
                None
            }
        });
        assert_eq!(result, "RETURN '$id''s' AS s, 42 AS x");
    }

    #[test]
    fn test_replace_cypher_parameters_doubled_backticks() {
        let query = "MATCH (n) WHERE n.`$id``x` = $id";
        let result = replace_cypher_parameters(query, |param| {
            if param == "id" {
                Some("42".to_string())
            } else {
                None
            }
        });
        assert_eq!(result, "MATCH (n) WHERE n.`$id``x` = 42");
    }

    #[test]
    fn test_replace_cypher_parameters_backslash_escape() {
        let query = r"RETURN '$id\'s' AS s, $id AS x";
        let result = replace_cypher_parameters(query, |param| {
            if param == "id" {
                Some("42".to_string())
            } else {
                None
            }
        });
        assert_eq!(result, r"RETURN '$id\'s' AS s, 42 AS x");
    }

    #[test]
    fn test_replace_cypher_parameters_no_match() {
        let query = "RETURN $unknown";
        let result = replace_cypher_parameters(query, |_| None);
        assert_eq!(result, "RETURN $unknown");
    }

    #[test]
    fn test_replace_cypher_parameters_multiple_params() {
        let query = "MATCH (n) WHERE n.id = $id AND n.name = $name";
        let result = replace_cypher_parameters(query, |param| match param {
            "id" => Some("42".to_string()),
            "name" => Some("'Alice'".to_string()),
            _ => None,
        });
        assert_eq!(result, "MATCH (n) WHERE n.id = 42 AND n.name = 'Alice'");
    }

    #[test]
    fn test_replace_cypher_parameters_param_with_underscore() {
        let query = "RETURN $user_id";
        let result = replace_cypher_parameters(query, |param| {
            if param == "user_id" {
                Some("123".to_string())
            } else {
                None
            }
        });
        assert_eq!(result, "RETURN 123");
    }

    #[test]
    fn test_replace_cypher_parameters_dollar_alone() {
        let query = "RETURN $ + 1";
        let result = replace_cypher_parameters(query, |_| Some("42".to_string()));
        assert_eq!(result, "RETURN $ + 1");
    }

    #[test]
    fn test_replace_cypher_parameters_at_end() {
        let query = "RETURN $id";
        let result = replace_cypher_parameters(query, |param| {
            if param == "id" {
                Some("42".to_string())
            } else {
                None
            }
        });
        assert_eq!(result, "RETURN 42");
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

    #[test]
    fn test_query_params_enum_simple() {
        // Test that QueryParams::Simple works with construct_query
        let query_str = "MATCH (n) RETURN n";
        let mut params = HashMap::new();
        params.insert("name".to_string(), "Alice".to_string());
        params.insert("age".to_string(), "30".to_string());

        let query_params = QueryParams::Simple(&params);

        let result = match query_params {
            QueryParams::Simple(p) => construct_query(query_str, Some(p)),
            QueryParams::Json(_) => panic!("Expected Simple params"),
        };

        assert!(result.starts_with("CYPHER "));
        assert!(result.contains(" name=Alice "));
        assert!(result.contains(" age=30 "));
        assert!(result.ends_with(" RETURN n"));
    }

    #[test]
    fn test_query_params_enum_json() {
        // Test that QueryParams::Json works with construct_query_with_json_params
        let query_str = "MATCH (n) WHERE n.id = $id RETURN n";
        let mut params = HashMap::new();
        params.insert("id".to_string(), serde_json::json!(42));

        let query_params = QueryParams::Json(&params);

        let result = match query_params {
            QueryParams::Json(p) => construct_query_with_json_params(query_str, p),
            QueryParams::Simple(_) => panic!("Expected Json params"),
        };

        assert_eq!(result, "MATCH (n) WHERE n.id = 42 RETURN n");
    }
}
