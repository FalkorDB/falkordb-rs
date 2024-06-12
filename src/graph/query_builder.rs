/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection, Constraint, ExecutionPlan, FalkorDBError,
    FalkorIndex, FalkorResponse, FalkorResult, LazyResultSet, SyncGraph,
};
use std::{collections::HashMap, fmt::Display, marker::PhantomData, ops::Not};

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

/// A Builder-pattern struct that allows creating and executing queries on a graph
pub struct QueryBuilder<'a, Output, T: Display> {
    _unused: PhantomData<Output>,
    graph: &'a mut SyncGraph,
    command: &'a str,
    query_string: T,
    params: Option<&'a HashMap<String, String>>,
    timeout: Option<i64>,
}

impl<'a, Output, T: Display> QueryBuilder<'a, Output, T> {
    pub(crate) fn new(
        graph: &'a mut SyncGraph,
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

    /// Specify a timeout after which to abort the query
    ///
    /// # Arguments
    /// * `timeout`: the timeout after which to abort, in ms
    pub fn with_timeout(
        self,
        timeout: i64,
    ) -> Self {
        Self {
            timeout: Some(timeout),
            ..self
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Common Query Execution Steps", skip_all)
    )]
    fn common_execute_steps(&mut self) -> FalkorResult<redis::Value> {
        let mut conn = self
            .graph
            .client
            .borrow_connection(self.graph.client.clone())?;
        let query = construct_query(&self.query_string, self.params);

        let timeout = self.timeout.map(|timeout| format!("timeout {timeout}"));
        let mut params = vec![query.as_str(), "--compact"];
        params.extend(timeout.as_deref());

        conn.execute_command(
            Some(self.graph.graph_name()),
            self.command,
            None,
            Some(params.as_slice()),
        )
    }
}

impl<'a, T: Display> QueryBuilder<'a, FalkorResponse<LazyResultSet<'a>>, T> {
    /// Executes the query, retuning a [`FalkorResponse`], with a [`LazyResultSet`] as its `data` member
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Execute Lazy Result Set Query", skip_all)
    )]
    pub fn execute(mut self) -> FalkorResult<FalkorResponse<LazyResultSet<'a>>> {
        let res = self
            .common_execute_steps()?
            .into_sequence()
            .map_err(|_| FalkorDBError::ParsingArray)?;

        match res.len() {
            1 => {
                let stats = res.into_iter().next().ok_or(
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "One element exist but using next() failed",
                    ),
                )?;

                FalkorResponse::from_response(
                    None,
                    LazyResultSet::new(Default::default(), &mut self.graph.graph_schema),
                    stats,
                )
            }
            2 => {
                let [header, stats]: [redis::Value; 2] = res.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "Two elements exist but couldn't be parsed to an array",
                    )
                })?;

                FalkorResponse::from_response(
                    Some(header),
                    LazyResultSet::new(Default::default(), &mut self.graph.graph_schema),
                    stats,
                )
            }
            3 => {
                let [header, data, stats]: [redis::Value; 3] = res.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "3 elements exist but couldn't be parsed to an array",
                    )
                })?;

                FalkorResponse::from_response(
                    Some(header),
                    LazyResultSet::new(
                        data.into_sequence()
                            .map_err(|_| FalkorDBError::ParsingArray)?,
                        &mut self.graph.graph_schema,
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

impl<'a, T: Display> QueryBuilder<'a, ExecutionPlan, T> {
    /// Executes the query, returning an [`ExecutionPlan`] from the data returned
    pub fn execute(mut self) -> FalkorResult<ExecutionPlan> {
        let res = self.common_execute_steps()?;

        ExecutionPlan::parse(res)
    }
}

pub(crate) fn generate_procedure_call<P: Display, T: Display, Z: Display>(
    procedure: P,
    args: Option<&[T]>,
    yields: Option<&[Z]>,
) -> (String, Option<HashMap<String, String>>) {
    let args_str = args
        .unwrap_or_default()
        .iter()
        .map(|e| format!("${}", e))
        .collect::<Vec<_>>()
        .join(",");
    let mut query_string = format!("CALL {}({})", procedure, args_str);

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
pub struct ProcedureQueryBuilder<'a, Output> {
    _unused: PhantomData<Output>,
    graph: &'a mut SyncGraph,
    readonly: bool,
    procedure_name: &'a str,
    args: Option<&'a [&'a str]>,
    yields: Option<&'a [&'a str]>,
}

impl<'a, Output> ProcedureQueryBuilder<'a, Output> {
    pub(crate) fn new(
        graph: &'a mut SyncGraph,
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
        graph: &'a mut SyncGraph,
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

    fn common_execute_steps(
        &mut self,
        conn: &mut BorrowedSyncConnection,
    ) -> FalkorResult<redis::Value> {
        let command = match self.readonly {
            true => "GRAPH.QUERY_RO",
            false => "GRAPH.QUERY",
        };

        let (query_string, params) =
            generate_procedure_call(self.procedure_name, self.args, self.yields);
        let query = construct_query(query_string, params.as_ref());

        conn.execute_command(
            Some(self.graph.graph_name()),
            command,
            None,
            Some(&[query.as_str(), "--compact"]),
        )
    }
}

impl<'a> ProcedureQueryBuilder<'a, FalkorResponse<Vec<FalkorIndex>>> {
    /// Executes the procedure call and return a [`FalkorResponse`] type containing a result set of [`FalkorIndex`]s
    /// This functions consumes self
    pub fn execute(mut self) -> FalkorResult<FalkorResponse<Vec<FalkorIndex>>> {
        self.common_execute_steps(
            &mut self
                .graph
                .client
                .borrow_connection(self.graph.client.clone())?,
        )
        .and_then(|res| res.into_sequence().map_err(|_| FalkorDBError::ParsingArray))
        .and_then(|res_vec| {
            let [header, indices, stats]: [redis::Value; 3] = res_vec.try_into().map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 3 elements in query response",
                )
            })?;

            FalkorResponse::from_response(
                Some(header),
                indices
                    .into_sequence()
                    .map_err(|_| FalkorDBError::ParsingArray)?
                    .into_iter()
                    .flat_map(|index| FalkorIndex::parse(index, &mut self.graph.graph_schema))
                    .collect(),
                stats,
            )
        })
    }
}

impl<'a> ProcedureQueryBuilder<'a, FalkorResponse<Vec<Constraint>>> {
    /// Executes the procedure call and return a [`FalkorResponse`] type containing a result set of [`Constraint`]s
    /// This functions consumes self
    pub fn execute(mut self) -> FalkorResult<FalkorResponse<Vec<Constraint>>> {
        self.common_execute_steps(
            &mut self
                .graph
                .client
                .borrow_connection(self.graph.client.clone())?,
        )
        .and_then(|res| res.into_sequence().map_err(|_| FalkorDBError::ParsingArray))
        .and_then(|res_vec| {
            let [header, indices, stats]: [redis::Value; 3] = res_vec.try_into().map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 3 elements in query response",
                )
            })?;

            FalkorResponse::from_response(
                Some(header),
                indices
                    .into_sequence()
                    .map(|indices| {
                        indices
                            .into_iter()
                            .flat_map(|constraint| {
                                Constraint::parse(constraint, &mut self.graph.graph_schema)
                            })
                            .collect()
                    })
                    .map_err(|_| FalkorDBError::ParsingArray)?,
                stats,
            )
        })
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

        let expected_query = "CALL my_procedure($arg1,$arg2)".to_string();
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

        let expected_query = "CALL my_procedure($arg1,$arg2) YIELD yield1,yield2".to_string();
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
}
