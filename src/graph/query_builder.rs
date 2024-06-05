/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection,
    parser::utils::{parse_header, parse_result_set},
    Constraint, ExecutionPlan, FalkorDBError, FalkorIndex, FalkorParsable, FalkorResponse,
    FalkorResult, FalkorValue, ResultSet, SyncGraph,
};
use std::{collections::HashMap, fmt::Display, marker::PhantomData};

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
        .map(|params_str| format!("CYPHER {params_str} "))
        .unwrap_or_default();
    format!("{params_str}{query_str}")
}

/// A Builder-pattern struct that allows creating and executing queries on a graph
pub struct QueryBuilder<'a, Output> {
    _unused: PhantomData<Output>,
    graph: &'a mut SyncGraph,
    command: &'a str,
    query_string: &'a str,
    params: Option<&'a HashMap<String, String>>,
    timeout: Option<i64>,
}

impl<'a, Output> QueryBuilder<'a, Output> {
    pub(crate) fn new(
        graph: &'a mut SyncGraph,
        command: &'a str,
        query_string: &'a str,
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

    /// Pass the following params to the query (as "CYPHER {param_key}={param_val}"
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

    fn common_execute_steps(
        &mut self,
        conn: &mut BorrowedSyncConnection,
    ) -> FalkorResult<FalkorValue> {
        let query = construct_query(self.query_string, self.params);

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

impl<'a> QueryBuilder<'a, FalkorResponse<ResultSet>> {
    /// Executes the query, retuning a [`FalkorResponse`], with a [`ResultSet`] as its `data` member
    pub fn execute(mut self) -> FalkorResult<FalkorResponse<ResultSet>> {
        let mut conn = self
            .graph
            .client
            .borrow_connection(self.graph.client.clone())?;
        let res = self.common_execute_steps(&mut conn)?.into_vec()?;

        match res.len() {
            1 => {
                let stats = res.into_iter().next().ok_or(
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "One element exist but using next() failed".to_string(),
                    ),
                )?;

                FalkorResponse::from_response(None, vec![], stats)
            }
            2 => {
                let [header, stats]: [FalkorValue; 2] = res.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "Two elements exist but couldn't be parsed to an array".to_string(),
                    )
                })?;

                FalkorResponse::from_response(Some(header), vec![], stats)
            }
            3 => {
                let [header, data, stats]: [FalkorValue; 3] = res.try_into().map_err(|_| {
                    FalkorDBError::ParsingArrayToStructElementCount(
                        "3 elements exist but couldn't be parsed to an array".to_string(),
                    )
                })?;

                FalkorResponse::from_response_with_headers(
                    parse_result_set(data, &mut self.graph.graph_schema)?,
                    parse_header(header)?,
                    stats,
                )
            }
            _ => Err(FalkorDBError::ParsingArrayToStructElementCount(
                "Invalid number of elements returned from query".to_string(),
            ))?,
        }
    }
}

impl<'a> QueryBuilder<'a, ExecutionPlan> {
    /// Executes the query, returning an [`ExecutionPlan`] from the data returned
    pub fn execute(mut self) -> FalkorResult<ExecutionPlan> {
        let mut conn = self
            .graph
            .client
            .borrow_connection(self.graph.client.clone())?;
        let res = self.common_execute_steps(&mut conn)?;

        ExecutionPlan::try_from(res)
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
    ) -> FalkorResult<FalkorValue> {
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
        let mut conn = self
            .graph
            .client
            .borrow_connection(self.graph.client.clone())?;

        let [header, indices, stats]: [FalkorValue; 3] = self
            .common_execute_steps(&mut conn)?
            .into_vec()?
            .try_into()
            .map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 3 elements in query response".to_string(),
                )
            })?;

        FalkorResponse::from_response(
            Some(header),
            indices
                .into_vec()?
                .into_iter()
                .flat_map(|index| {
                    FalkorIndex::from_falkor_value(index, &mut self.graph.graph_schema)
                })
                .collect(),
            stats,
        )
    }
}

impl<'a> ProcedureQueryBuilder<'a, FalkorResponse<Vec<Constraint>>> {
    /// Executes the procedure call and return a [`FalkorResponse`] type containing a result set of [`Constraint`]s
    /// This functions consumes self
    pub fn execute(mut self) -> FalkorResult<FalkorResponse<Vec<Constraint>>> {
        let mut conn = self
            .graph
            .client
            .borrow_connection(self.graph.client.clone())?;

        let [header, query_res, stats]: [FalkorValue; 3] = self
            .common_execute_steps(&mut conn)?
            .into_vec()?
            .try_into()
            .map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 3 elements in query response".to_string(),
                )
            })?;

        FalkorResponse::from_response(
            Some(header),
            query_res
                .into_vec()?
                .into_iter()
                .flat_map(|item| Constraint::from_falkor_value(item, &mut self.graph.graph_schema))
                .collect(),
            stats,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_procedure_call() {
        let (query, params) = generate_procedure_call(
            "DB.CONSTRAINTS",
            Some(&["Hello", "World"]),
            Some(&["Foo", "Bar"]),
        );

        assert_eq!(query, "CALL DB.CONSTRAINTS($Hello,$World) YIELD Foo,Bar");
        assert!(params.is_some());

        let params = params.unwrap();
        assert_eq!(params["param0"], "Hello");
        assert_eq!(params["param1"], "World");
    }

    #[test]
    fn test_construct_query() {
        let query = construct_query("MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 100",
                                    Some(&HashMap::from([("Foo", "Bar"), ("Bizz", "Bazz")])));
        assert!(query.starts_with("CYPHER "));
        assert!(query.ends_with(" MATCH (a:actor) WITH a MATCH (b:actor) WHERE a.age = b.age AND a <> b RETURN a, collect(b) LIMIT 100"));

        // Order not guaranteed
        assert!(query.contains(" Foo=Bar "));
        assert!(query.contains(" Bizz=Bazz "));
    }
}
