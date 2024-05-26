/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::{collections::HashMap, fmt::Display, ops::Not};

pub(crate) fn generate_procedure_call<P: ToString, T: Display, Z: Display>(
    procedure: P,
    args: Option<&[T]>,
    yields: Option<&[Z]>,
) -> (String, Option<HashMap<String, String>>) {
    let params = args.map(|args| {
        args.iter()
            .enumerate()
            .fold(HashMap::new(), |mut acc, (idx, param)| {
                acc.insert(format!("param{idx}"), param.to_string());
                acc
            })
    });

    let mut query_string = format!(
        "CALL {}({})",
        procedure.to_string(),
        args.unwrap_or_default()
            .iter()
            .map(|element| format!("${}", element))
            .collect::<Vec<_>>()
            .join(",")
    );

    let yields = yields.unwrap_or_default();
    if !yields.is_empty() {
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

pub(crate) fn construct_query<Q: ToString, T: ToString, Z: ToString>(
    query_str: Q,
    params: Option<&HashMap<T, Z>>,
) -> String {
    format!(
        "{}{}",
        params
            .and_then(|params| params.is_empty().not().then(|| params
                .iter()
                .fold("CYPHER ".to_string(), |acc, (key, val)| {
                    format!("{}{}={} ", acc, key.to_string(), val.to_string())
                })))
            .unwrap_or_default(),
        query_str.to_string()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_procedure_call() {
        let (query, params) = generate_procedure_call(
            "DB.CONSTRAINTS",
            Some(&["Hello".to_string(), "World".to_string()]),
            Some(&["Foo".to_string(), "Bar".to_string()]),
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
