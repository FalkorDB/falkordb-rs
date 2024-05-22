/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use std::collections::HashMap;

pub(crate) fn generate_procedure_call<P: ToString>(
    procedure: P,
    args: Option<&[String]>,
    yields: Option<&[String]>,
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
            "YIELD {}",
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
