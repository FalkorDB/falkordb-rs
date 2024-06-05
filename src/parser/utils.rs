/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    value::utils::parse_type, FalkorDBError, FalkorResult, FalkorValue, GraphSchema, ResultSet,
};

pub(crate) fn string_vec_from_val(value: FalkorValue) -> FalkorResult<Vec<String>> {
    value.into_vec().map(|value_as_vec| {
        value_as_vec
            .into_iter()
            .flat_map(FalkorValue::into_string)
            .collect()
    })
}

pub(crate) fn parse_header(header: FalkorValue) -> FalkorResult<Vec<String>> {
    let in_vec = header.into_vec()?;

    let mut out_vec = Vec::with_capacity(in_vec.len());
    for item in in_vec {
        let item_vec = item.into_vec()?;

        out_vec.push(
            if item_vec.len() == 2 {
                let [_, key]: [FalkorValue; 2] = item_vec.try_into().map_err(|_| {
                    FalkorDBError::ParsingHeader(
                        "Could not get 2-sized array despite there being 2 elements".to_string(),
                    )
                })?;
                key
            } else {
                item_vec
                    .into_iter()
                    .next()
                    .ok_or(FalkorDBError::ParsingHeader(
                        "Expected at least one item in header vector".to_string(),
                    ))?
            }
            .into_string()?,
        )
    }

    Ok(out_vec)
}

pub(crate) fn parse_result_set(
    data: FalkorValue,
    graph_schema: &mut GraphSchema,
) -> FalkorResult<ResultSet> {
    let data_vec = data.into_vec()?;

    let mut out_vec = Vec::with_capacity(data_vec.len());
    for column in data_vec {
        out_vec.push(parse_type(6, column, graph_schema)?.into_vec()?);
    }

    Ok(out_vec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FalkorDBError, FalkorValue};

    #[test]
    fn test_parse_header_valid_single_key() {
        let header = FalkorValue::Array(vec![FalkorValue::Array(vec![FalkorValue::String(
            "key1".to_string(),
        )])]);
        let result = parse_header(header);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["key1".to_string()]);
    }

    #[test]
    fn test_parse_header_valid_multiple_keys() {
        let header = FalkorValue::Array(vec![
            FalkorValue::Array(vec![
                FalkorValue::String("type".to_string()),
                FalkorValue::String("header1".to_string()),
            ]),
            FalkorValue::Array(vec![FalkorValue::String("key2".to_string())]),
        ]);
        let result = parse_header(header);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            vec!["header1".to_string(), "key2".to_string()]
        );
    }

    #[test]
    fn test_parse_header_empty_header() {
        let header = FalkorValue::Array(vec![]);
        let result = parse_header(header);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::<String>::new());
    }

    #[test]
    fn test_parse_header_empty_vec() {
        let header = FalkorValue::Array(vec![FalkorValue::Array(vec![])]);
        let result = parse_header(header);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            FalkorDBError::ParsingHeader("Expected at least one item in header vector".to_string())
        );
    }

    #[test]
    fn test_parse_header_many_elements() {
        let header = FalkorValue::Array(vec![FalkorValue::Array(vec![
            FalkorValue::String("just_some_header".to_string()),
            FalkorValue::String("header1".to_string()),
            FalkorValue::String("extra".to_string()),
        ])]);
        let result = parse_header(header);
        assert!(result.is_ok());
        assert_eq!(result.unwrap()[0], "just_some_header");
    }
}
