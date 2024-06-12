/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::redis_ext::redis_value_as_string;
use crate::{FalkorDBError, FalkorResult};

pub(crate) fn string_vec_from_val(value: redis::Value) -> FalkorResult<Vec<String>> {
    value
        .into_sequence()
        .map(|as_vec| as_vec.into_iter().flat_map(redis_value_as_string).collect())
        .map_err(|_| FalkorDBError::ParsingArray)
}

pub(crate) fn parse_header(header: redis::Value) -> FalkorResult<Vec<String>> {
    header.into_sequence().map_err(|_| FalkorDBError::ParsingArray)
        .and_then(|in_vec| {
            let in_vec_len = in_vec.len();
            in_vec
                .into_iter()
                .try_fold(Vec::with_capacity(in_vec_len), |mut acc, item| {
                    item.into_sequence().map_err(|_| FalkorDBError::ParsingArray)
                        .and_then(|item_vec| {
                            acc.push(
                                redis_value_as_string(if item_vec.len() == 2 {
                                    let [_, key]: [redis::Value; 2] =
                                        item_vec.try_into().map_err(|_| {
                                            FalkorDBError::ParsingHeader(
                                                "Could not get 2-sized array despite there being 2 elements"
                                            )
                                        })?;
                                    key
                                } else {
                                    item_vec.into_iter().next().ok_or(
                                        FalkorDBError::ParsingHeader(
                                            "Expected at least one item in header vector"
                                        ),
                                    )?
                                })?
                            );
                            Ok(acc)
                        })
                })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FalkorDBError;

    #[test]
    fn test_parse_header_valid_single_key() {
        let header = redis::Value::Bulk(vec![redis::Value::Bulk(vec![redis::Value::Data(
            "key1".as_bytes().to_vec(),
        )])]);
        let result = parse_header(header);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["key1".to_string()]);
    }

    #[test]
    fn test_parse_header_valid_multiple_keys() {
        let header = redis::Value::Bulk(vec![
            redis::Value::Bulk(vec![
                redis::Value::Data("type".as_bytes().to_vec()),
                redis::Value::Data("header1".as_bytes().to_vec()),
            ]),
            redis::Value::Bulk(vec![redis::Value::Data("key2".as_bytes().to_vec())]),
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
        let header = redis::Value::Bulk(vec![]);
        let result = parse_header(header);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::<String>::new());
    }

    #[test]
    fn test_parse_header_empty_vec() {
        let header = redis::Value::Bulk(vec![redis::Value::Bulk(vec![])]);
        let result = parse_header(header);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            FalkorDBError::ParsingHeader("Expected at least one item in header vector")
        );
    }

    #[test]
    fn test_parse_header_many_elements() {
        let header = redis::Value::Bulk(vec![redis::Value::Bulk(vec![
            redis::Value::Data("just_some_header".as_bytes().to_vec()),
            redis::Value::Data("header1".as_bytes().to_vec()),
            redis::Value::Data("extra".as_bytes().to_vec()),
        ])]);
        let result = parse_header(header);
        assert!(result.is_ok());
        assert_eq!(result.unwrap()[0], "just_some_header");
    }
}
