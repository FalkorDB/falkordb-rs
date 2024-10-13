/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    parser::{redis_value_as_float, redis_value_as_vec},
    FalkorDBError::ParsingVec32,
    FalkorResult,
};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Vec32 {
    /// The values of the vector
    pub values: Vec<f32>,
}

impl Vec32 {
    /// Parses a Vec32 from a redis::Value::Array,
    /// # Arguments
    /// * `value`: The value to parse
    ///
    /// # Returns
    /// Self, if successful
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Vec32", skip_all, level = "trace")
    )]
    pub fn parse(value: redis::Value) -> FalkorResult<Vec32> {
        let values: Vec<redis::Value> = redis_value_as_vec(value).map_err(|_| ParsingVec32)?;

        let mut vec32 = Vec32 {
            values: Vec::with_capacity(values.len()),
        };

        for val in values {
            vec32.values.push(redis_value_as_float(val)?);
        }

        Ok(vec32)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::FalkorDBError;

    #[test]
    fn test_parse_valid_vec32() {
        let value = redis::Value::Array(vec![
            redis::Value::SimpleString("45.0".to_string()),
            redis::Value::SimpleString("90.0".to_string()),
        ]);
        let result = Vec32::parse(value);
        assert!(result.is_ok());
        let vec = result.unwrap().values;
        assert_eq!(vec.len(), 2);
        assert_eq!(vec[0], 45.0);
        assert_eq!(vec[1], 90.0);
    }

    #[test]
    fn test_parse_invalid_vec32_not_an_array() {
        let value = redis::Value::SimpleString("not an array".to_string());
        let result = Vec32::parse(value);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), FalkorDBError::ParsingVec32);
    }

    #[test]
    fn test_parse_invalid_vec32_invalid_elements() {
        let value = redis::Value::Array(vec![
            redis::Value::SimpleString("45.0".to_string()),
            redis::Value::SimpleString("not a number".to_string()),
        ]);
        let result = Vec32::parse(value);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), FalkorDBError::ParsingF32);
    }

    #[test]
    fn test_parse_empty_vec32() {
        let value = redis::Value::Array(vec![]);
        let result = Vec32::parse(value);
        assert!(result.is_ok());
        assert!(result.unwrap().values.is_empty());
    }
}
