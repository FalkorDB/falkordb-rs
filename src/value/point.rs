/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    FalkorDBError, FalkorResult,
    parser::{redis_value_as_double, redis_value_as_vec},
};

/// A point in the world.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Point {
    /// The latitude coordinate
    pub latitude: f64,
    /// The longitude coordinate
    pub longitude: f64,
}

impl Point {
    /// Parses a point from a redis::Value::Array,
    /// taking the first element as an f64 latitude, and second element as an f64 longitude
    ///
    /// # Arguments
    /// * `value`: The value to parse
    ///
    /// # Returns
    /// Self, if successful
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Point", skip_all, level = "trace")
    )]
    pub fn parse(value: redis::Value) -> FalkorResult<Point> {
        let [lat, long]: [redis::Value; 2] = redis_value_as_vec(value).and_then(|val_vec| {
            val_vec.try_into().map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 2 element in point - latitude and longitude",
                )
            })
        })?;

        Ok(Point {
            latitude: redis_value_as_double(lat)?,
            longitude: redis_value_as_double(long)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_point() {
        let value = redis::Value::Array(vec![
            redis::Value::SimpleString("45.0".to_string()),
            redis::Value::SimpleString("90.0".to_string()),
        ]);
        let result = Point::parse(value);
        assert!(result.is_ok());
        let point = result.unwrap();
        assert_eq!(point.latitude, 45.0);
        assert_eq!(point.longitude, 90.0);
    }

    #[test]
    fn test_parse_invalid_point_missing_elements() {
        let value = redis::Value::Array(vec![redis::Value::SimpleString("45.0".to_string())]);
        let result = Point::parse(value);
        assert!(result.is_err());
        match result {
            Err(FalkorDBError::ParsingArrayToStructElementCount(msg)) => {
                assert_eq!(
                    msg,
                    "Expected exactly 2 element in point - latitude and longitude".to_string()
                );
            }
            _ => panic!("Expected ParsingArrayToStructElementCount error"),
        }
    }

    #[test]
    fn test_parse_invalid_point_extra_elements() {
        let value = redis::Value::Array(vec![
            redis::Value::SimpleString("45.0".to_string()),
            redis::Value::SimpleString("90.0".to_string()),
            redis::Value::SimpleString("30.0".to_string()),
        ]);
        let result = Point::parse(value);
        assert!(result.is_err());
        match result {
            Err(FalkorDBError::ParsingArrayToStructElementCount(msg)) => {
                assert_eq!(
                    msg,
                    "Expected exactly 2 element in point - latitude and longitude".to_string()
                );
            }
            _ => panic!("Expected ParsingArrayToStructElementCount error"),
        }
    }

    #[test]
    fn test_parse_invalid_point_not_an_array() {
        let value = redis::Value::SimpleString("not an array".to_string());
        let result = Point::parse(value);
        assert!(result.is_err());
        // Check for the specific error type if needed
    }
}
