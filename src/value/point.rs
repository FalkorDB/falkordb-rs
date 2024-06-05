/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorResult, FalkorValue};

/// A point in the world.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Point {
    /// The latitude coordinate
    pub latitude: f64,
    /// The longitude coordinate
    pub longitude: f64,
}

impl Point {
    /// Parses a point from a FalkorValue::Array,
    /// taking the first element as an f64 latitude, and second element as an f64 longitude
    ///
    /// # Arguments
    /// * `value`: The value to parse
    ///
    /// # Returns
    /// Self, if successful
    pub fn parse(value: FalkorValue) -> FalkorResult<Point> {
        let [lat, long]: [FalkorValue; 2] = value.into_vec()?.try_into().map_err(|_| {
            FalkorDBError::ParsingArrayToStructElementCount(
                "Expected exactly 2 element in point - latitude and longitude".to_string(),
            )
        })?;

        Ok(Point {
            latitude: lat.to_f64().ok_or(FalkorDBError::ParsingF64)?,
            longitude: long.to_f64().ok_or(FalkorDBError::ParsingF64)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_point() {
        let value = FalkorValue::Array(vec![FalkorValue::F64(45.0), FalkorValue::F64(90.0)]);
        let result = Point::parse(value);
        assert!(result.is_ok());
        let point = result.unwrap();
        assert_eq!(point.latitude, 45.0);
        assert_eq!(point.longitude, 90.0);
    }

    #[test]
    fn test_parse_invalid_point_missing_elements() {
        let value = FalkorValue::Array(vec![FalkorValue::F64(45.0)]);
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
        let value = FalkorValue::Array(vec![
            FalkorValue::F64(45.0),
            FalkorValue::F64(90.0),
            FalkorValue::F64(30.0),
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
    fn test_parse_invalid_point_non_f64_elements() {
        let value = FalkorValue::Array(vec![
            FalkorValue::String("45.0".to_string()),
            FalkorValue::String("90.0".to_string()),
        ]);
        let result = Point::parse(value);
        assert!(result.is_err());
        match result {
            Err(FalkorDBError::ParsingF64) => {}
            _ => panic!("Expected ParsingF64 error"),
        }
    }

    #[test]
    fn test_parse_invalid_point_not_an_array() {
        let value = FalkorValue::String("not an array".to_string());
        let result = Point::parse(value);
        assert!(result.is_err());
        // Check for the specific error type if needed
    }
}
