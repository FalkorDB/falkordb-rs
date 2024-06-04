/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorResult, FalkorValue};

/// A point in the world.
#[derive(Clone, Debug, PartialEq)]
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
        let [lat, long]: [FalkorValue; 2] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        Ok(Point {
            latitude: lat.to_f64().ok_or(FalkorDBError::ParsingF64)?,
            longitude: long.to_f64().ok_or(FalkorDBError::ParsingF64)?,
        })
    }
}
