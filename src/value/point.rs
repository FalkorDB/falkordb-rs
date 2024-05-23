/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorValue};

/// A point in the world.
#[derive(Clone, Debug)]
pub struct Point {
    /// The latitude coordinate
    pub latitude: f64,
    /// The longitude coordinate
    pub longitude: f64,
}

impl Point {
    pub fn parse(value: FalkorValue) -> anyhow::Result<Point> {
        let [lat, long]: [FalkorValue; 2] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        Ok(Point {
            latitude: lat.to_f64().ok_or(FalkorDBError::ParsingError)?,
            longitude: long.to_f64().ok_or(FalkorDBError::ParsingError)?,
        })
    }
}
