/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::error::FalkorDBError;
use crate::value::FalkorValue;

#[derive(Clone, Debug)]
pub struct Point {
    latitude: f64,
    longitude: f64,
}

impl Point {
    pub fn parse(value: FalkorValue) -> anyhow::Result<Point> {
        let [lat, long]: [FalkorValue; 2] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingError)?;

        Ok(Point {
            latitude: lat.to_f64().ok_or(FalkorDBError::ParsingError)?,
            longitude: long.to_f64().ok_or(FalkorDBError::ParsingError)?,
        })
    }
}
