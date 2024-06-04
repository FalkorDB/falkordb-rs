/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection, value::utils::parse_type, FalkorDBError,
    FalkorParsable, FalkorResult, FalkorValue, GraphSchema,
};
use std::collections::HashMap;

impl FalkorParsable for HashMap<String, FalkorValue> {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &mut GraphSchema,
        conn: &mut BorrowedSyncConnection,
    ) -> FalkorResult<Self> {
        let val_vec = value.into_vec()?;
        if val_vec.len() % 2 != 0 {
            Err(FalkorDBError::ParsingFMap)?;
        }

        Ok(val_vec
            .chunks_exact(2)
            .flat_map(|pair| {
                let [key, val]: [FalkorValue; 2] = pair
                    .to_vec()
                    .try_into()
                    .map_err(|_| FalkorDBError::ParsingFMap)?;

                let [type_marker, val]: [FalkorValue; 2] = val
                    .into_vec()?
                    .try_into()
                    .map_err(|_| FalkorDBError::ParsingFMap)?;

                FalkorResult::<_>::Ok((
                    key.into_string()?,
                    parse_type(
                        type_marker.to_i64().ok_or(FalkorDBError::ParsingKTVTypes)?,
                        val,
                        graph_schema,
                        conn,
                    )?,
                ))
            })
            .collect())
    }
}
