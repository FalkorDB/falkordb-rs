/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{ConfigValue, FalkorDBError, FalkorResult, FalkorValue};
use std::collections::HashMap;

pub(super) fn parse_config_response(
    config: Vec<FalkorValue>
) -> FalkorResult<HashMap<String, ConfigValue>> {
    if config.len() == 2 {
        let [key, val]: [FalkorValue; 2] = config.try_into().map_err(|_| {
            FalkorDBError::ParsingArrayToStructElementCount(
                "Expected exactly 2 elements for configuration option".to_string(),
            )
        })?;

        return Ok(HashMap::from([(
            key.into_string()?,
            ConfigValue::try_from(val)?,
        )]));
    }

    Ok(config
        .into_iter()
        .flat_map(|config| {
            let [key, val]: [FalkorValue; 2] = config.into_vec()?.try_into().map_err(|_| {
                FalkorDBError::ParsingArrayToStructElementCount(
                    "Expected exactly 2 elements for configuration option".to_string(),
                )
            })?;

            Result::<_, FalkorDBError>::Ok((key.into_string()?, ConfigValue::try_from(val)?))
        })
        .collect::<HashMap<String, ConfigValue>>())
}
