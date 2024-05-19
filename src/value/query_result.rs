/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::error::FalkorDBError;
use crate::value::FalkorValue;
use anyhow::Result;

#[derive(Default)]
pub struct QueryResult {
    raw_stats: Vec<FalkorValue>,
    header: Vec<FalkorValue>,
    result_set: Vec<FalkorValue>,
}

fn filter_stat_only_response(res: &[FalkorValue]) -> Result<Vec<FalkorValue>> {
    Ok(
        match match res.len() {
            0 => return Err(FalkorDBError::InvalidDataReceived.into()),
            1 => res.first(),
            _ => res.last(),
        } {
            Some(FalkorValue::FVec(val)) => val.clone(),
            _ => return Err(FalkorDBError::InvalidDataReceived.into()),
        },
    )
}

impl QueryResult {
    pub fn from_result(res: FalkorValue) -> Result<Self> {
        let res = res.into_vec()?;
        if res.is_empty() {
            return Err(FalkorDBError::InvalidDataReceived.into());
        }

        match res.len() {
            1 => res.first(),
            _ => res.last(),
        };

        let header = filter_stat_only_response(res.as_slice())?;
        if header.is_empty() {
            return Ok(Default::default());
        }

        // let records = res.into_iter().enumerate().skip(1).fold(Vec::with_capacity()) {
        //
        // }

        // match res.len() {
        //     0 => {
        //         return Err(FalkorDBError::InvalidDataReceived.into());
        //     }
        //     1 => {
        //         return Ok(Self {
        //             raw_stats: match res.first() {
        //                 Some(FalkorValue::FString(str_val)) => {
        //                     panic!("{str_val}"); // Want to catch errors here before release, in case this is actually a possibility
        //                 }
        //                 Some(FalkorValue::FVec(vec_val)) => vec_val.clone(),
        //                 _ => return Err(FalkorDBError::InvalidDataReceived.into()),
        //             },
        //             header: vec![],
        //             result_set: vec![],
        //         });
        //     }
        //     _ => {
        //         let mut new_result = Self {
        //             raw_stats: vec![],
        //             header: match res.first() {
        //                 Some(FalkorValue::FVec(vec_val)) => {
        //                     if vec_val.is_empty() {
        //                         return Ok(Default::default());
        //                     }
        //
        //                     vec_val.clone()
        //                 }
        //                 _ => {
        //                     return Err(FalkorDBError::InvalidDataReceived);
        //                 }
        //             },
        //             result_set: vec![],
        //         };
        //     }
        // }

        Err(FalkorDBError::InvalidDataReceived.into())

        // Ok(new_result)
    }
}
