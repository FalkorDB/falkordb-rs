/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    value::utils::parse_type, FalkorDBError, FalkorParsable, FalkorResult, FalkorValue, GraphSchema,
};
use std::collections::HashMap;

impl FalkorParsable for HashMap<String, FalkorValue> {
    fn from_falkor_value(
        value: FalkorValue,
        graph_schema: &mut GraphSchema,
    ) -> FalkorResult<Self> {
        let val_vec = value.into_vec()?;
        if val_vec.len() % 2 != 0 {
            Err(FalkorDBError::ParsingFMap(
                "Map should have an even amount of elements".to_string(),
            ))?;
        }

        let mut out_map = HashMap::with_capacity(val_vec.len());
        for pair in val_vec.chunks_exact(2) {
            let [key, val]: [FalkorValue; 2] = pair.to_vec().try_into().map_err(|_| {
                FalkorDBError::ParsingFMap(
                    "The vec returned from using chunks_exact(2) should be comprised of 2 elements"
                        .to_string(),
                )
            })?;

            let [type_marker, val]: [FalkorValue; 2] =
                val.into_vec()?.try_into().map_err(|_| {
                    FalkorDBError::ParsingFMap(
                        "The value in a map should be comprised of a type marker and value"
                            .to_string(),
                    )
                })?;

            out_map.insert(
                key.into_string()?,
                parse_type(
                    type_marker.to_i64().ok_or(FalkorDBError::ParsingKTVTypes)?,
                    val,
                    graph_schema,
                )?,
            );
        }

        Ok(out_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{client::blocking::create_empty_client, GraphSchema};

    #[test]
    fn test_not_a_vec() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_client());

        let res: FalkorResult<HashMap<String, FalkorValue>> = FalkorParsable::from_falkor_value(
            FalkorValue::String("Hello".to_string()),
            &mut graph_schema,
        );

        assert!(res.is_err())
    }

    #[test]
    fn test_vec_odd_element_count() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_client());

        let res: FalkorResult<HashMap<String, FalkorValue>> = FalkorParsable::from_falkor_value(
            FalkorValue::Array(vec![FalkorValue::None; 7]),
            &mut graph_schema,
        );

        assert!(res.is_err())
    }

    #[test]
    fn test_val_element_is_not_array() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_client());

        let res: FalkorResult<HashMap<String, FalkorValue>> = FalkorParsable::from_falkor_value(
            FalkorValue::Array(vec![
                FalkorValue::String("Key".to_string()),
                FalkorValue::Bool(false),
            ]),
            &mut graph_schema,
        );

        assert!(res.is_err())
    }

    #[test]
    fn test_val_element_has_only_1_element() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_client());

        let res: FalkorResult<HashMap<String, FalkorValue>> = FalkorParsable::from_falkor_value(
            FalkorValue::Array(vec![
                FalkorValue::String("Key".to_string()),
                FalkorValue::Array(vec![FalkorValue::I64(7)]),
            ]),
            &mut graph_schema,
        );

        assert!(res.is_err())
    }

    #[test]
    fn test_val_element_has_ge_2_elements() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_client());

        let res: FalkorResult<HashMap<String, FalkorValue>> = FalkorParsable::from_falkor_value(
            FalkorValue::Array(vec![
                FalkorValue::String("Key".to_string()),
                FalkorValue::Array(vec![FalkorValue::I64(3); 3]),
            ]),
            &mut graph_schema,
        );

        assert!(res.is_err())
    }

    #[test]
    fn test_val_element_mismatch_type_marker() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_client());

        let res: FalkorResult<HashMap<String, FalkorValue>> = FalkorParsable::from_falkor_value(
            FalkorValue::Array(vec![
                FalkorValue::String("Key".to_string()),
                FalkorValue::Array(vec![FalkorValue::I64(3), FalkorValue::Bool(true)]),
            ]),
            &mut graph_schema,
        );

        assert!(res.is_err())
    }

    #[test]
    fn test_ok_values() {
        let mut graph_schema = GraphSchema::new("test_graph", create_empty_client());

        let res: HashMap<String, FalkorValue> = FalkorParsable::from_falkor_value(
            FalkorValue::Array(vec![
                FalkorValue::String("IntKey".to_string()),
                FalkorValue::Array(vec![FalkorValue::I64(3), FalkorValue::I64(1)]),
                FalkorValue::String("BoolKey".to_string()),
                FalkorValue::Array(vec![FalkorValue::I64(4), FalkorValue::Bool(true)]),
            ]),
            &mut graph_schema,
        )
        .expect("Could not parse map");

        assert_eq!(res.get("IntKey"), Some(FalkorValue::I64(1)).as_ref());
        assert_eq!(res.get("BoolKey"), Some(FalkorValue::Bool(true)).as_ref());
    }
}
