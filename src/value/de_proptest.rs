/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Property-based tests for the `serde` integration ([`from_falkor_value`] / [`from_falkor_row`]).
//!
//! These complement the example-based unit tests in [`super::de`] with three families of
//! properties:
//!
//! 1. **Agreement with `serde_json`** — over the data model shared by [`FalkorValue`] and
//!    `serde_json::Value` (null, bool, finite numbers, string, array, object), mapping a JSON
//!    value into a [`FalkorValue`] and deserializing it back reproduces the original.
//! 2. **No-panic robustness** — deserializing an *arbitrary* [`FalkorValue`] (including the
//!    exotic variants `Node`/`Edge`/`Point`/`Path`/`Vec32`/`Unparseable` and non-finite floats)
//!    into several targets always returns `Ok`/`Err` and never panics.
//! 3. **Row-level invariants** — a single-column row maps exactly like the lone value (the
//!    header is irrelevant), and a multi-column row whose length disagrees with the header is
//!    always rejected.

use crate::value::graph_entities::{Edge, Node};
use crate::value::path::Path;
use crate::value::point::Point;
use crate::value::vec32::Vec32;
use crate::{from_falkor_row, from_falkor_value, FalkorValue};
use proptest::collection::{hash_map, vec};
use proptest::prelude::*;
use serde::Deserialize;
use serde_json::{Map, Number, Value as Json};
use std::collections::HashMap;

/// A `serde_json::Value` restricted to the data model shared with [`FalkorValue`]:
/// null, bool, finite numbers, strings, and (recursively) arrays and string-keyed objects.
fn json_strategy() -> impl Strategy<Value = Json> {
    let leaf = prop_oneof![
        Just(Json::Null),
        any::<bool>().prop_map(Json::Bool),
        any::<i64>().prop_map(|n| Json::Number(n.into())),
        // Finite, bounded floats so `serde_json::Number` can always represent them.
        (-1.0e12f64..1.0e12f64)
            .prop_map(|f| Json::Number(Number::from_f64(f).expect("bounded range is finite"))),
        ".*".prop_map(Json::String),
    ];
    leaf.prop_recursive(4, 48, 6, |inner| {
        prop_oneof![
            vec(inner.clone(), 0..6).prop_map(Json::Array),
            hash_map("[a-z]{1,8}", inner, 0..6)
                .prop_map(|m| Json::Object(m.into_iter().collect::<Map<String, Json>>())),
        ]
    })
}

/// Map a JSON value produced by [`json_strategy`] onto the equivalent [`FalkorValue`].
fn json_to_falkor(value: &Json) -> FalkorValue {
    match value {
        Json::Null => FalkorValue::None,
        Json::Bool(b) => FalkorValue::Bool(*b),
        Json::Number(n) => n
            .as_i64()
            .map(FalkorValue::I64)
            .unwrap_or_else(|| FalkorValue::F64(n.as_f64().expect("non-i64 number is f64"))),
        Json::String(s) => FalkorValue::String(s.clone()),
        Json::Array(items) => FalkorValue::Array(items.iter().map(json_to_falkor).collect()),
        Json::Object(map) => FalkorValue::Map(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_falkor(v)))
                .collect(),
        ),
    }
}

/// An arbitrary [`FalkorValue`], spanning every variant including the ones the deserializer
/// rejects (`Path`, `Unparseable`) and non-finite floats, to stress the mapping for panics.
fn falkor_strategy() -> impl Strategy<Value = FalkorValue> {
    let leaf = prop_oneof![
        Just(FalkorValue::None),
        any::<bool>().prop_map(FalkorValue::Bool),
        any::<i64>().prop_map(FalkorValue::I64),
        any::<f64>().prop_map(FalkorValue::F64),
        ".*".prop_map(FalkorValue::String),
        ".*".prop_map(FalkorValue::Unparseable),
        vec(any::<f32>(), 0..6).prop_map(|values| FalkorValue::Vec32(Vec32 { values })),
        (any::<f64>(), any::<f64>()).prop_map(|(latitude, longitude)| {
            FalkorValue::Point(Point {
                latitude,
                longitude,
            })
        }),
        Just(FalkorValue::Path(Path::default())),
    ];
    leaf.prop_recursive(4, 64, 6, |inner| {
        prop_oneof![
            vec(inner.clone(), 0..6).prop_map(FalkorValue::Array),
            hash_map("[a-z]{1,8}", inner.clone(), 0..6).prop_map(FalkorValue::Map),
            (
                any::<i64>(),
                vec("[a-z]{1,8}", 0..3),
                hash_map("[a-z]{1,8}", inner.clone(), 0..4),
            )
                .prop_map(|(entity_id, labels, properties)| {
                    FalkorValue::Node(Node {
                        entity_id,
                        labels,
                        properties,
                    })
                }),
            (
                any::<i64>(),
                "[a-z]{1,8}",
                any::<i64>(),
                any::<i64>(),
                hash_map("[a-z]{1,8}", inner, 0..4),
            )
                .prop_map(
                    |(entity_id, relationship_type, src_node_id, dst_node_id, properties)| {
                        FalkorValue::Edge(Edge {
                            entity_id,
                            relationship_type,
                            src_node_id,
                            dst_node_id,
                            properties,
                        })
                    }
                ),
        ]
    })
}

/// A sample enum target, so the no-panic property also exercises the enum deserialization path.
/// The payloads are only ever produced by `serde`, never read, hence `allow(dead_code)`.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
enum Sample {
    Unit,
    Newtype(i64),
    Struct { x: i64 },
}

proptest! {
    /// #1 — `FalkorValue`'s deserializer agrees with `serde_json` on their shared data model.
    #[test]
    fn prop_agrees_with_serde_json(json in json_strategy()) {
        let falkor = json_to_falkor(&json);
        let round_tripped: Json =
            from_falkor_value(falkor).expect("shared-model values always deserialize");
        prop_assert_eq!(round_tripped, json);
    }

    /// #2 — deserializing an arbitrary value never panics, whatever the target type.
    #[test]
    fn prop_never_panics(value in falkor_strategy()) {
        // Each call must return `Ok` or `Err`, but must never panic or abort.
        let _: Result<Json, _> = from_falkor_value(value.clone());
        let _: Result<Vec<Json>, _> = from_falkor_value(value.clone());
        let _: Result<HashMap<String, Json>, _> = from_falkor_value(value.clone());
        let _: Result<Sample, _> = from_falkor_value(value);
    }

    /// #3a — a single-column row maps exactly like the lone value.
    #[test]
    fn prop_single_column_row_matches_value(column in ".*", value in falkor_strategy()) {
        let header = vec![column];
        let from_row: Option<Json> = from_falkor_row(&header, vec![value.clone()]).ok();
        let from_value: Option<Json> = from_falkor_value(value).ok();
        prop_assert_eq!(from_row, from_value);
    }

    /// #3b — a row whose value count disagrees with the header length is always rejected,
    /// including the single-value case when the header does not also have exactly one column.
    #[test]
    fn prop_length_mismatch_is_rejected(
        (header_len, values_len) in (0usize..6, 0usize..6)
            .prop_filter("mismatched lengths", |(h, v)| h != v)
    ) {
        let header: Vec<String> = (0..header_len).map(|i| format!("c{i}")).collect();
        let values: Vec<FalkorValue> = (0..values_len).map(|i| FalkorValue::I64(i as i64)).collect();
        let result: Result<Json, _> = from_falkor_row(&header, values);
        prop_assert!(result.is_err());
    }
}
