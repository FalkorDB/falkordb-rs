/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Property-based tests for query-parameter encoding ([`to_cypher_param`]).
//!
//! These complement the golden unit tests in [`super::param`] with:
//! - **no-panic robustness**: encoding an arbitrary [`FalkorValue`] (every variant, arbitrary
//!   strings with control bytes/NUL, non-finite floats) always returns `Ok`/`Err`;
//! - **lossless string escaping**: reversing the documented escapes reproduces the input;
//! - **NUL rejection** and **float finiteness** invariants.

use crate::value::graph_entities::{Edge, Node};
use crate::value::path::Path;
use crate::value::point::Point;
use crate::value::vec32::Vec32;
use crate::{to_cypher_param, FalkorValue};
use proptest::collection::{hash_map, vec};
use proptest::prelude::*;

/// An arbitrary [`FalkorValue`] spanning every variant, with deliberately adversarial leaves
/// (arbitrary strings incl. control bytes/NUL, non-finite floats) to stress the encoders.
fn falkor_value_strategy() -> impl Strategy<Value = FalkorValue> {
    let leaf = prop_oneof![
        Just(FalkorValue::None),
        any::<bool>().prop_map(FalkorValue::Bool),
        any::<i64>().prop_map(FalkorValue::I64),
        any::<f64>().prop_map(FalkorValue::F64),
        ".*".prop_map(FalkorValue::String),
        ".*".prop_map(FalkorValue::Unparseable),
        vec(any::<f32>(), 0..5).prop_map(|values| FalkorValue::Vec32(Vec32 { values })),
        (any::<f64>(), any::<f64>()).prop_map(|(latitude, longitude)| {
            FalkorValue::Point(Point {
                latitude,
                longitude,
            })
        }),
        Just(FalkorValue::Path(Path::default())),
    ];
    leaf.prop_recursive(4, 48, 5, |inner| {
        prop_oneof![
            vec(inner.clone(), 0..5).prop_map(FalkorValue::Array),
            // Map keys are arbitrary strings, so non-identifier / backtick / empty / NUL keys
            // are exercised.
            hash_map(".*", inner.clone(), 0..5).prop_map(FalkorValue::Map),
            (
                any::<i64>(),
                vec("[a-z]{1,6}", 0..3),
                hash_map("[a-z]{1,6}", inner.clone(), 0..3),
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
                "[a-z]{1,6}",
                any::<i64>(),
                any::<i64>(),
                hash_map("[a-z]{1,6}", inner, 0..3),
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

/// Reverse the documented Cypher string escaping, to check `encode_str` is lossless.
fn unescape_cypher_string(encoded: &str) -> String {
    let inner = &encoded[1..encoded.len() - 1]; // strip the surrounding single quotes
    let mut out = String::new();
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('\\') => out.push('\\'),
                Some('\'') => out.push('\''),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('t') => out.push('\t'),
                Some('b') => out.push('\u{08}'),
                Some('f') => out.push('\u{0C}'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(ch);
        }
    }
    out
}

proptest! {
    /// Encoding any value returns `Ok`/`Err` but never panics or aborts.
    #[test]
    fn prop_to_cypher_param_never_panics(value in falkor_value_strategy()) {
        let _ = to_cypher_param(&value);
    }

    /// A string without a NUL byte encodes losslessly: reversing the escaping yields the input,
    /// and the result is a single-quoted literal.
    #[test]
    fn prop_string_escaping_is_lossless(s in "(?s).*") {
        prop_assume!(!s.contains('\0'));
        let encoded = to_cypher_param(&s).expect("non-NUL strings always encode");
        prop_assert!(encoded.starts_with('\'') && encoded.ends_with('\''));
        prop_assert_eq!(unescape_cypher_string(&encoded), s);
    }

    /// A string containing a NUL byte cannot be encoded.
    #[test]
    fn prop_nul_string_is_rejected(prefix in "(?s).*", suffix in "(?s).*") {
        let s = format!("{prefix}\0{suffix}");
        prop_assert!(to_cypher_param(&s).is_err());
    }

    /// A float encodes if and only if it is finite.
    #[test]
    fn prop_float_finiteness(f in any::<f64>()) {
        prop_assert_eq!(to_cypher_param(&f).is_ok(), f.is_finite());
    }
}
