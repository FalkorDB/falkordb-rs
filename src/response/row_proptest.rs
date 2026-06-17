/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Property-based tests for [`Row`].
//!
//! These complement the golden unit tests in [`super::row`] with invariants that must hold for an
//! arbitrary row:
//! - **no-panic robustness**: [`Row::try_get`]/[`Row::try_get_at`] always return `Ok`/`Err`,
//!   whatever the column name, index, or target type;
//! - **first-match / last-wins** duplicate-column semantics for [`Row::get`]/[`Row::into_map`];
//! - **round-trip**: building a row from `(column, value)` pairs preserves columns and values.

use crate::{FalkorValue, Row};
use proptest::collection::vec;
use proptest::prelude::*;

/// An arbitrary [`FalkorValue`]. Floats are kept finite so structural equality (used by the
/// invariants below) is reflexive (`NaN != NaN` would otherwise spuriously fail comparisons).
fn falkor_value_strategy() -> impl Strategy<Value = FalkorValue> {
    let leaf = prop_oneof![
        Just(FalkorValue::None),
        any::<bool>().prop_map(FalkorValue::Bool),
        any::<i64>().prop_map(FalkorValue::I64),
        (-1.0e12f64..1.0e12).prop_map(FalkorValue::F64),
        ".*".prop_map(FalkorValue::String),
    ];
    leaf.prop_recursive(2, 8, 4, |inner| {
        vec(inner, 0..4).prop_map(FalkorValue::Array)
    })
}

/// A short column name, biased towards collisions (and the empty string) so the duplicate-column
/// rules are exercised.
fn column_strategy() -> impl Strategy<Value = String> {
    prop_oneof![Just(String::new()), "[a-c]{0,3}"]
}

prop_compose! {
    fn row_strategy()(
        pairs in vec((column_strategy(), falkor_value_strategy()), 0..6)
    ) -> (Vec<(String, FalkorValue)>, Row) {
        let row = Row::from_iter(pairs.clone());
        (pairs, row)
    }
}

proptest! {
    /// Typed extraction never panics, whatever the row and the requested column / index / type.
    #[test]
    fn prop_try_get_never_panics(
        (_, row) in row_strategy(),
        name in column_strategy(),
        index in 0usize..8,
    ) {
        let _ = row.try_get::<i64>(&name);
        let _ = row.try_get::<f64>(&name);
        let _ = row.try_get::<bool>(&name);
        let _ = row.try_get::<String>(&name);
        let _ = row.try_get::<Option<i64>>(&name);
        let _ = row.try_get::<Vec<i64>>(&name);
        let _ = row.try_get::<FalkorValue>(&name);
        let _ = row.try_get_at::<i64>(index);
        let _ = row.try_get_at::<FalkorValue>(index);
    }

    /// `get` returns the value at the first index whose column matches (the documented dup rule).
    #[test]
    fn prop_get_returns_first_match((pairs, row) in row_strategy()) {
        for (name, _) in &pairs {
            let first = pairs.iter().find(|(column, _)| column == name).map(|(_, value)| value);
            prop_assert_eq!(row.get(name), first);
        }
    }

    /// `get_at` and `try_get_at` agree, and are `Some`/`Ok` exactly within bounds.
    #[test]
    fn prop_get_at_in_bounds((pairs, row) in row_strategy(), index in 0usize..8) {
        let expected = pairs.get(index).map(|(_, value)| value);
        prop_assert_eq!(row.get_at(index), expected);
        match row.try_get_at::<FalkorValue>(index) {
            Ok(value) => prop_assert_eq!(Some(&value), expected),
            Err(_) => prop_assert!(expected.is_none()),
        }
    }

    /// `get_all` returns exactly the values whose column matches, in order.
    #[test]
    fn prop_get_all_matches((pairs, row) in row_strategy(), name in column_strategy()) {
        let expected: Vec<&FalkorValue> = pairs
            .iter()
            .filter(|(column, _)| *column == name)
            .map(|(_, value)| value)
            .collect();
        let actual: Vec<&FalkorValue> = row.get_all(&name).collect();
        prop_assert_eq!(actual, expected);
    }

    /// `into_map` keeps the last value for a duplicated column name.
    #[test]
    fn prop_into_map_last_wins((pairs, row) in row_strategy()) {
        let map = row.into_map();
        prop_assert_eq!(map.len(), pairs.iter().map(|(column, _)| column).collect::<std::collections::HashSet<_>>().len());
        for (name, _) in &pairs {
            let last = pairs.iter().rev().find(|(column, _)| column == name).map(|(_, value)| value);
            prop_assert_eq!(map.get(name), last);
        }
    }

    /// Building from `(column, value)` pairs preserves columns and values in order.
    #[test]
    fn prop_columns_values_roundtrip((pairs, row) in row_strategy()) {
        let columns: Vec<String> = pairs.iter().map(|(column, _)| column.clone()).collect();
        let values: Vec<FalkorValue> = pairs.iter().map(|(_, value)| value.clone()).collect();
        prop_assert_eq!(row.columns(), columns.as_slice());
        prop_assert_eq!(row.len(), values.len());
        prop_assert_eq!(row.into_values(), values);
    }
}
