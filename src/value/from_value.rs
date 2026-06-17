/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Converting a [`FalkorValue`] into a Rust type.
//!
//! This powers [`Row::try_get`](crate::Row::try_get): a value-by-value, strict (no silent lossy
//! casts) conversion that returns [`FalkorDBError::TypeError`] on a mismatch.

use crate::value::graph_entities::{Edge, Node};
use crate::value::path::Path;
use crate::value::point::Point;
use crate::value::vec32::Vec32;
use crate::{FalkorDBError, FalkorResult, FalkorValue};
use std::collections::HashMap;

/// The largest magnitude integer for which every smaller-or-equal integer is exactly representable
/// as an `f64` (`2^53`). `i64` values within `±F64_EXACT_INT_LIMIT` widen to `f64` losslessly.
const F64_EXACT_INT_LIMIT: i64 = 1 << 53;

/// A type that can be extracted from a [`FalkorValue`].
///
/// Implemented for the scalar types ([`i64`], [`f64`], [`bool`], [`String`]), the graph entity
/// types ([`Node`], [`Edge`], [`Point`], [`Path`], `Vec32`), smaller integers ([`i32`], [`u32`],
/// [`u64`], `usize`, range-checked), `Option<T>` (a `null` value becomes `None`), `Vec<T>`,
/// `HashMap<String, T>`, and [`FalkorValue`] itself (the identity).
///
/// Conversions are **strict**: a string `"true"` does not become a `bool`, an `F64` does not become
/// an `i64`, and out-of-range integers fail rather than silently truncating. The one widening
/// allowed is `i64` → `f64` when the integer is exactly representable (magnitude up to `2^53`).
pub trait FromFalkorValue: Sized {
    /// Convert `value` into `Self`.
    ///
    /// # Errors
    ///
    /// Returns [`FalkorDBError::TypeError`] if `value` is not the expected type (or, for the
    /// narrower integer types, is out of range).
    fn from_falkor_value(value: FalkorValue) -> FalkorResult<Self>;
}

/// The name of a [`FalkorValue`] variant, for error messages.
pub(crate) fn variant_name(value: &FalkorValue) -> &'static str {
    match value {
        FalkorValue::Node(_) => "Node",
        FalkorValue::Edge(_) => "Edge",
        FalkorValue::Array(_) => "Array",
        FalkorValue::Map(_) => "Map",
        FalkorValue::Vec32(_) => "Vec32",
        FalkorValue::String(_) => "String",
        FalkorValue::Bool(_) => "Bool",
        FalkorValue::I64(_) => "I64",
        FalkorValue::F64(_) => "F64",
        FalkorValue::Point(_) => "Point",
        FalkorValue::Path(_) => "Path",
        FalkorValue::None => "None",
        FalkorValue::Unparseable(_) => "Unparseable",
    }
}

fn type_error<T>(
    expected: &'static str,
    got: &FalkorValue,
) -> FalkorResult<T> {
    Err(FalkorDBError::TypeError {
        expected,
        got: variant_name(got),
    })
}

impl FromFalkorValue for FalkorValue {
    fn from_falkor_value(value: FalkorValue) -> FalkorResult<Self> {
        Ok(value)
    }
}

impl FromFalkorValue for i64 {
    fn from_falkor_value(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::I64(int) => Ok(int),
            other => type_error("i64", &other),
        }
    }
}

macro_rules! impl_narrow_int_from_value {
    ($($t:ty),* $(,)?) => {$(
        impl FromFalkorValue for $t {
            fn from_falkor_value(value: FalkorValue) -> FalkorResult<Self> {
                match value {
                    FalkorValue::I64(int) => <$t>::try_from(int).map_err(|_| FalkorDBError::TypeError {
                        expected: stringify!($t),
                        got: "I64",
                    }),
                    other => type_error(stringify!($t), &other),
                }
            }
        }
    )*};
}
impl_narrow_int_from_value!(i8, i16, i32, u8, u16, u32, u64, usize, isize);

impl FromFalkorValue for f64 {
    fn from_falkor_value(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::F64(float) => Ok(float),
            // An i64 with magnitude up to 2^53 is exactly representable as f64, so widening it is
            // lossless; larger integers would lose precision and are rejected instead.
            FalkorValue::I64(int)
                if (-F64_EXACT_INT_LIMIT..=F64_EXACT_INT_LIMIT).contains(&int) =>
            {
                Ok(int as f64)
            }
            other => type_error("f64", &other),
        }
    }
}

impl FromFalkorValue for bool {
    fn from_falkor_value(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::Bool(boolean) => Ok(boolean),
            other => type_error("bool", &other),
        }
    }
}

impl FromFalkorValue for String {
    fn from_falkor_value(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::String(string) => Ok(string),
            other => type_error("String", &other),
        }
    }
}

macro_rules! impl_entity_from_value {
    ($($t:ty => $variant:ident => $name:literal),* $(,)?) => {$(
        impl FromFalkorValue for $t {
            fn from_falkor_value(value: FalkorValue) -> FalkorResult<Self> {
                match value {
                    FalkorValue::$variant(entity) => Ok(entity),
                    other => type_error($name, &other),
                }
            }
        }
    )*};
}
impl_entity_from_value!(
    Node => Node => "Node",
    Edge => Edge => "Edge",
    Point => Point => "Point",
    Path => Path => "Path",
    Vec32 => Vec32 => "Vec32",
);

impl<T: FromFalkorValue> FromFalkorValue for Option<T> {
    fn from_falkor_value(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::None => Ok(None),
            other => T::from_falkor_value(other).map(Some),
        }
    }
}

impl<T: FromFalkorValue> FromFalkorValue for Vec<T> {
    fn from_falkor_value(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::Array(items) => items.into_iter().map(T::from_falkor_value).collect(),
            other => type_error("Vec", &other),
        }
    }
}

impl<T: FromFalkorValue> FromFalkorValue for HashMap<String, T> {
    fn from_falkor_value(value: FalkorValue) -> FalkorResult<Self> {
        match value {
            FalkorValue::Map(map) => map
                .into_iter()
                .map(|(key, value)| Ok((key, T::from_falkor_value(value)?)))
                .collect(),
            other => type_error("HashMap", &other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn convert<T: FromFalkorValue>(value: FalkorValue) -> FalkorResult<T> {
        T::from_falkor_value(value)
    }

    #[test]
    fn test_scalar_conversions() {
        assert_eq!(convert::<i64>(FalkorValue::I64(7)).unwrap(), 7);
        assert_eq!(convert::<f64>(FalkorValue::F64(2.5)).unwrap(), 2.5);
        assert!(convert::<bool>(FalkorValue::Bool(true)).unwrap());
        assert_eq!(
            convert::<String>(FalkorValue::String("hi".into())).unwrap(),
            "hi"
        );
        // identity
        assert_eq!(
            convert::<FalkorValue>(FalkorValue::I64(1)).unwrap(),
            FalkorValue::I64(1)
        );
    }

    #[test]
    fn test_strict_no_coercion() {
        // "true" is not a bool, F64 is not an i64.
        assert!(convert::<bool>(FalkorValue::String("true".into())).is_err());
        assert!(convert::<i64>(FalkorValue::F64(1.0)).is_err());
    }

    #[test]
    fn test_f64_from_i64_lossless_only() {
        // Exactly representable integers widen to f64...
        assert_eq!(convert::<f64>(FalkorValue::I64(1)).unwrap(), 1.0);
        assert_eq!(
            convert::<f64>(FalkorValue::I64(F64_EXACT_INT_LIMIT)).unwrap(),
            F64_EXACT_INT_LIMIT as f64
        );
        // ...but integers beyond 2^53 would lose precision and are rejected.
        assert!(convert::<f64>(FalkorValue::I64(F64_EXACT_INT_LIMIT + 1)).is_err());
        assert!(convert::<f64>(FalkorValue::I64(i64::MAX)).is_err());
    }

    #[test]
    fn test_narrow_int_range_checked() {
        assert_eq!(convert::<i32>(FalkorValue::I64(5)).unwrap(), 5);
        assert_eq!(convert::<u32>(FalkorValue::I64(5)).unwrap(), 5);
        assert!(convert::<i32>(FalkorValue::I64(i64::MAX)).is_err());
        assert!(convert::<u32>(FalkorValue::I64(-1)).is_err());
    }

    #[test]
    fn test_option_and_vec() {
        assert_eq!(convert::<Option<i64>>(FalkorValue::None).unwrap(), None);
        assert_eq!(
            convert::<Option<i64>>(FalkorValue::I64(3)).unwrap(),
            Some(3)
        );
        assert_eq!(
            convert::<Vec<i64>>(FalkorValue::Array(vec![
                FalkorValue::I64(1),
                FalkorValue::I64(2)
            ]))
            .unwrap(),
            vec![1, 2]
        );
        // element type mismatch propagates
        assert!(convert::<Vec<i64>>(FalkorValue::Array(vec![FalkorValue::Bool(true)])).is_err());
    }

    #[test]
    fn test_map_and_entities() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), FalkorValue::I64(1));
        let converted: HashMap<String, i64> = convert(FalkorValue::Map(map)).unwrap();
        assert_eq!(converted.get("a"), Some(&1));

        assert!(convert::<Node>(FalkorValue::Node(Node::default())).is_ok());
        assert!(convert::<Node>(FalkorValue::I64(1)).is_err());
    }

    #[test]
    fn test_type_error_reports_variant() {
        match convert::<i64>(FalkorValue::Bool(true)) {
            Err(FalkorDBError::TypeError { expected, got }) => {
                assert_eq!(expected, "i64");
                assert_eq!(got, "Bool");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
