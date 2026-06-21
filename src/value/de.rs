/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! `serde` integration for [`FalkorValue`].
//!
//! This module provides a [`serde::Deserializer`] implementation for [`FalkorValue`],
//! allowing query results to be mapped directly into user-defined types that derive
//! [`serde::Deserialize`]. It is only available when the `serde` feature is enabled.

use crate::{FalkorDBError, FalkorResult, FalkorValue};
use serde::de::{
    value::{MapDeserializer, SeqDeserializer, StringDeserializer},
    DeserializeSeed, Deserializer, EnumAccess, IntoDeserializer, VariantAccess, Visitor,
};
use serde::forward_to_deserialize_any;

/// Deserialize a [`FalkorValue`] into any type that implements [`serde::Deserialize`].
///
/// This is the free-function form of [`FalkorValue::deserialize_into`].
///
/// # Mapping
///
/// | [`FalkorValue`]            | Rust representation                                  |
/// |----------------------------|-----------------------------------------------------|
/// | `I64` / `F64` / `Bool`     | the matching primitive                              |
/// | `String`                   | [`String`]                                          |
/// | `None`                     | `()` or `Option::None`                              |
/// | `Array` / `Vec32`          | a sequence (e.g. [`Vec`])                           |
/// | `Map`                      | a map / struct keyed by string                      |
/// | `Node` / `Edge`            | a map / struct built from the entity's `properties` |
/// | `Point`                    | a map / struct with `latitude` and `longitude`      |
/// | `DateTime`/`Date`/`Time`/`Duration` | the raw temporal scalar as an [`i64`]      |
/// | `Path` / `Unparseable`     | an error                                            |
///
/// # Errors
///
/// Returns [`FalkorDBError::SerdeError`] if the value cannot be mapped onto the target type.
pub fn from_falkor_value<T>(value: FalkorValue) -> FalkorResult<T>
where
    T: serde::de::DeserializeOwned,
{
    T::deserialize(value.into_deserializer())
}

/// Deserialize a single result row into any type that implements [`serde::Deserialize`].
///
/// A row is the `header` (the column aliases) zipped with its `values`. This powers
/// [`crate::QueryBuilder::query_as`] and is also useful directly when you already hold a
/// header and a row.
///
/// # Mapping
///
/// - A **single-column** row deserializes as that one column's value, using the same rules as
///   [`from_falkor_value`]. This is the common case: `RETURN m` maps the node, `RETURN n.name`
///   and `RETURN 42` map the scalar.
/// - A **multi-column** row deserializes as a map from column name to value (for structs and
///   maps) or as a sequence (for tuples and [`Vec`]). Column names come from `header`, so use
///   query aliases (`RETURN m.title AS title`) to match struct field names.
///
/// # Errors
///
/// Returns [`FalkorDBError::SerdeError`] if the row cannot be mapped onto the target type, or if the
/// `values` length does not match the `header` length (a malformed result shape that would otherwise
/// be silently truncated by the header/value zip).
pub fn from_falkor_row<T>(
    header: &[String],
    mut values: Vec<FalkorValue>,
) -> FalkorResult<T>
where
    T: serde::de::DeserializeOwned,
{
    // The row is zipped with the header by name, so a length mismatch would silently drop columns
    // or values; reject it before deserializing. (Checked up front so the single-column fast path
    // below cannot mask a mismatch where the header claims a different number of columns.)
    if header.len() != values.len() {
        return Err(FalkorDBError::SerdeError(format!(
            "result row shape mismatch: header has {} column(s) but the row has {} value(s)",
            header.len(),
            values.len(),
        )));
    }
    // A single-column row is mapped on its own (so `RETURN m` maps the node and `RETURN n.name` maps
    // the scalar). Keyed on the header length, not merely the value count, so the shape is trusted.
    if header.len() == 1 {
        let value = values
            .pop()
            .expect("length checked to equal a header length of one");
        return T::deserialize(value.into_deserializer());
    }
    T::deserialize(RowDeserializer { header, values })
}

impl FalkorValue {
    /// Deserialize this [`FalkorValue`] into any type that implements [`serde::Deserialize`].
    ///
    /// This is a convenience wrapper around [`from_falkor_value`]. See its documentation for the
    /// full type mapping table.
    ///
    /// # Errors
    ///
    /// Returns [`FalkorDBError::SerdeError`] if the value cannot be mapped onto the target type.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Deserialize FalkorValue", skip_all)
    )]
    pub fn deserialize_into<T>(self) -> FalkorResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        from_falkor_value(self)
    }
}

/// A [`serde::Deserializer`] that consumes an owned [`FalkorValue`].
pub struct FalkorValueDeserializer {
    value: FalkorValue,
}

impl<'de> IntoDeserializer<'de, FalkorDBError> for FalkorValue {
    type Deserializer = FalkorValueDeserializer;

    fn into_deserializer(self) -> Self::Deserializer {
        FalkorValueDeserializer { value: self }
    }
}

impl<'de> Deserializer<'de> for FalkorValueDeserializer {
    type Error = FalkorDBError;

    fn deserialize_any<V>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            FalkorValue::I64(int_val) => visitor.visit_i64(int_val),
            FalkorValue::F64(float_val) => visitor.visit_f64(float_val),
            FalkorValue::Bool(bool_val) => visitor.visit_bool(bool_val),
            FalkorValue::String(str_val) => visitor.visit_string(str_val),
            FalkorValue::None => visitor.visit_unit(),
            FalkorValue::Array(array) => {
                SeqDeserializer::new(array.into_iter()).deserialize_any(visitor)
            }
            FalkorValue::Vec32(vec32) => {
                SeqDeserializer::new(vec32.values.into_iter()).deserialize_any(visitor)
            }
            FalkorValue::Map(map) => MapDeserializer::new(map.into_iter()).deserialize_any(visitor),
            FalkorValue::Node(node) => {
                MapDeserializer::new(node.properties.into_iter()).deserialize_any(visitor)
            }
            FalkorValue::Edge(edge) => {
                MapDeserializer::new(edge.properties.into_iter()).deserialize_any(visitor)
            }
            FalkorValue::Point(point) => MapDeserializer::new(
                [
                    ("latitude", FalkorValue::F64(point.latitude)),
                    ("longitude", FalkorValue::F64(point.longitude)),
                ]
                .into_iter(),
            )
            .deserialize_any(visitor),
            FalkorValue::DateTime(datetime) => visitor.visit_i64(datetime.seconds().get()),
            FalkorValue::Date(date) => visitor.visit_i64(date.seconds().get()),
            FalkorValue::Time(time) => visitor.visit_i64(time.seconds().get()),
            FalkorValue::Duration(duration) => visitor.visit_i64(duration.seconds().get()),
            FalkorValue::Path(_) => Err(FalkorDBError::SerdeError(
                "deserializing a Path into a user type is not supported".to_string(),
            )),
            FalkorValue::Unparseable(reason) => Err(FalkorDBError::SerdeError(format!(
                "cannot deserialize an unparseable value: {reason}"
            ))),
        }
    }

    fn deserialize_option<V>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            FalkorValue::None => visitor.visit_none(),
            value => visitor.visit_some(value.into_deserializer()),
        }
    }

    fn deserialize_unit<V>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            FalkorValue::None => visitor.visit_unit(),
            value => value.into_deserializer().deserialize_any(visitor),
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            // Unit variants are represented as a plain string, e.g. `FalkorValue::String("Red")`.
            FalkorValue::String(variant) => {
                let variant: StringDeserializer<FalkorDBError> = variant.into_deserializer();
                visitor.visit_enum(variant)
            }
            // Data-carrying variants use the externally tagged form: a single-entry map
            // `{ "Variant": payload }`.
            FalkorValue::Map(map) if map.len() == 1 => {
                let (variant, value) = map
                    .into_iter()
                    .next()
                    .expect("map with len 1 always yields one entry");
                visitor.visit_enum(EnumValueAccess { variant, value })
            }
            other => Err(FalkorDBError::SerdeError(format!(
                "cannot deserialize an enum from this value: {other:?}"
            ))),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit_struct seq tuple tuple_struct map struct
        identifier ignored_any
    }
}

/// [`EnumAccess`] for an externally tagged enum carried by a single-entry [`FalkorValue::Map`].
struct EnumValueAccess {
    variant: String,
    value: FalkorValue,
}

impl<'de> EnumAccess<'de> for EnumValueAccess {
    type Error = FalkorDBError;
    type Variant = VariantValueAccess;

    fn variant_seed<S>(
        self,
        seed: S,
    ) -> Result<(S::Value, Self::Variant), Self::Error>
    where
        S: DeserializeSeed<'de>,
    {
        let variant_de: StringDeserializer<FalkorDBError> = self.variant.into_deserializer();
        let variant = seed.deserialize(variant_de)?;
        Ok((variant, VariantValueAccess { value: self.value }))
    }
}

/// [`VariantAccess`] yielding the payload of an externally tagged enum variant.
struct VariantValueAccess {
    value: FalkorValue,
}

impl<'de> VariantAccess<'de> for VariantValueAccess {
    type Error = FalkorDBError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        serde::Deserialize::deserialize(self.value.into_deserializer())
    }

    fn newtype_variant_seed<S>(
        self,
        seed: S,
    ) -> Result<S::Value, Self::Error>
    where
        S: DeserializeSeed<'de>,
    {
        seed.deserialize(self.value.into_deserializer())
    }

    fn tuple_variant<V>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.value.into_deserializer().deserialize_any(visitor)
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.value.into_deserializer().deserialize_any(visitor)
    }
}

/// A [`serde::Deserializer`] over a single multi-column result row, pairing the result
/// `header` with the row's `values`.
///
/// Single-column rows are unwrapped by [`from_falkor_row`] before this is constructed, so this
/// only handles rows with two or more columns: structs and maps are built from the
/// column-name/value pairs, while tuples and sequences consume the values in order.
struct RowDeserializer<'h> {
    header: &'h [String],
    values: Vec<FalkorValue>,
}

impl<'de> Deserializer<'de> for RowDeserializer<'_> {
    type Error = FalkorDBError;

    fn deserialize_any<V>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        SeqDeserializer::new(self.values.into_iter()).deserialize_any(visitor)
    }

    fn deserialize_map<V>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // Borrow `&str` keys from the header rather than cloning each `String` per row.
        let RowDeserializer { header, values } = self;
        let pairs = header.iter().map(String::as_str).zip(values);
        MapDeserializer::new(pairs).deserialize_any(visitor)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_option<V>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct seq tuple tuple_struct enum identifier
        ignored_any
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::graph_entities::{Edge, Node};
    use crate::value::path::Path;
    use crate::value::point::Point;
    use crate::value::vec32::Vec32;
    use serde::Deserialize;
    use std::collections::HashMap;

    #[test]
    fn test_deserialize_scalars() {
        assert_eq!(FalkorValue::I64(42).deserialize_into::<i64>().unwrap(), 42);
        assert_eq!(FalkorValue::I64(7).deserialize_into::<u8>().unwrap(), 7);
        assert_eq!(
            FalkorValue::F64(2.5).deserialize_into::<f64>().unwrap(),
            2.5
        );
        assert!(FalkorValue::Bool(true).deserialize_into::<bool>().unwrap());
        assert_eq!(
            FalkorValue::String("hi".to_string())
                .deserialize_into::<String>()
                .unwrap(),
            "hi"
        );
    }

    #[test]
    fn test_deserialize_option() {
        assert_eq!(
            FalkorValue::None.deserialize_into::<Option<i64>>().unwrap(),
            None
        );
        assert_eq!(
            FalkorValue::I64(5)
                .deserialize_into::<Option<i64>>()
                .unwrap(),
            Some(5)
        );
    }

    #[test]
    fn test_deserialize_sequences() {
        let array = FalkorValue::Array(vec![FalkorValue::I64(1), FalkorValue::I64(2)]);
        assert_eq!(array.deserialize_into::<Vec<i64>>().unwrap(), vec![1, 2]);

        let vec32 = FalkorValue::Vec32(Vec32 {
            values: vec![1.0, 2.0],
        });
        assert_eq!(
            vec32.deserialize_into::<Vec<f32>>().unwrap(),
            vec![1.0, 2.0]
        );
    }

    #[test]
    fn test_deserialize_map() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), FalkorValue::I64(1));
        map.insert("b".to_string(), FalkorValue::I64(2));
        let result: HashMap<String, i64> = FalkorValue::Map(map).deserialize_into().unwrap();
        assert_eq!(result.get("a"), Some(&1));
        assert_eq!(result.get("b"), Some(&2));
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Movie {
        title: String,
        year: i64,
        rating: Option<f64>,
    }

    #[test]
    fn test_deserialize_node_into_struct() {
        let mut properties = HashMap::new();
        properties.insert("title".to_string(), FalkorValue::String("Heat".to_string()));
        properties.insert("year".to_string(), FalkorValue::I64(1995));
        let node = FalkorValue::Node(Node {
            entity_id: 1,
            labels: vec!["Movie".to_string()],
            properties,
        });

        let movie: Movie = node.deserialize_into().unwrap();
        assert_eq!(
            movie,
            Movie {
                title: "Heat".to_string(),
                year: 1995,
                rating: None,
            }
        );
    }

    #[test]
    fn test_deserialize_point_into_struct() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Coord {
            latitude: f64,
            longitude: f64,
        }
        let point = FalkorValue::Point(Point {
            latitude: 32.0,
            longitude: 34.0,
        });
        let coord: Coord = point.deserialize_into().unwrap();
        assert_eq!(
            coord,
            Coord {
                latitude: 32.0,
                longitude: 34.0
            }
        );
    }

    #[test]
    fn test_deserialize_errors_on_unparseable() {
        let value = FalkorValue::Unparseable("boom".to_string());
        let err = value.deserialize_into::<i64>().unwrap_err();
        assert!(matches!(err, FalkorDBError::SerdeError(_)));
    }

    #[test]
    fn test_deserialize_temporal_as_i64() {
        use crate::value::temporal::{Date, DateTime, Duration, Time};
        assert_eq!(
            FalkorValue::DateTime(DateTime::new(1_700_000_000))
                .deserialize_into::<i64>()
                .unwrap(),
            1_700_000_000
        );
        assert_eq!(
            FalkorValue::Date(Date::new(-697_161_600))
                .deserialize_into::<i64>()
                .unwrap(),
            -697_161_600
        );
        assert_eq!(
            FalkorValue::Time(Time::new(3600))
                .deserialize_into::<i64>()
                .unwrap(),
            3600
        );
        assert_eq!(
            FalkorValue::Duration(Duration::new(259_200))
                .deserialize_into::<i64>()
                .unwrap(),
            259_200
        );
    }

    #[test]
    fn test_deserialize_type_mismatch_is_error() {
        let value = FalkorValue::String("not a number".to_string());
        assert!(value.deserialize_into::<i64>().is_err());
    }

    #[derive(Debug, Deserialize, PartialEq)]
    enum Genre {
        Action,
        Drama,
        Rated(i64),
        Detailed { stars: i64 },
    }

    #[test]
    fn test_deserialize_unit_enum_variant() {
        let value = FalkorValue::String("Action".to_string());
        assert_eq!(value.deserialize_into::<Genre>().unwrap(), Genre::Action);
    }

    #[test]
    fn test_deserialize_newtype_enum_variant() {
        let mut map = HashMap::new();
        map.insert("Rated".to_string(), FalkorValue::I64(5));
        assert_eq!(
            FalkorValue::Map(map).deserialize_into::<Genre>().unwrap(),
            Genre::Rated(5)
        );
    }

    #[test]
    fn test_deserialize_struct_enum_variant() {
        let mut inner = HashMap::new();
        inner.insert("stars".to_string(), FalkorValue::I64(4));
        let mut map = HashMap::new();
        map.insert("Detailed".to_string(), FalkorValue::Map(inner));
        assert_eq!(
            FalkorValue::Map(map).deserialize_into::<Genre>().unwrap(),
            Genre::Detailed { stars: 4 }
        );
    }

    #[test]
    fn test_deserialize_tuple_rejects_extra_elements() {
        let array = FalkorValue::Array(vec![FalkorValue::I64(1), FalkorValue::I64(2)]);
        assert!(array.deserialize_into::<(i64,)>().is_err());
    }

    #[test]
    fn test_deserialize_edge_into_struct() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Rel {
            since: i64,
        }
        let mut properties = HashMap::new();
        properties.insert("since".to_string(), FalkorValue::I64(2020));
        let edge = FalkorValue::Edge(Edge {
            entity_id: 1,
            relationship_type: "KNOWS".to_string(),
            src_node_id: 2,
            dst_node_id: 3,
            properties,
        });
        assert_eq!(edge.deserialize_into::<Rel>().unwrap(), Rel { since: 2020 });
    }

    #[test]
    fn test_deserialize_path_is_error() {
        let path = FalkorValue::Path(Path::default());
        let err = path.deserialize_into::<Vec<i64>>().unwrap_err();
        assert!(matches!(err, FalkorDBError::SerdeError(_)));
    }

    #[test]
    fn test_deserialize_unit_from_none() {
        FalkorValue::None.deserialize_into::<()>().unwrap();
    }

    #[test]
    fn test_deserialize_newtype_struct() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Wrapper(i64);
        assert_eq!(
            FalkorValue::I64(9).deserialize_into::<Wrapper>().unwrap(),
            Wrapper(9)
        );
    }

    #[test]
    fn test_deserialize_tuple_enum_variant() {
        #[derive(Debug, Deserialize, PartialEq)]
        enum Shape {
            Rect(i64, i64),
        }
        let mut map = HashMap::new();
        map.insert(
            "Rect".to_string(),
            FalkorValue::Array(vec![FalkorValue::I64(3), FalkorValue::I64(4)]),
        );
        assert_eq!(
            FalkorValue::Map(map).deserialize_into::<Shape>().unwrap(),
            Shape::Rect(3, 4)
        );
    }

    #[test]
    fn test_deserialize_enum_from_invalid_value_is_error() {
        let err = FalkorValue::I64(1).deserialize_into::<Genre>().unwrap_err();
        assert!(matches!(err, FalkorDBError::SerdeError(_)));
    }

    #[test]
    fn test_row_single_column_node_into_struct() {
        let mut properties = HashMap::new();
        properties.insert("title".to_string(), FalkorValue::String("Heat".to_string()));
        properties.insert("year".to_string(), FalkorValue::I64(1995));
        let node = FalkorValue::Node(Node {
            entity_id: 1,
            labels: vec!["Movie".to_string()],
            properties,
        });

        let movie: Movie = from_falkor_row(&["m".to_string()], vec![node]).unwrap();
        assert_eq!(
            movie,
            Movie {
                title: "Heat".to_string(),
                year: 1995,
                rating: None,
            }
        );
    }

    #[test]
    fn test_row_single_column_scalar() {
        let value: i64 =
            from_falkor_row(&["count(*)".to_string()], vec![FalkorValue::I64(42)]).unwrap();
        assert_eq!(value, 42);
    }

    #[test]
    fn test_row_single_column_none_into_option() {
        let value: Option<i64> =
            from_falkor_row(&["x".to_string()], vec![FalkorValue::None]).unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_row_multi_column_into_struct_by_header() {
        let header = ["title".to_string(), "year".to_string()];
        let values = vec![
            FalkorValue::String("Heat".to_string()),
            FalkorValue::I64(1995),
        ];
        let movie: Movie = from_falkor_row(&header, values).unwrap();
        assert_eq!(
            movie,
            Movie {
                title: "Heat".to_string(),
                year: 1995,
                rating: None,
            }
        );
    }

    #[test]
    fn test_row_multi_column_into_tuple() {
        let header = ["a".to_string(), "b".to_string()];
        let values = vec![FalkorValue::I64(1), FalkorValue::String("two".to_string())];
        let pair: (i64, String) = from_falkor_row(&header, values).unwrap();
        assert_eq!(pair, (1, "two".to_string()));
    }

    #[test]
    fn test_row_multi_column_into_map() {
        let header = ["a".to_string(), "b".to_string()];
        let values = vec![FalkorValue::I64(1), FalkorValue::I64(2)];
        let map: HashMap<String, i64> = from_falkor_row(&header, values).unwrap();
        assert_eq!(map.get("a"), Some(&1));
        assert_eq!(map.get("b"), Some(&2));
    }

    #[test]
    fn test_row_missing_struct_field_is_error() {
        // Header/value lengths match, but the required `year` column is absent.
        let header = ["title".to_string(), "other".to_string()];
        let values = vec![
            FalkorValue::String("Heat".to_string()),
            FalkorValue::I64(1995),
        ];
        assert!(from_falkor_row::<Movie>(&header, values).is_err());
    }

    #[test]
    fn test_row_length_mismatch_is_error() {
        // A multi-column row whose value count differs from the header is rejected rather than
        // silently truncated by the header/value zip.
        let header = ["a".to_string(), "b".to_string(), "c".to_string()];
        let values = vec![FalkorValue::I64(1), FalkorValue::I64(2)];
        let err = from_falkor_row::<HashMap<String, i64>>(&header, values).unwrap_err();
        assert!(matches!(err, FalkorDBError::SerdeError(_)));
    }

    #[test]
    fn test_row_single_column_unparseable_is_error() {
        let err = from_falkor_row::<i64>(
            &["x".to_string()],
            vec![FalkorValue::Unparseable("boom".to_string())],
        )
        .unwrap_err();
        assert!(matches!(err, FalkorDBError::SerdeError(_)));
    }
}
