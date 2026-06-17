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
            _ => visitor.visit_some(self),
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
            _ => self.deserialize_any(visitor),
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
}
