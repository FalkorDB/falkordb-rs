/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Type-safe, injection-proof query parameters.
//!
//! FalkorDB does not bind parameters server-side; the client prepends a `CYPHER name=value …`
//! preamble to the query text and the server parses each `value` as a Cypher literal. This module
//! encodes Rust values into exactly those literals (with correct escaping), so callers never have
//! to hand-quote strings or risk Cypher injection.
//!
//! Use [`QueryBuilder::with_param`](crate::QueryBuilder::with_param) /
//! [`with_params`](crate::QueryBuilder::with_params), or the lower-level [`to_cypher_param`].

use crate::value::point::Point;
use crate::value::vec32::Vec32;
use crate::{FalkorDBError, FalkorResult, FalkorValue};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;

mod sealed {
    pub trait Sealed {}
    pub trait SealedParams {}
}

/// A Rust value that can be encoded as a single FalkorDB/Cypher query-parameter literal.
///
/// This trait is **sealed**: the crate guarantees that every implementation emits exactly one
/// safe Cypher literal, which is what makes parameters injection-proof. To pass a value whose
/// type does not implement it, either convert it to a supported type (for example a map or a
/// list) or use [`QueryBuilder::with_raw_param`](crate::QueryBuilder::with_raw_param) for a raw,
/// already-valid Cypher expression.
///
/// Encoding is fallible: a few values have no Cypher representation — non-finite floats, strings
/// containing a NUL byte, integers outside the `i64` range, map keys containing a backtick, and
/// the `Node`/`Edge`/`Path` graph-entity [`FalkorValue`] variants.
pub trait IntoFalkorParam: sealed::Sealed {
    /// Append the Cypher literal for `self` to `out`.
    #[doc(hidden)]
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()>;
}

/// Encode a single value as a Cypher parameter literal.
///
/// This is the building block behind [`QueryBuilder::with_param`](crate::QueryBuilder::with_param);
/// it is exposed for testing and advanced use.
///
/// # Examples
///
/// ```
/// let s = falkordb::to_cypher_param(&"it's").unwrap();
/// assert_eq!(s, "'it\\'s'");
///
/// let s = falkordb::to_cypher_param(&[1, 2, 3]).unwrap();
/// assert_eq!(s, "[1, 2, 3]");
/// ```
///
/// # Errors
///
/// Returns [`FalkorDBError::ParamEncoding`] if the value has no Cypher representation.
pub fn to_cypher_param<V: IntoFalkorParam + ?Sized>(value: &V) -> FalkorResult<String> {
    let mut out = String::new();
    value.encode_param(&mut out)?;
    Ok(out)
}

fn param_err(message: impl Into<String>) -> FalkorDBError {
    FalkorDBError::ParamEncoding {
        parameter: None,
        message: message.into(),
    }
}

/// Attach a parameter name to an encoding error (the per-value encoders don't know the name).
fn with_param_name(
    err: FalkorDBError,
    name: &str,
) -> FalkorDBError {
    match err {
        FalkorDBError::ParamEncoding { message, .. } => FalkorDBError::ParamEncoding {
            parameter: Some(name.to_string()),
            message,
        },
        other => other,
    }
}

/// Reconstruct a stored encoding error by value (FalkorDBError is not `Clone`).
fn clone_param_err(err: &FalkorDBError) -> FalkorDBError {
    match err {
        FalkorDBError::ParamEncoding { parameter, message } => FalkorDBError::ParamEncoding {
            parameter: parameter.clone(),
            message: message.clone(),
        },
        other => param_err(other.to_string()),
    }
}

/// Validate a parameter name. Names are referenced as `$name` in the query, so they must be Cypher
/// identifiers; this also stops the name itself from breaking the `CYPHER name=value` preamble.
pub(crate) fn validate_param_name(name: &str) -> FalkorResult<()> {
    let mut chars = name.chars();
    let valid = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(FalkorDBError::ParamEncoding {
            parameter: Some(name.to_string()),
            message: "parameter name must be a Cypher identifier ([A-Za-z_][A-Za-z0-9_]*)"
                .to_string(),
        })
    }
}

/// Encode a string as a single-quoted Cypher string literal.
///
/// Verified against FalkorDB: only `\\ \' \n \r \t \b \f` are interpreted as escapes, `\uXXXX`
/// is *not*, and a NUL byte is rejected. Every other code point (including non-ASCII) is emitted
/// raw as UTF-8.
pub(crate) fn encode_str(
    s: &str,
    out: &mut String,
) -> FalkorResult<()> {
    out.push('\'');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            '\0' => {
                return Err(param_err(
                    "string contains a NUL byte, which FalkorDB cannot encode",
                ))
            }
            _ => out.push(ch),
        }
    }
    out.push('\'');
    Ok(())
}

fn is_bare_identifier(key: &str) -> bool {
    let mut chars = key.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Encode a map key. Cypher map keys must be identifiers; non-identifier keys are backtick-quoted.
/// Keys containing a backtick can't be encoded (verified: the server rejects the doubled-backtick
/// escape in a parameter value), and empty keys are invalid.
fn encode_map_key(
    key: &str,
    out: &mut String,
) -> FalkorResult<()> {
    if key.is_empty() {
        return Err(param_err("map parameter has an empty key"));
    }
    if key.contains('`') {
        return Err(param_err(
            "map parameter key contains a backtick, which FalkorDB cannot encode",
        ));
    }
    if key.contains('\0') {
        return Err(param_err(
            "map parameter key contains a NUL byte, which FalkorDB cannot encode",
        ));
    }
    if is_bare_identifier(key) {
        out.push_str(key);
    } else {
        out.push('`');
        out.push_str(key);
        out.push('`');
    }
    Ok(())
}

fn encode_list<T: IntoFalkorParam>(
    items: &[T],
    out: &mut String,
) -> FalkorResult<()> {
    out.push('[');
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        item.encode_param(out)?;
    }
    out.push(']');
    Ok(())
}

fn encode_map<'a, K, V, I>(
    entries: I,
    out: &mut String,
) -> FalkorResult<()>
where
    K: AsRef<str> + 'a,
    V: IntoFalkorParam + 'a,
    I: Iterator<Item = (&'a K, &'a V)>,
{
    out.push('{');
    for (idx, (key, value)) in entries.enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        encode_map_key(key.as_ref(), out)?;
        out.push_str(": ");
        value.encode_param(out)?;
    }
    out.push('}');
    Ok(())
}

// ---- scalar impls ---------------------------------------------------------------------------

macro_rules! impl_int_param {
    ($($t:ty),* $(,)?) => {$(
        impl sealed::Sealed for $t {}
        impl IntoFalkorParam for $t {
            fn encode_param(&self, out: &mut String) -> FalkorResult<()> {
                let _ = write!(out, "{self}");
                Ok(())
            }
        }
    )*};
}
impl_int_param!(i8, i16, i32, i64, u8, u16, u32);

macro_rules! impl_big_int_param {
    ($($t:ty),* $(,)?) => {$(
        impl sealed::Sealed for $t {}
        impl IntoFalkorParam for $t {
            fn encode_param(&self, out: &mut String) -> FalkorResult<()> {
                let value = i64::try_from(*self).map_err(|_| {
                    param_err(format!("integer {self} is outside FalkorDB's i64 range"))
                })?;
                let _ = write!(out, "{value}");
                Ok(())
            }
        }
    )*};
}
impl_big_int_param!(u64, u128, i128, usize, isize);

macro_rules! impl_float_param {
    ($($t:ty),* $(,)?) => {$(
        impl sealed::Sealed for $t {}
        impl IntoFalkorParam for $t {
            fn encode_param(&self, out: &mut String) -> FalkorResult<()> {
                if !self.is_finite() {
                    return Err(param_err(format!(
                        "float {self} is not finite; FalkorDB has no literal for NaN/Infinity"
                    )));
                }
                // `{:?}` is the shortest round-trippable form and always keeps a decimal point,
                // so the value stays a float (e.g. `1.0`, not `1`).
                let _ = write!(out, "{:?}", *self);
                Ok(())
            }
        }
    )*};
}
impl_float_param!(f32, f64);

impl sealed::Sealed for bool {}
impl IntoFalkorParam for bool {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        out.push_str(if *self { "true" } else { "false" });
        Ok(())
    }
}

// ---- string impls ---------------------------------------------------------------------------

impl sealed::Sealed for str {}
impl IntoFalkorParam for str {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        encode_str(self, out)
    }
}

impl sealed::Sealed for String {}
impl IntoFalkorParam for String {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        encode_str(self, out)
    }
}

impl sealed::Sealed for Cow<'_, str> {}
impl IntoFalkorParam for Cow<'_, str> {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        encode_str(self, out)
    }
}

impl sealed::Sealed for char {}
impl IntoFalkorParam for char {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        let mut buf = [0u8; 4];
        encode_str(self.encode_utf8(&mut buf), out)
    }
}

// ---- reference / smart-pointer impls --------------------------------------------------------

impl<T: sealed::Sealed + ?Sized> sealed::Sealed for &T {}
impl<T: IntoFalkorParam + ?Sized> IntoFalkorParam for &T {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        (**self).encode_param(out)
    }
}

impl<T: sealed::Sealed + ?Sized> sealed::Sealed for Box<T> {}
impl<T: IntoFalkorParam + ?Sized> IntoFalkorParam for Box<T> {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        (**self).encode_param(out)
    }
}

// ---- option / unit --------------------------------------------------------------------------

impl<T: sealed::Sealed> sealed::Sealed for Option<T> {}
impl<T: IntoFalkorParam> IntoFalkorParam for Option<T> {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        match self {
            Some(value) => value.encode_param(out),
            None => {
                out.push_str("null");
                Ok(())
            }
        }
    }
}

impl sealed::Sealed for () {}
impl IntoFalkorParam for () {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        out.push_str("null");
        Ok(())
    }
}

// ---- collections ----------------------------------------------------------------------------

impl<T: sealed::Sealed> sealed::Sealed for Vec<T> {}
impl<T: IntoFalkorParam> IntoFalkorParam for Vec<T> {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        encode_list(self, out)
    }
}

impl<T: sealed::Sealed> sealed::Sealed for [T] {}
impl<T: IntoFalkorParam> IntoFalkorParam for [T] {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        encode_list(self, out)
    }
}

impl<T: sealed::Sealed, const N: usize> sealed::Sealed for [T; N] {}
impl<T: IntoFalkorParam, const N: usize> IntoFalkorParam for [T; N] {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        encode_list(self, out)
    }
}

impl<K: AsRef<str>, V: sealed::Sealed> sealed::Sealed for HashMap<K, V> {}
impl<K: AsRef<str>, V: IntoFalkorParam> IntoFalkorParam for HashMap<K, V> {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        encode_map(self.iter(), out)
    }
}

impl<K: AsRef<str>, V: sealed::Sealed> sealed::Sealed for BTreeMap<K, V> {}
impl<K: AsRef<str>, V: IntoFalkorParam> IntoFalkorParam for BTreeMap<K, V> {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        encode_map(self.iter(), out)
    }
}

macro_rules! impl_tuple_param {
    ($($name:ident),+) => {
        impl<$($name: sealed::Sealed),+> sealed::Sealed for ($($name,)+) {}
        impl<$($name: IntoFalkorParam),+> IntoFalkorParam for ($($name,)+) {
            #[allow(non_snake_case)]
            fn encode_param(&self, out: &mut String) -> FalkorResult<()> {
                let ($($name,)+) = self;
                out.push('[');
                let mut first = true;
                $(
                    if !first { out.push_str(", "); }
                    first = false;
                    $name.encode_param(out)?;
                )+
                let _ = first;
                out.push(']');
                Ok(())
            }
        }
    };
}
impl_tuple_param!(A);
impl_tuple_param!(A, B);
impl_tuple_param!(A, B, C);
impl_tuple_param!(A, B, C, D);
impl_tuple_param!(A, B, C, D, E);
impl_tuple_param!(A, B, C, D, E, F);

// ---- domain types ---------------------------------------------------------------------------

impl sealed::Sealed for Point {}
impl IntoFalkorParam for Point {
    /// Encodes as the map `{latitude: …, longitude: …}`. FalkorDB cannot bind a point directly,
    /// so wrap the parameter with `point($p)` in your query.
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        out.push_str("{latitude: ");
        self.latitude.encode_param(out)?;
        out.push_str(", longitude: ");
        self.longitude.encode_param(out)?;
        out.push('}');
        Ok(())
    }
}

impl sealed::Sealed for Vec32 {}
impl IntoFalkorParam for Vec32 {
    /// Encodes as a float list. FalkorDB cannot bind a vector directly, so wrap the parameter
    /// with `vecf32($v)` in your query.
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        encode_list(&self.values, out)
    }
}

impl sealed::Sealed for FalkorValue {}
impl IntoFalkorParam for FalkorValue {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        match self {
            FalkorValue::I64(value) => value.encode_param(out),
            FalkorValue::F64(value) => value.encode_param(out),
            FalkorValue::Bool(value) => value.encode_param(out),
            FalkorValue::String(value) => value.encode_param(out),
            FalkorValue::None => {
                out.push_str("null");
                Ok(())
            }
            FalkorValue::Array(values) => encode_list(values, out),
            FalkorValue::Map(map) => encode_map(map.iter(), out),
            FalkorValue::Point(point) => point.encode_param(out),
            FalkorValue::Vec32(vec) => vec.encode_param(out),
            FalkorValue::Node(_) => Err(param_err("a Node cannot be used as a query parameter")),
            FalkorValue::Edge(_) => Err(param_err("an Edge cannot be used as a query parameter")),
            FalkorValue::Path(_) => Err(param_err("a Path cannot be used as a query parameter")),
            FalkorValue::DateTime(_) => Err(param_err(
                "a DateTime cannot be used as a query parameter; build it in Cypher instead, e.g. `datetime($s)`",
            )),
            FalkorValue::Date(_) => Err(param_err(
                "a Date cannot be used as a query parameter; build it in Cypher instead, e.g. `date($s)`",
            )),
            FalkorValue::Time(_) => Err(param_err(
                "a Time cannot be used as a query parameter; build it in Cypher instead, e.g. `localtime($s)`",
            )),
            FalkorValue::Duration(_) => Err(param_err(
                "a Duration cannot be used as a query parameter; build it in Cypher instead, e.g. `duration($s)`",
            )),
            FalkorValue::Unparseable(_) => Err(param_err(
                "an unparseable value cannot be used as a query parameter",
            )),
        }
    }
}

/// A raw, already-valid Cypher expression to use as a parameter value, **without any escaping**.
///
/// This is the explicit escape hatch behind
/// [`QueryBuilder::with_raw_param`](crate::QueryBuilder::with_raw_param). The parameter *name* is
/// still validated, but the value is your responsibility — prefer the typed values whenever
/// possible, as a malformed expression can reintroduce Cypher injection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawParam(pub String);

impl sealed::Sealed for RawParam {}
impl IntoFalkorParam for RawParam {
    fn encode_param(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        out.push_str(&self.0);
        Ok(())
    }
}

// ---- the parameter set ----------------------------------------------------------------------

/// An ordered, name-deduplicated set of query parameters.
///
/// This is what [`QueryBuilder::with_params`](crate::QueryBuilder::with_params) accepts (via
/// [`IntoFalkorParams`]). Each value is encoded eagerly when added; if a value cannot be encoded,
/// the error is stored and surfaced when the query is executed. Inserting a name that already
/// exists replaces its value ("last write wins").
#[derive(Debug, Default)]
pub struct FalkorParams {
    entries: Vec<(String, FalkorResult<String>)>,
}

impl FalkorParams {
    /// Create an empty parameter set.
    pub fn new() -> Self {
        Self::default()
    }

    fn set(
        &mut self,
        name: &str,
        encoded: FalkorResult<String>,
    ) {
        if let Some(slot) = self
            .entries
            .iter_mut()
            .find(|(existing, _)| existing == name)
        {
            slot.1 = encoded;
        } else {
            self.entries.push((name.to_string(), encoded));
        }
    }

    pub(crate) fn add_param<V: IntoFalkorParam>(
        &mut self,
        name: &str,
        value: V,
    ) {
        let encoded = validate_param_name(name)
            .and_then(|()| to_cypher_param(&value).map_err(|err| with_param_name(err, name)));
        self.set(name, encoded);
    }

    pub(crate) fn add_raw(
        &mut self,
        name: &str,
        raw_cypher: String,
    ) {
        let encoded = validate_param_name(name).map(|()| raw_cypher);
        self.set(name, encoded);
    }

    pub(crate) fn merge(
        &mut self,
        other: FalkorParams,
    ) {
        for (name, encoded) in other.entries {
            self.set(&name, encoded);
        }
    }

    /// Returns whether there are no parameters.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn first_error(&self) -> Option<FalkorDBError> {
        self.entries
            .iter()
            .find_map(|(_, encoded)| encoded.as_ref().err().map(clone_param_err))
    }

    /// Append the `CYPHER name=value …` preamble (with a trailing space) to `out`, or nothing if
    /// there are no parameters. Returns the first parameter encoding error, if any.
    pub(crate) fn encode_preamble(
        &self,
        out: &mut String,
    ) -> FalkorResult<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        out.push_str("CYPHER ");
        for (name, encoded) in &self.entries {
            let value = encoded.as_ref().map_err(clone_param_err)?;
            out.push_str(name);
            out.push('=');
            out.push_str(value);
            out.push(' ');
        }
        Ok(())
    }
}

/// A collection that can be turned into a set of query parameters for
/// [`QueryBuilder::with_params`](crate::QueryBuilder::with_params).
///
/// Implemented for [`FalkorParams`], `()` (no parameters), `Vec<(K, V)>`, `[(K, V); N]`, and
/// `HashMap`/`BTreeMap<K, V>`, where `K: AsRef<str>` and `V: IntoFalkorParam`.
pub trait IntoFalkorParams: sealed::SealedParams {
    /// Encode all entries into a [`FalkorParams`].
    #[doc(hidden)]
    fn into_falkor_params(self) -> FalkorParams;
}

impl sealed::SealedParams for FalkorParams {}
impl sealed::SealedParams for () {}
impl<K: AsRef<str>, V: IntoFalkorParam> sealed::SealedParams for Vec<(K, V)> {}
impl<K: AsRef<str>, V: IntoFalkorParam, const N: usize> sealed::SealedParams for [(K, V); N] {}
impl<K: AsRef<str>, V: IntoFalkorParam> sealed::SealedParams for HashMap<K, V> {}
impl<K: AsRef<str>, V: IntoFalkorParam> sealed::SealedParams for BTreeMap<K, V> {}

impl IntoFalkorParams for FalkorParams {
    fn into_falkor_params(self) -> FalkorParams {
        self
    }
}

impl IntoFalkorParams for () {
    fn into_falkor_params(self) -> FalkorParams {
        FalkorParams::new()
    }
}

impl<K: AsRef<str>, V: IntoFalkorParam> IntoFalkorParams for Vec<(K, V)> {
    fn into_falkor_params(self) -> FalkorParams {
        let mut params = FalkorParams::new();
        for (key, value) in self {
            params.add_param(key.as_ref(), value);
        }
        params
    }
}

impl<K: AsRef<str>, V: IntoFalkorParam, const N: usize> IntoFalkorParams for [(K, V); N] {
    fn into_falkor_params(self) -> FalkorParams {
        let mut params = FalkorParams::new();
        for (key, value) in self {
            params.add_param(key.as_ref(), value);
        }
        params
    }
}

impl<K: AsRef<str>, V: IntoFalkorParam> IntoFalkorParams for HashMap<K, V> {
    fn into_falkor_params(self) -> FalkorParams {
        let mut params = FalkorParams::new();
        for (key, value) in self {
            params.add_param(key.as_ref(), value);
        }
        params
    }
}

impl<K: AsRef<str>, V: IntoFalkorParam> IntoFalkorParams for BTreeMap<K, V> {
    fn into_falkor_params(self) -> FalkorParams {
        let mut params = FalkorParams::new();
        for (key, value) in self {
            params.add_param(key.as_ref(), value);
        }
        params
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc<V: IntoFalkorParam>(value: V) -> String {
        to_cypher_param(&value).expect("should encode")
    }

    #[test]
    fn test_encode_scalars() {
        assert_eq!(enc(42i64), "42");
        assert_eq!(enc(-7i32), "-7");
        assert_eq!(enc(255u8), "255");
        assert_eq!(enc(true), "true");
        assert_eq!(enc(false), "false");
        assert_eq!(enc(Option::<i64>::None), "null");
        assert_eq!(enc(Some(5i64)), "5");
        assert_eq!(enc(()), "null");
    }

    #[test]
    fn test_encode_floats() {
        assert_eq!(enc(1.0f64), "1.0");
        assert_eq!(enc(2.75f64), "2.75");
        assert_eq!(enc(-0.0f64), "-0.0");
        assert_eq!(enc(1.5f32), "1.5");
    }

    #[test]
    fn test_encode_non_finite_float_is_error() {
        assert!(to_cypher_param(&f64::NAN).is_err());
        assert!(to_cypher_param(&f64::INFINITY).is_err());
        assert!(to_cypher_param(&f32::NEG_INFINITY).is_err());
    }

    #[test]
    fn test_encode_strings() {
        assert_eq!(enc("hello"), "'hello'");
        assert_eq!(enc("it's"), "'it\\'s'");
        assert_eq!(enc("a\\b"), "'a\\\\b'");
        assert_eq!(enc("line1\nline2"), "'line1\\nline2'");
        assert_eq!(enc("tab\tend"), "'tab\\tend'");
        assert_eq!(enc("café ☕"), "'café ☕'");
        assert_eq!(enc('x'), "'x'");
    }

    #[test]
    fn test_encode_string_injection_is_neutralized() {
        // The classic break-out attempt becomes one inert string literal.
        let payload = "'; MATCH (n) DETACH DELETE n //";
        assert_eq!(enc(payload), "'\\'; MATCH (n) DETACH DELETE n //'");
    }

    #[test]
    fn test_encode_nul_string_is_error() {
        assert!(to_cypher_param(&"a\0b").is_err());
    }

    #[test]
    fn test_encode_big_int_range() {
        assert_eq!(enc(i64::MAX as u64), "9223372036854775807");
        assert!(to_cypher_param(&(i64::MAX as u64 + 1)).is_err());
        assert!(to_cypher_param(&u128::MAX).is_err());
    }

    #[test]
    fn test_encode_lists() {
        assert_eq!(enc(vec![1i64, 2, 3]), "[1, 2, 3]");
        assert_eq!(enc([1i64, 2]), "[1, 2]");
        assert_eq!(enc(Vec::<i64>::new()), "[]");
        assert_eq!(enc(vec!["a", "b"]), "['a', 'b']");
        assert_eq!(enc((1i64, "two", true)), "[1, 'two', true]");
    }

    #[test]
    fn test_encode_map() {
        let mut map = BTreeMap::new();
        map.insert("age", 30i64);
        map.insert("name_key", 1i64);
        // BTreeMap ⇒ deterministic key order.
        assert_eq!(enc(map), "{age: 30, name_key: 1}");
    }

    #[test]
    fn test_encode_map_quotes_non_identifier_keys() {
        let mut map = BTreeMap::new();
        map.insert("weird key", 1i64);
        assert_eq!(enc(map), "{`weird key`: 1}");
    }

    #[test]
    fn test_encode_map_backtick_key_is_error() {
        let mut map = BTreeMap::new();
        map.insert("a`b", 1i64);
        assert!(to_cypher_param(&map).is_err());
    }

    #[test]
    fn test_encode_nested() {
        let mut inner = BTreeMap::new();
        inner.insert("inner", vec![1i64, 2]);
        let outer = vec![inner];
        assert_eq!(enc(outer), "[{inner: [1, 2]}]");
    }

    #[test]
    fn test_encode_point_and_vector() {
        let point = Point {
            latitude: 1.5,
            longitude: 2.5,
        };
        assert_eq!(enc(point), "{latitude: 1.5, longitude: 2.5}");
        let vec = Vec32 {
            values: vec![1.0, 2.0],
        };
        assert_eq!(enc(vec), "[1.0, 2.0]");
    }

    #[test]
    fn test_encode_falkor_value_unsupported_variants_error() {
        use crate::value::graph_entities::Node;
        let node = FalkorValue::Node(Node::default());
        assert!(to_cypher_param(&node).is_err());
        let path = FalkorValue::Path(crate::value::path::Path::default());
        assert!(to_cypher_param(&path).is_err());
    }

    #[test]
    fn test_raw_param_is_verbatim() {
        assert_eq!(
            enc(RawParam("point({latitude: 1, longitude: 2})".to_string())),
            "point({latitude: 1, longitude: 2})"
        );
    }

    #[test]
    fn test_validate_param_name() {
        assert!(validate_param_name("age").is_ok());
        assert!(validate_param_name("_x1").is_ok());
        assert!(validate_param_name("").is_err());
        assert!(validate_param_name("1x").is_err());
        assert!(validate_param_name("a b").is_err());
        assert!(validate_param_name("a=1").is_err());
        assert!(validate_param_name("$x").is_err());
        assert!(validate_param_name("`x`").is_err());
    }

    #[test]
    fn test_params_preamble_and_dedup() {
        let mut params = FalkorParams::new();
        params.add_param("a", 1i64);
        params.add_param("b", "x");
        params.add_param("a", 2i64); // last write wins
        let mut out = String::new();
        params.encode_preamble(&mut out).unwrap();
        assert_eq!(out, "CYPHER a=2 b='x' ");
    }

    #[test]
    fn test_params_invalid_name_surfaces_error() {
        let mut params = FalkorParams::new();
        params.add_param("bad name", 1i64);
        let err = params.first_error().expect("should have an error");
        assert!(matches!(err, FalkorDBError::ParamEncoding { .. }));
        assert!(params.encode_preamble(&mut String::new()).is_err());
    }

    #[test]
    fn test_params_error_carries_name() {
        let mut params = FalkorParams::new();
        params.add_param("score", f64::NAN);
        match params.first_error() {
            Some(FalkorDBError::ParamEncoding { parameter, .. }) => {
                assert_eq!(parameter.as_deref(), Some("score"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_empty_params_no_preamble() {
        let params = FalkorParams::new();
        assert!(params.is_empty());
        let mut out = String::new();
        params.encode_preamble(&mut out).unwrap();
        assert_eq!(out, "");
    }

    #[test]
    fn test_encode_misc_value_types() {
        assert_eq!(enc(Cow::Borrowed("hi")), "'hi'");
        assert_eq!(enc(Cow::<str>::Owned("hi".to_string())), "'hi'");
        assert_eq!(enc(Box::new(5i64)), "5");
        assert_eq!(enc(42usize), "42");
        assert_eq!(enc(7isize), "7");
    }

    #[test]
    fn test_encode_tuples_of_various_arities() {
        assert_eq!(enc((1i64,)), "[1]");
        assert_eq!(enc((1i64, 2i64)), "[1, 2]");
        assert_eq!(enc((1i64, 2i64, 3i64, 4i64)), "[1, 2, 3, 4]");
        assert_eq!(enc((1i64, 2i64, 3i64, 4i64, 5i64)), "[1, 2, 3, 4, 5]");
        assert_eq!(
            enc((1i64, 2i64, 3i64, 4i64, 5i64, 6i64)),
            "[1, 2, 3, 4, 5, 6]"
        );
    }

    #[test]
    fn test_encode_hashmap() {
        let mut map = HashMap::new();
        map.insert("only", 1i64);
        assert_eq!(enc(map), "{only: 1}");
    }

    #[test]
    fn test_encode_falkor_value_variants() {
        assert_eq!(enc(FalkorValue::I64(5)), "5");
        assert_eq!(enc(FalkorValue::F64(2.5)), "2.5");
        assert_eq!(enc(FalkorValue::Bool(true)), "true");
        assert_eq!(enc(FalkorValue::String("x".to_string())), "'x'");
        assert_eq!(enc(FalkorValue::None), "null");
        assert_eq!(
            enc(FalkorValue::Array(vec![
                FalkorValue::I64(1),
                FalkorValue::I64(2)
            ])),
            "[1, 2]"
        );
        let mut map = HashMap::new();
        map.insert("k".to_string(), FalkorValue::I64(9));
        assert_eq!(enc(FalkorValue::Map(map)), "{k: 9}");
        assert_eq!(
            enc(FalkorValue::Point(Point {
                latitude: 1.0,
                longitude: 2.0
            })),
            "{latitude: 1.0, longitude: 2.0}"
        );
        assert_eq!(
            enc(FalkorValue::Vec32(Vec32 { values: vec![1.0] })),
            "[1.0]"
        );
    }

    #[test]
    fn test_encode_falkor_value_edge_and_unparseable_error() {
        use crate::value::graph_entities::Edge;
        assert!(to_cypher_param(&FalkorValue::Edge(Edge::default())).is_err());
        assert!(to_cypher_param(&FalkorValue::Unparseable("boom".to_string())).is_err());
    }

    #[test]
    fn test_encode_temporal_values_are_rejected() {
        use crate::value::temporal::{Date, DateTime, Duration, Time};
        assert!(to_cypher_param(&FalkorValue::DateTime(DateTime::new(1))).is_err());
        assert!(to_cypher_param(&FalkorValue::Date(Date::new(1))).is_err());
        assert!(to_cypher_param(&FalkorValue::Time(Time::new(1))).is_err());
        assert!(to_cypher_param(&FalkorValue::Duration(Duration::new(1))).is_err());
    }

    fn preamble_of(params: FalkorParams) -> String {
        let mut out = String::new();
        params.encode_preamble(&mut out).expect("should encode");
        out
    }

    #[test]
    fn test_into_falkor_params_impls() {
        assert_eq!(
            preamble_of(vec![("a", 1i64), ("b", 2i64)].into_falkor_params()),
            "CYPHER a=1 b=2 "
        );
        assert_eq!(
            preamble_of([("a", 1i64)].into_falkor_params()),
            "CYPHER a=1 "
        );
        assert_eq!(
            preamble_of(BTreeMap::from([("a", 1i64)]).into_falkor_params()),
            "CYPHER a=1 "
        );
        assert_eq!(
            preamble_of(HashMap::from([("a", 1i64)]).into_falkor_params()),
            "CYPHER a=1 "
        );
        assert!(().into_falkor_params().is_empty());
        let mut identity = FalkorParams::new();
        identity.add_param("a", 1i64);
        assert!(!identity.into_falkor_params().is_empty());
    }

    #[test]
    fn test_params_add_raw_and_merge() {
        let mut raw = FalkorParams::new();
        raw.add_raw("expr", "point({latitude: 1, longitude: 2})".to_string());
        assert_eq!(
            preamble_of(raw),
            "CYPHER expr=point({latitude: 1, longitude: 2}) "
        );

        let mut base = FalkorParams::new();
        base.add_param("a", 1i64);
        let mut extra = FalkorParams::new();
        extra.add_param("a", 9i64); // overrides on merge
        extra.add_param("b", 2i64);
        base.merge(extra);
        assert_eq!(preamble_of(base), "CYPHER a=9 b=2 ");
    }

    #[test]
    fn test_add_raw_validates_name() {
        let mut params = FalkorParams::new();
        params.add_raw("bad name", "1".to_string());
        assert!(params.first_error().is_some());
    }

    #[test]
    fn test_encode_map_key_nul_is_error() {
        let mut map = BTreeMap::new();
        map.insert("a\0b", 1i64);
        assert!(to_cypher_param(&map).is_err());
    }

    #[test]
    fn test_encode_f32_uses_shortest_form() {
        // f32 is encoded as its shortest round-trippable decimal (matching serde_json), so
        // `0.1f32` becomes `0.1` rather than the widened `0.10000000149...`.
        assert_eq!(enc(0.1f32), "0.1");
        assert_eq!(enc(2.0f32), "2.0");
    }

    #[test]
    fn test_encode_map_keyword_keys_stay_bare() {
        // Cypher keywords are valid bare map keys (verified against the server).
        let mut map = BTreeMap::new();
        map.insert("match", 2i64);
        map.insert("true", 1i64);
        assert_eq!(enc(map), "{match: 2, true: 1}");
    }

    #[test]
    fn test_nested_nul_propagates_error() {
        assert!(to_cypher_param(&vec!["ok", "a\0b"]).is_err());
        assert!(to_cypher_param(&("ok", "a\0b")).is_err());
        let mut map = BTreeMap::new();
        map.insert("k", "a\0b");
        assert!(to_cypher_param(&map).is_err());
        let array = FalkorValue::Array(vec![FalkorValue::String("a\0b".to_string())]);
        assert!(to_cypher_param(&array).is_err());
    }

    #[test]
    fn test_nested_non_finite_float_propagates_error() {
        assert!(to_cypher_param(&vec![1.0f64, f64::NAN]).is_err());
        let mut map = BTreeMap::new();
        map.insert("k", f64::INFINITY);
        assert!(to_cypher_param(&map).is_err());
    }

    #[test]
    fn test_nested_error_carries_param_name() {
        let mut params = FalkorParams::new();
        params.add_param("coords", vec![1.0f64, f64::NAN]); // error nested inside a list
        match params.first_error() {
            Some(FalkorDBError::ParamEncoding { parameter, .. }) => {
                assert_eq!(parameter.as_deref(), Some("coords"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
