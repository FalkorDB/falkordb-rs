/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! A single, header-aware result row.

use crate::{FalkorDBError, FalkorResult, FalkorValue, FromFalkorValue};
use std::collections::HashMap;
use std::sync::Arc;

/// A single result row: the query's column names (the *header*) paired with this row's values.
///
/// `Row` is what the default [`LazyResultSet`](crate::LazyResultSet) backing `QueryResult::data`
/// yields (as `FalkorResult<Row>`, so a parse error surfaces instead of being swallowed). Columns
/// can be read by name or by index, untyped (borrowing) or typed (converting):
///
/// ```
/// # use falkordb::{FalkorValue, Row};
/// # fn main() -> Result<(), falkordb::FalkorDBError> {
/// let row = Row::from_iter([
///     ("title".to_string(), FalkorValue::from("Dune")),
///     ("year".to_string(), FalkorValue::I64(1965)),
/// ]);
///
/// let title: String = row.try_get("title")?;
/// let year: i64 = row.try_get("year")?;
/// assert_eq!(title, "Dune");
/// assert_eq!(year, 1965);
/// # Ok(())
/// # }
/// ```
///
/// # Duplicate column names
///
/// A query like `RETURN a AS x, b AS x` produces two columns both named `x`. To keep every access
/// path predictable:
///
/// - [`get`](Self::get) / [`try_get`](Self::try_get) return the **first** match.
/// - [`get_all`](Self::get_all) returns **every** match, in column order.
/// - [`into_map`](Self::into_map) keeps the **last** value for a duplicated name.
///
/// Prefer distinct aliases to avoid the ambiguity entirely.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    header: Arc<[String]>,
    values: Vec<FalkorValue>,
}

impl Row {
    /// Builds a row from an already-validated header and value vector.
    ///
    /// The two are guaranteed equal in length by the caller ([`LazyResultSet`](crate::LazyResultSet)
    /// validates the row shape), so this constructor performs no check.
    pub(crate) fn new(
        header: Arc<[String]>,
        values: Vec<FalkorValue>,
    ) -> Self {
        Self { header, values }
    }

    fn index_of(
        &self,
        column: &str,
    ) -> Option<usize> {
        self.header.iter().position(|name| name == column)
    }

    /// The column names of this row, in order.
    pub fn columns(&self) -> &[String] {
        &self.header
    }

    /// The number of columns in this row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether this row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// A reference to the value in the column named `column`, or [`None`] if there is no such
    /// column. For duplicate column names this returns the **first** match (see the type docs).
    pub fn get(
        &self,
        column: &str,
    ) -> Option<&FalkorValue> {
        self.index_of(column).map(|index| &self.values[index])
    }

    /// A reference to the value at `index`, or [`None`] if `index` is out of bounds.
    pub fn get_at(
        &self,
        index: usize,
    ) -> Option<&FalkorValue> {
        self.values.get(index)
    }

    /// References to every value whose column is named `column`, in column order.
    ///
    /// This is the explicit way to handle the rare duplicate-column case (`RETURN a AS x, b AS x`);
    /// for the common unique-column case prefer [`get`](Self::get).
    pub fn get_all<'s>(
        &'s self,
        column: &str,
    ) -> impl Iterator<Item = &'s FalkorValue> + 's {
        let indices: Vec<usize> = self
            .header
            .iter()
            .enumerate()
            .filter_map(|(index, name)| (name == column).then_some(index))
            .collect();
        indices.into_iter().map(move |index| &self.values[index])
    }

    /// Extracts and converts the value in the column named `column`.
    ///
    /// Conversions are strict (see [`FromFalkorValue`]). Use `try_get::<Option<T>>` to distinguish a
    /// `null` value (`Ok(None)`) from a missing column (`Err(MissingColumn)`).
    ///
    /// # Errors
    ///
    /// - [`FalkorDBError::MissingColumn`] if no column is named `column`.
    /// - [`FalkorDBError::TypeError`] if the value is not the requested type.
    pub fn try_get<T: FromFalkorValue>(
        &self,
        column: &str,
    ) -> FalkorResult<T> {
        let index = self
            .index_of(column)
            .ok_or_else(|| FalkorDBError::MissingColumn {
                name: column.to_string(),
            })?;
        T::from_falkor_value(self.values[index].clone())
    }

    /// Extracts and converts the value at `index`.
    ///
    /// # Errors
    ///
    /// - [`FalkorDBError::ColumnIndexOutOfBounds`] if `index` is out of bounds.
    /// - [`FalkorDBError::TypeError`] if the value is not the requested type.
    pub fn try_get_at<T: FromFalkorValue>(
        &self,
        index: usize,
    ) -> FalkorResult<T> {
        let value = self
            .values
            .get(index)
            .ok_or(FalkorDBError::ColumnIndexOutOfBounds {
                index,
                len: self.values.len(),
            })?;
        T::from_falkor_value(value.clone())
    }

    /// Consumes the row, returning its values in column order.
    pub fn into_values(self) -> Vec<FalkorValue> {
        self.values
    }

    /// Consumes the row into a `{ column => value }` map.
    ///
    /// If a column name is duplicated the **last** value wins (see the type docs).
    ///
    /// ```
    /// # use falkordb::{FalkorValue, Row};
    /// let row = Row::from_iter([
    ///     ("name".to_string(), FalkorValue::from("Neo")),
    ///     ("age".to_string(), FalkorValue::I64(30)),
    /// ]);
    /// let map = row.into_map();
    /// assert_eq!(map.get("name"), Some(&FalkorValue::from("Neo")));
    /// assert_eq!(map.get("age"), Some(&FalkorValue::I64(30)));
    /// ```
    pub fn into_map(self) -> HashMap<String, FalkorValue> {
        self.header.iter().cloned().zip(self.values).collect()
    }

    /// Deserializes the whole row into `T` using `serde`.
    ///
    /// A single-column row deserializes as that column's value; a multi-column row deserializes as a
    /// map from column name to value (for structs/maps) or a sequence (for tuples). This is the same
    /// mapping used by [`QueryBuilder::query_as`](crate::QueryBuilder::query_as).
    ///
    /// # Errors
    ///
    /// Returns [`FalkorDBError::SerdeError`] if the row cannot be mapped onto `T`.
    #[cfg(feature = "serde")]
    pub fn deserialize<T: serde::de::DeserializeOwned>(self) -> FalkorResult<T> {
        crate::from_falkor_row(&self.header, self.values)
    }
}

impl IntoIterator for Row {
    type Item = FalkorValue;
    type IntoIter = std::vec::IntoIter<FalkorValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

/// Builds a [`Row`] from `(column, value)` pairs.
///
/// Useful for tests and for mapping data into a `Row` by hand. The header is taken from the keys in
/// iteration order, so duplicate keys behave exactly as a row returned by the server would.
impl FromIterator<(String, FalkorValue)> for Row {
    fn from_iter<I: IntoIterator<Item = (String, FalkorValue)>>(iter: I) -> Self {
        let (header, values): (Vec<String>, Vec<FalkorValue>) = iter.into_iter().unzip();
        Self {
            header: header.into(),
            values,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Row {
        Row::from_iter([
            ("title".to_string(), FalkorValue::from("Dune")),
            ("year".to_string(), FalkorValue::I64(1965)),
            ("rating".to_string(), FalkorValue::F64(8.2)),
            ("sequel".to_string(), FalkorValue::None),
        ])
    }

    #[test]
    fn columns_len_is_empty() {
        let row = sample();
        assert_eq!(row.columns(), ["title", "year", "rating", "sequel"]);
        assert_eq!(row.len(), 4);
        assert!(!row.is_empty());

        let empty = Row::from_iter([]);
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn get_and_get_at() {
        let row = sample();
        assert_eq!(row.get("year"), Some(&FalkorValue::I64(1965)));
        assert_eq!(row.get("missing"), None);
        assert_eq!(row.get_at(0), Some(&FalkorValue::from("Dune")));
        assert_eq!(row.get_at(99), None);
    }

    #[test]
    fn try_get_typed() {
        let row = sample();
        assert_eq!(row.try_get::<String>("title").unwrap(), "Dune");
        assert_eq!(row.try_get::<i64>("year").unwrap(), 1965);
        assert_eq!(row.try_get::<f64>("rating").unwrap(), 8.2);
        // a null value as Option
        assert_eq!(row.try_get::<Option<String>>("sequel").unwrap(), None);
    }

    #[test]
    fn try_get_missing_vs_null() {
        let row = sample();
        // absent column -> MissingColumn, even for Option
        match row.try_get::<Option<String>>("nope") {
            Err(FalkorDBError::MissingColumn { name }) => assert_eq!(name, "nope"),
            other => panic!("unexpected: {other:?}"),
        }
        // present-but-null -> Ok(None)
        assert_eq!(row.try_get::<Option<String>>("sequel").unwrap(), None);
    }

    #[test]
    fn try_get_type_error() {
        let row = sample();
        match row.try_get::<i64>("title") {
            Err(FalkorDBError::TypeError { expected, got }) => {
                assert_eq!(expected, "i64");
                assert_eq!(got, "String");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn try_get_at_out_of_bounds() {
        let row = sample();
        match row.try_get_at::<i64>(10) {
            Err(FalkorDBError::ColumnIndexOutOfBounds { index, len }) => {
                assert_eq!(index, 10);
                assert_eq!(len, 4);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn duplicate_columns_first_wins_and_get_all() {
        let row = Row::from_iter([
            ("x".to_string(), FalkorValue::I64(1)),
            ("x".to_string(), FalkorValue::I64(2)),
            ("y".to_string(), FalkorValue::I64(3)),
        ]);
        // first wins
        assert_eq!(row.get("x"), Some(&FalkorValue::I64(1)));
        assert_eq!(row.try_get::<i64>("x").unwrap(), 1);
        // get_all returns every match
        let all: Vec<&FalkorValue> = row.get_all("x").collect();
        assert_eq!(all, vec![&FalkorValue::I64(1), &FalkorValue::I64(2)]);
        // into_map: last wins
        let map = row.into_map();
        assert_eq!(map.get("x"), Some(&FalkorValue::I64(2)));
    }

    #[test]
    fn into_values_and_iter() {
        let row = sample();
        let collected: Vec<FalkorValue> = row.clone().into_iter().collect();
        assert_eq!(collected, row.into_values());
    }
}
