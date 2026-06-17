/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Typed iteration over a query result set, available with the `serde` feature.

use crate::value::from_falkor_row;
use crate::{FalkorResult, LazyResultSet};
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::sync::Arc;

/// A typed view over a [`LazyResultSet`] that deserializes each row into `T` on demand.
///
/// This is the `data` member of the [`crate::QueryResult`] returned by
/// [`crate::QueryBuilder::query_as`]. It implements [`Iterator`], yielding one
/// `FalkorResult<T>` per row, so it composes with the standard iterator adapters:
///
/// ```rust,ignore
/// let result = graph.query("MATCH (m:Movie) RETURN m").query_as::<Movie>().execute()?;
/// let movies: Vec<Movie> = result.data.collect::<Result<_, _>>()?;
/// ```
///
/// Each row is mapped with [`from_falkor_row`]: a single-column row deserializes as that
/// column's value (so `RETURN m` maps the node and `RETURN n.name` maps the scalar), while a
/// multi-column row deserializes as a map from column name to value, or as a sequence.
pub struct TypedLazyResultSet<'a, T> {
    header: Arc<[String]>,
    inner: LazyResultSet<'a>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T> TypedLazyResultSet<'a, T> {
    pub(crate) fn new(
        header: Arc<[String]>,
        inner: LazyResultSet<'a>,
    ) -> Self {
        Self {
            header,
            inner,
            _marker: PhantomData,
        }
    }

    /// Returns the number of rows remaining in the result set.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether this result set is empty or depleted.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T> Iterator for TypedLazyResultSet<'_, T>
where
    T: DeserializeOwned,
{
    type Item = FalkorResult<T>;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Deserialize Next Row", skip_all)
    )]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|row| from_falkor_row(&self.header, row))
    }
}
