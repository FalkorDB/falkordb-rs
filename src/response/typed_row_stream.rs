/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Typed async streaming over a [`RowStream`], available with the `serde` + `tokio` features.

use crate::{FalkorResult, Row, RowStream};
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A typed view over a [`RowStream`] that deserializes each row into `T` on demand.
///
/// This is the `data` member of the [`QueryResult`](crate::QueryResult) returned by
/// [`QueryBuilder::query_as`](crate::QueryBuilder::query_as) on an
/// [`AsyncGraph`](crate::AsyncGraph). It is `Send + 'static` and implements
/// [`futures_core::Stream`], yielding one `FalkorResult<T>` per row.
///
/// ```rust,no_run
/// # #![recursion_limit = "256"]
/// # use serde::Deserialize;
/// # #[derive(Deserialize)] struct Movie { title: String }
/// # async fn run(mut graph: falkordb::AsyncGraph) -> Result<(), falkordb::FalkorDBError> {
/// use futures::TryStreamExt;
/// let movies: Vec<Movie> = graph
///     .query("MATCH (m:Movie) RETURN m")
///     .query_as::<Movie>()
///     .execute()
///     .await?
///     .data
///     .try_collect()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct TypedRowStream<T> {
    inner: RowStream,
    _marker: PhantomData<fn() -> T>,
}

impl<T> TypedRowStream<T> {
    pub(crate) fn new(inner: RowStream) -> Self {
        Self {
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

impl<T> futures_core::Stream for TypedRowStream<T>
where
    T: DeserializeOwned,
{
    type Item = FalkorResult<T>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let inner = &mut self.get_mut().inner;
        futures_core::Stream::poll_next(Pin::new(inner), cx)
            .map(|row| row.map(|row| row.and_then(Row::deserialize::<T>)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.inner.len(), Some(self.inner.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::blocking::create_empty_inner_sync_client;
    use crate::GraphSchema;
    use std::sync::Arc;

    /// A single-column row holding one scalar `i64` (`ParserTypeMarker::I64 == 3`).
    fn scalar_row(n: i64) -> redis::Value {
        redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::Int(3),
            redis::Value::Int(n),
        ])])
    }

    fn typed_stream(values: &[i64]) -> TypedRowStream<i64> {
        let header: Arc<[String]> = Arc::from(vec!["n".to_string()]);
        let mut schema = GraphSchema::new("test", create_empty_inner_sync_client());
        TypedRowStream::new(RowStream::parse(
            header,
            values.iter().map(|&n| scalar_row(n)).collect(),
            &mut schema,
        ))
    }

    #[test]
    fn len_and_is_empty_track_remaining_rows() {
        assert_eq!(typed_stream(&[1, 2]).len(), 2);
        assert!(!typed_stream(&[1]).is_empty());
        assert!(typed_stream(&[]).is_empty());
    }

    #[test]
    fn try_collect_deserializes_each_row() {
        use futures::TryStreamExt;

        let out: Vec<i64> =
            futures::executor::block_on(typed_stream(&[10, 20]).try_collect()).unwrap();
        assert_eq!(out, vec![10, 20]);
    }

    #[test]
    fn size_hint_reports_remaining_rows() {
        use futures_core::Stream;

        assert_eq!(Stream::size_hint(&typed_stream(&[1, 2, 3])), (3, Some(3)));
    }
}
