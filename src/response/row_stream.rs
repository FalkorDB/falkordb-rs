/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! An owned, `Send + 'static` async result set implementing [`futures_core::Stream`], available
//! with the `tokio` feature.

use crate::{FalkorResult, FalkorValue, GraphSchema, Row};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// The owned result set produced by an [`AsyncGraph`](crate::AsyncGraph) query.
///
/// Each row of the `GRAPH.QUERY` reply is parsed **eagerly** when the query runs (resolving compact
/// ids against the graph schema, refreshing it once on a cache miss) into a `FalkorResult<Row>`, so
/// a `RowStream` is a self-contained, `Send + 'static` buffer of already-decoded rows. It can be
/// moved into a `tokio::spawn` task and composed with `futures::StreamExt`/`futures::TryStreamExt`,
/// and it stays valid even if the graph is mutated or deleted before the stream is consumed.
///
/// It implements [`futures_core::Stream`], yielding `FalkorResult<Row>`. The rows are already in
/// memory, so this is a buffered stream — `poll_next` never returns `Poll::Pending`. Drain it with
/// `StreamExt`/`TryStreamExt` (`try_next().await`, `try_collect().await`,
/// `map(..).buffer_unordered(..)`, …).
///
/// ```rust,no_run
/// # async fn run(mut graph: falkordb::AsyncGraph) -> Result<(), falkordb::FalkorDBError> {
/// use futures::TryStreamExt;
/// let movies: Vec<falkordb::Row> = graph
///     .query("MATCH (m:Movie) RETURN m")
///     .execute()
///     .await?
///     .data
///     .try_collect()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct RowStream {
    rows: VecDeque<FalkorResult<Row>>,
}

impl RowStream {
    /// Parses every raw row of a reply into a [`Row`], surfacing per-row parse failures as `Err`
    /// items, while resolving compact ids against `graph_schema` (refreshing it on a cache miss).
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Row Stream", skip_all, level = "debug")
    )]
    pub(crate) fn parse(
        header: Arc<[String]>,
        raw_rows: Vec<redis::Value>,
        graph_schema: &mut GraphSchema,
    ) -> Self {
        Self {
            rows: crate::response::row::parse_rows(header, raw_rows, graph_schema).into(),
        }
    }

    /// Returns the number of rows remaining in the result set.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns whether this result set is empty or depleted.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Iterates the rows as bare `Vec<FalkorValue>`, reproducing the pre-0.7 behaviour in which a
    /// row that fails to parse is yielded as a single `[FalkorValue::Unparseable]` element instead
    /// of surfacing the error. Prefer the default fallible iteration.
    pub fn into_values_lossy(self) -> impl Iterator<Item = Vec<FalkorValue>> {
        self.rows.into_iter().map(|row| match row {
            Ok(row) => row.into_values(),
            Err(err) => vec![FalkorValue::Unparseable(err.to_string())],
        })
    }
}

impl futures_core::Stream for RowStream {
    type Item = FalkorResult<Row>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // Rows are already parsed and buffered in memory, so a row is always immediately ready;
        // this never parks the task.
        Poll::Ready(self.get_mut().rows.pop_front())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.rows.len(), Some(self.rows.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::blocking::create_empty_inner_sync_client;
    use crate::FalkorDBError;

    /// A single-column row holding one scalar `i64` (`ParserTypeMarker::I64 == 3`), which parses
    /// without needing any schema lookup.
    fn scalar_row(n: i64) -> redis::Value {
        redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::Int(3),
            redis::Value::Int(n),
        ])])
    }

    fn stream_of(values: &[i64]) -> RowStream {
        let header: Arc<[String]> = Arc::from(vec!["n".to_string()]);
        let mut schema = GraphSchema::new("test", create_empty_inner_sync_client());
        RowStream::parse(
            header,
            values.iter().map(|&n| scalar_row(n)).collect(),
            &mut schema,
        )
    }

    fn assert_send_static<T: Send + 'static>() {}

    #[test]
    fn row_stream_is_send_and_static() {
        // The whole point: a `RowStream` can move into a `tokio::spawn` task.
        assert_send_static::<RowStream>();
    }

    #[test]
    fn len_and_is_empty_track_remaining_rows() {
        let stream = stream_of(&[42, 7]);
        assert_eq!(stream.len(), 2);
        assert!(!stream.is_empty());
        assert!(stream_of(&[]).is_empty());
    }

    #[test]
    fn stream_try_collect_parses_each_row() {
        use futures::TryStreamExt;

        let rows: Vec<Row> =
            futures::executor::block_on(stream_of(&[42, 7]).try_collect()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].try_get_at::<i64>(0).unwrap(), 42);
        assert_eq!(rows[1].try_get_at::<i64>(0).unwrap(), 7);
    }

    #[test]
    fn stream_next_via_streamext() {
        use futures::StreamExt;

        let mut stream = stream_of(&[1, 2]);
        let row = futures::executor::block_on(stream.next()).unwrap().unwrap();
        assert_eq!(row.try_get_at::<i64>(0).unwrap(), 1);
        assert_eq!(stream.len(), 1);
    }

    #[test]
    fn into_values_lossy_yields_bare_rows() {
        let rows: Vec<Vec<FalkorValue>> = stream_of(&[5]).into_values_lossy().collect();
        assert_eq!(rows, vec![vec![FalkorValue::I64(5)]]);
    }

    #[test]
    fn shape_mismatch_surfaces_as_error() {
        // The header declares one column, but this row carries two values.
        let two_value_row = redis::Value::Array(vec![
            redis::Value::Array(vec![redis::Value::Int(3), redis::Value::Int(1)]),
            redis::Value::Array(vec![redis::Value::Int(3), redis::Value::Int(2)]),
        ]);
        let header: Arc<[String]> = Arc::from(vec!["n".to_string()]);
        let mut schema = GraphSchema::new("test", create_empty_inner_sync_client());
        let mut stream = RowStream::parse(header, vec![two_value_row], &mut schema);

        let err = stream.rows.pop_front().unwrap().unwrap_err();
        assert!(matches!(
            err,
            FalkorDBError::RowShapeMismatch {
                header_len: 1,
                value_len: 2,
            }
        ));
    }

    #[test]
    fn into_values_lossy_collapses_parse_errors() {
        // An unknown type marker makes `parse_type` fail; the lossy iterator must yield a single
        // `Unparseable` element rather than propagate the error.
        let bad_row = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::Int(9999),
            redis::Value::Int(1),
        ])]);
        let header: Arc<[String]> = Arc::from(vec!["n".to_string()]);
        let mut schema = GraphSchema::new("test", create_empty_inner_sync_client());
        let stream = RowStream::parse(header, vec![bad_row], &mut schema);

        let rows: Vec<Vec<FalkorValue>> = stream.into_values_lossy().collect();
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0].as_slice(), [FalkorValue::Unparseable(_)]));
    }

    #[test]
    fn size_hint_reports_remaining_rows() {
        use futures_core::Stream;

        let stream = stream_of(&[1, 2, 3]);
        assert_eq!(Stream::size_hint(&stream), (3, Some(3)));
    }
}
