/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! An owned, `Send + 'static` async result set implementing [`futures_core::Stream`], available
//! with the `tokio` feature.

use crate::parser::{parse_type, ParserTypeMarker};
use crate::response::row::parse_row;
use crate::{FalkorResult, FalkorValue, GraphSchema, Row};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// The owned result set produced by an [`AsyncGraph`](crate::AsyncGraph) query.
///
/// When a query runs, any compact schema ids referenced by the reply are resolved up front (with an
/// async refresh on a cache miss) and a cheap `Arc`-backed snapshot of the [`GraphSchema`] is
/// captured alongside the raw reply rows. Individual rows are then decoded **lazily on demand** as
/// the stream is polled (`poll_next`), matching [`LazyResultSet`](crate::LazyResultSet) while
/// keeping `RowStream` a self-contained, `Send + 'static` value that can be moved into a
/// `tokio::spawn` task, composed with `futures::StreamExt`/`futures::TryStreamExt`, and consumed
/// even if the graph is mutated or deleted afterwards.
///
/// It implements [`futures_core::Stream`], yielding `FalkorResult<Row>`. The raw reply is already in
/// memory, so `poll_next` never returns `Poll::Pending`. Drain it with
/// `StreamExt`/`TryStreamExt` (`try_next().await`, `try_collect().await`,
/// `map(..).buffer_unordered(..)`, …).
///
/// ```rust,no_run
/// # #![recursion_limit = "256"]
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
    header: Arc<[String]>,
    raw_rows: VecDeque<redis::Value>,
    graph_schema: GraphSchema,
}

impl RowStream {
    /// Pre-scans `raw_rows` against `graph_schema` so any missing compact schema ids are recorded
    /// (or refreshed on a sync schema), and constructs a lazy [`RowStream`] holding a snapshot of
    /// `graph_schema` that decodes each row on demand when polled.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Row Stream", skip_all, level = "debug")
    )]
    pub(crate) fn parse(
        header: Arc<[String]>,
        raw_rows: Vec<redis::Value>,
        graph_schema: &mut GraphSchema,
    ) -> Self {
        graph_schema.scan_rows_for_missing_ids(&raw_rows);
        Self {
            header,
            raw_rows: raw_rows.into(),
            graph_schema: graph_schema.snapshot(),
        }
    }

    /// Returns the number of rows remaining in the result set.
    pub fn len(&self) -> usize {
        self.raw_rows.len()
    }

    /// Returns whether this result set is empty or depleted.
    pub fn is_empty(&self) -> bool {
        self.raw_rows.is_empty()
    }

    /// Iterates the rows as bare `Vec<FalkorValue>`, reproducing the pre-0.7 behaviour in which a
    /// row that fails to parse is yielded as a single `[FalkorValue::Unparseable]` element instead
    /// of surfacing the error. Prefer the default fallible iteration.
    pub fn into_values_lossy(mut self) -> impl Iterator<Item = Vec<FalkorValue>> {
        std::iter::from_fn(move || {
            self.raw_rows.pop_front().map(|raw| {
                parse_type(ParserTypeMarker::Array, raw, &mut self.graph_schema)
                    .and_then(FalkorValue::into_vec)
                    .unwrap_or_else(|err| vec![FalkorValue::Unparseable(err.to_string())])
            })
        })
    }
}

impl futures_core::Stream for RowStream {
    type Item = FalkorResult<Row>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // Raw rows and the resolved schema snapshot are in memory, so each row decodes on demand
        // without parking the task.
        let this = self.get_mut();
        Poll::Ready(
            this.raw_rows
                .pop_front()
                .map(|raw| parse_row(&this.header, raw, &mut this.graph_schema)),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.raw_rows.len(), Some(self.raw_rows.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::blocking::create_empty_inner_sync_client;
    use crate::{FalkorDBError, SchemaType};

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
        use futures::StreamExt;

        // The header declares one column, but this row carries two values.
        let two_value_row = redis::Value::Array(vec![
            redis::Value::Array(vec![redis::Value::Int(3), redis::Value::Int(1)]),
            redis::Value::Array(vec![redis::Value::Int(3), redis::Value::Int(2)]),
        ]);
        let header: Arc<[String]> = Arc::from(vec!["n".to_string()]);
        let mut schema = GraphSchema::new("test", create_empty_inner_sync_client());
        let mut stream = RowStream::parse(header, vec![two_value_row], &mut schema);

        let err = futures::executor::block_on(stream.next())
            .unwrap()
            .unwrap_err();
        assert_eq!(
            err,
            FalkorDBError::RowShapeMismatch {
                header_len: 1,
                value_len: 2,
            }
        );
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

    #[test]
    fn test_row_stream_decodes_lazily_on_poll_next() {
        use futures::StreamExt;

        // Row 0 is a valid scalar row; row 1 has an unknown type marker (`9999`) and only fails
        // when `poll_next` decodes that row.
        let malformed_row = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::Int(9999),
            redis::Value::Int(1),
        ])]);
        let header: Arc<[String]> = Arc::from(vec!["n".to_string()]);
        let mut schema = GraphSchema::new_in_memory("test");
        let mut stream = RowStream::parse(header, vec![scalar_row(42), malformed_row], &mut schema);

        // Constructing `RowStream` and polling only row 0 succeeds without evaluating row 1.
        let first = futures::executor::block_on(stream.next()).unwrap().unwrap();
        assert_eq!(first.try_get_at::<i64>(0).unwrap(), 42);
        assert_eq!(stream.len(), 1);

        // Polling the second item now decodes row 1 and surfaces its parse error.
        let second_err = futures::executor::block_on(stream.next())
            .unwrap()
            .unwrap_err();
        assert_eq!(second_err, FalkorDBError::ParsingUnknownType);
        assert!(stream.is_empty());
    }

    #[test]
    fn test_row_stream_pre_scans_missing_schema_ids_and_isolates_snapshot() {
        use futures::StreamExt;

        let node_row = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::Int(8),
            redis::Value::Array(vec![
                redis::Value::Int(1),
                redis::Value::Array(vec![redis::Value::Int(0)]),
                redis::Value::Array(vec![redis::Value::Array(vec![
                    redis::Value::Int(0),
                    redis::Value::Int(2),
                    redis::Value::SimpleString("Alice".to_string()),
                ])]),
            ]),
        ])]);
        let header: Arc<[String]> = Arc::from(vec!["n".to_string()]);
        let mut schema = GraphSchema::new_in_memory("test");

        // On a cold in-memory schema, `RowStream::parse` records missing schema types up front.
        let _cold = RowStream::parse(Arc::clone(&header), vec![node_row.clone()], &mut schema);
        assert_eq!(
            schema.take_missing(),
            vec![SchemaType::Labels, SchemaType::Properties]
        );

        // Once the schema is populated, `RowStream::parse` captures an isolated snapshot that
        // remains valid even if `schema.clear()` is called before the stream is polled.
        let reply = |name: &str| {
            redis::Value::Array(vec![
                redis::Value::Array(vec![]),
                redis::Value::Array(vec![redis::Value::Array(vec![redis::Value::SimpleString(
                    name.to_string(),
                )])]),
                redis::Value::Array(vec![]),
            ])
        };
        schema
            .apply_refresh_reply(SchemaType::Labels, reply("Person"))
            .unwrap();
        schema
            .apply_refresh_reply(SchemaType::Properties, reply("name"))
            .unwrap();

        let mut warm = RowStream::parse(header, vec![node_row], &mut schema);
        assert!(schema.take_missing().is_empty());
        schema.clear();

        let row = futures::executor::block_on(warm.next()).unwrap().unwrap();
        let node: crate::Node = row.try_get("n").unwrap();
        assert_eq!(node.labels, vec!["Person".to_string()]);
        assert_eq!(
            node.properties.get("name"),
            Some(&FalkorValue::String("Alice".to_string()))
        );
    }

    #[test]
    fn test_row_stream_concurrent_readers_do_not_block() {
        let mut base = GraphSchema::new_in_memory("test");
        let reply = redis::Value::Array(vec![
            redis::Value::Array(vec![]),
            redis::Value::Array(vec![redis::Value::Array(vec![redis::Value::SimpleString(
                "Person".to_string(),
            )])]),
            redis::Value::Array(vec![]),
        ]);
        base.apply_refresh_reply(SchemaType::Labels, reply).unwrap();
        let shared = Arc::new(parking_lot::RwLock::new(base));

        // Hold a shared read guard across another `RowStream::parse` on a second thread: if parsing
        // required an exclusive write lock, this would deadlock.
        let held_guard = shared.read();
        let shared_clone = Arc::clone(&shared);
        let handle = std::thread::spawn(move || {
            let mut snapshot = shared_clone.read().snapshot();
            let header: Arc<[String]> = Arc::from(vec!["n".to_string()]);
            let mut stream = RowStream::parse(header, vec![scalar_row(99)], &mut snapshot);
            assert!(snapshot.take_missing().is_empty());
            let row = futures::executor::block_on(futures::StreamExt::next(&mut stream))
                .unwrap()
                .unwrap();
            row.try_get_at::<i64>(0).unwrap()
        });
        assert_eq!(handle.join().unwrap(), 99);
        assert_eq!(
            held_guard.labels().get(&0).map(String::as_str),
            Some("Person")
        );
    }
}
