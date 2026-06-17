/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::parser::ParserTypeMarker;
use crate::{parser::parse_type, FalkorDBError, FalkorResult, FalkorValue, GraphSchema, Row};
use std::collections::VecDeque;
use std::sync::Arc;

/// A wrapper around the returned raw data, parsing each row on demand into a header-aware [`Row`].
///
/// This implements [`Iterator`] with `Item = FalkorResult<Row>`, so a row that fails to parse
/// surfaces as an [`Err`] (rather than being swallowed) and can be short-circuited with
/// `collect::<FalkorResult<Vec<Row>>>()` or handled per row with `?`.
pub struct LazyResultSet<'a> {
    header: Arc<[String]>,
    data: VecDeque<redis::Value>,
    graph_schema: &'a mut GraphSchema,
}

impl<'a> LazyResultSet<'a> {
    pub(crate) fn new(
        header: Arc<[String]>,
        data: Vec<redis::Value>,
        graph_schema: &'a mut GraphSchema,
    ) -> Self {
        Self {
            header,
            data: data.into(),
            graph_schema,
        }
    }

    /// Returns the remaining rows in the result set.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns whether this result set is empty or depleted
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Iterates the rows as bare `Vec<FalkorValue>`, reproducing the pre-0.7 behavior in which a
    /// row that fails to parse is yielded as a single `[FalkorValue::Unparseable]` element instead
    /// of surfacing the error.
    ///
    /// This is an opt-in escape hatch for code that cannot adopt the fallible, header-aware
    /// iteration yet. Prefer the default `Iterator<Item = FalkorResult<Row>>`, which lets you `?`
    /// real parse errors and read columns by name.
    pub fn into_values_lossy(mut self) -> impl Iterator<Item = Vec<FalkorValue>> + 'a {
        std::iter::from_fn(move || {
            self.data.pop_front().map(|current_result| {
                parse_type(ParserTypeMarker::Array, current_result, self.graph_schema)
                    .and_then(FalkorValue::into_vec)
                    .unwrap_or_else(|err| vec![FalkorValue::Unparseable(err.to_string())])
            })
        })
    }
}

impl Iterator for LazyResultSet<'_> {
    type Item = FalkorResult<Row>;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Next Result", skip_all)
    )]
    fn next(&mut self) -> Option<Self::Item> {
        let current_result = self.data.pop_front()?;
        let parsed = parse_type(ParserTypeMarker::Array, current_result, self.graph_schema)
            .and_then(FalkorValue::into_vec);
        Some(parsed.and_then(|values| {
            // The server returns exactly one value per header column; a mismatch means a malformed
            // result, so reject it loudly rather than silently zipping to the shorter length.
            if values.len() != self.header.len() {
                return Err(FalkorDBError::RowShapeMismatch {
                    header_len: self.header.len(),
                    value_len: values.len(),
                });
            }
            Ok(Row::new(Arc::clone(&self.header), values))
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::graph::HasGraphSchema;
    use crate::test_utils::imdb_test_client;
    use crate::{Edge, FalkorDBError, FalkorResult, FalkorValue, LazyResultSet, Node, Row};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn node_row() -> redis::Value {
        redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::Int(8),
            redis::Value::Array(vec![
                redis::Value::Int(203),
                redis::Value::Array(vec![redis::Value::Int(0)]),
                redis::Value::Array(vec![redis::Value::Array(vec![
                    redis::Value::Int(1),
                    redis::Value::Int(2),
                    redis::Value::BulkString("FirstNode".to_string().into_bytes()),
                ])]),
            ]),
        ])])
    }

    fn edge_row() -> redis::Value {
        redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::Int(7),
            redis::Value::Array(vec![
                redis::Value::Int(100),
                redis::Value::Int(0),
                redis::Value::Int(203),
                redis::Value::Int(204),
                redis::Value::Array(vec![redis::Value::Array(vec![
                    redis::Value::Int(1),
                    redis::Value::Int(2),
                    redis::Value::BulkString("Edge".to_string().into_bytes()),
                ])]),
            ]),
        ])])
    }

    fn expected_node() -> FalkorValue {
        FalkorValue::Node(Node {
            entity_id: 203,
            labels: vec!["actor".to_string()],
            properties: HashMap::from([(
                "name".to_string(),
                FalkorValue::String("FirstNode".to_string()),
            )]),
        })
    }

    fn expected_edge() -> FalkorValue {
        FalkorValue::Edge(Edge {
            entity_id: 100,
            relationship_type: "act".to_string(),
            src_node_id: 203,
            dst_node_id: 204,
            properties: HashMap::from([(
                "name".to_string(),
                FalkorValue::String("Edge".to_string()),
            )]),
        })
    }

    #[test]
    fn test_lazy_result_set() {
        let client = imdb_test_client();
        let mut graph = client.select_graph("imdb");

        let header: Arc<[String]> = Arc::from(vec!["entity".to_string()]);
        let mut result_set = LazyResultSet::new(
            header,
            vec![node_row(), node_row(), edge_row()],
            graph.get_graph_schema_mut(),
        );

        let first = result_set.next().unwrap().unwrap();
        assert_eq!(first.get("entity"), Some(&expected_node()));
        assert_eq!(first.get_at(0), Some(&expected_node()));

        let rows = result_set.collect::<FalkorResult<Vec<Row>>>().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get_at(0), Some(&expected_node()));
        assert_eq!(rows[1].get_at(0), Some(&expected_edge()));
    }

    #[test]
    fn shape_mismatch_surfaces_error() {
        let client = imdb_test_client();
        let mut graph = client.select_graph("imdb");

        // The row parses to a single value, but the header claims two columns.
        let header: Arc<[String]> = Arc::from(vec!["a".to_string(), "b".to_string()]);
        let mut result_set =
            LazyResultSet::new(header, vec![node_row()], graph.get_graph_schema_mut());

        match result_set.next() {
            Some(Err(FalkorDBError::RowShapeMismatch {
                header_len,
                value_len,
            })) => {
                assert_eq!(header_len, 2);
                assert_eq!(value_len, 1);
            }
            other => panic!("expected RowShapeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn into_values_lossy_yields_bare_rows() {
        let client = imdb_test_client();
        let mut graph = client.select_graph("imdb");

        let header: Arc<[String]> = Arc::from(vec!["entity".to_string()]);
        let result_set = LazyResultSet::new(header, vec![node_row()], graph.get_graph_schema_mut());

        let rows: Vec<Vec<FalkorValue>> = result_set.into_values_lossy().collect();
        assert_eq!(rows, vec![vec![expected_node()]]);
    }
}
