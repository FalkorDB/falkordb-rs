/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{parser::parse_type, FalkorValue, GraphSchema};
use std::collections::VecDeque;

/// A wrapper around the returned raw data, allowing parsing on demand of each result
/// This implements Iterator, so can simply be collect()'ed into any desired container
pub struct LazyResultSet<'a> {
    data: VecDeque<redis::Value>,
    graph_schema: &'a mut GraphSchema,
}

impl<'a> LazyResultSet<'a> {
    pub(crate) fn new(
        data: Vec<redis::Value>,
        graph_schema: &'a mut GraphSchema,
    ) -> Self {
        Self {
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
}

impl<'a> Iterator for LazyResultSet<'a> {
    type Item = Vec<FalkorValue>;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Next Result", skip_all)
    )]
    fn next(&mut self) -> Option<Self::Item> {
        self.data.pop_front().map(|current_result| {
            parse_type(6, current_result, self.graph_schema)
                .and_then(FalkorValue::into_vec)
                .unwrap_or(vec![FalkorValue::Unparseable])
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::graph::HasGraphSchema;
    use crate::test_utils::create_test_client;
    use crate::{Edge, FalkorValue, LazyResultSet, Node};
    use std::collections::HashMap;

    #[test]
    fn test_lazy_result_set() {
        let client = create_test_client();
        let mut graph = client.select_graph("imdb");

        let mut result_set = LazyResultSet::new(
            vec![
                redis::Value::Bulk(vec![redis::Value::Bulk(vec![
                    redis::Value::Int(8),
                    redis::Value::Bulk(vec![
                        redis::Value::Int(203),
                        redis::Value::Bulk(vec![redis::Value::Int(0)]),
                        redis::Value::Bulk(vec![redis::Value::Bulk(vec![
                            redis::Value::Int(1),
                            redis::Value::Int(2),
                            redis::Value::Data("FirstNode".to_string().into_bytes()),
                        ])]),
                    ]),
                ])]),
                redis::Value::Bulk(vec![redis::Value::Bulk(vec![
                    redis::Value::Int(8),
                    redis::Value::Bulk(vec![
                        redis::Value::Int(203),
                        redis::Value::Bulk(vec![redis::Value::Int(0)]),
                        redis::Value::Bulk(vec![redis::Value::Bulk(vec![
                            redis::Value::Int(1),
                            redis::Value::Int(2),
                            redis::Value::Data("FirstNode".to_string().into_bytes()),
                        ])]),
                    ]),
                ])]),
                redis::Value::Bulk(vec![redis::Value::Bulk(vec![
                    redis::Value::Int(7),
                    redis::Value::Bulk(vec![
                        redis::Value::Int(100),
                        redis::Value::Int(0),
                        redis::Value::Int(203),
                        redis::Value::Int(204),
                        redis::Value::Bulk(vec![redis::Value::Bulk(vec![
                            redis::Value::Int(1),
                            redis::Value::Int(2),
                            redis::Value::Data("Edge".to_string().into_bytes()),
                        ])]),
                    ]),
                ])]),
            ],
            graph.get_graph_schema_mut(),
        );

        assert_eq!(
            result_set.next(),
            Some(vec![FalkorValue::Node(Node {
                entity_id: 203,
                labels: vec!["actor".to_string()],
                properties: HashMap::from([(
                    "name".to_string(),
                    FalkorValue::String("FirstNode".to_string())
                )]),
            })])
        );

        assert_eq!(
            result_set.collect::<Vec<_>>(),
            vec![
                vec![FalkorValue::Node(Node {
                    entity_id: 203,
                    labels: vec!["actor".to_string()],
                    properties: HashMap::from([(
                        "name".to_string(),
                        FalkorValue::String("FirstNode".to_string())
                    )]),
                })],
                vec![FalkorValue::Edge(Edge {
                    entity_id: 100,
                    relationship_type: "act".to_string(),
                    src_node_id: 203,
                    dst_node_id: 204,
                    properties: HashMap::from([(
                        "name".to_string(),
                        FalkorValue::String("Edge".to_string())
                    )]),
                })]
            ],
        );
    }
}
