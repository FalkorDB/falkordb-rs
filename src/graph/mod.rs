/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{EntityType, GraphSchema, IndexType};
use std::{collections::HashMap, fmt::Display};

pub(crate) mod blocking;
pub(crate) mod query_builder;

pub(crate) mod batch;

pub(crate) mod ops;

#[cfg(feature = "tokio")]
pub(crate) mod asynchronous;

pub trait HasGraphSchema {
    fn get_graph_schema_mut(&mut self) -> &mut GraphSchema;
}

/// The similarity function a vector index uses when comparing vectors.
///
/// Passed to [`SyncGraph::create_node_vector_index`](crate::SyncGraph::create_node_vector_index)
/// and the matching edge/async helpers. [`VectorSimilarity::Euclidean`] is the FalkorDB default.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Default, strum::Display)]
#[strum(serialize_all = "lowercase")]
#[non_exhaustive]
pub enum VectorSimilarity {
    /// Euclidean (L2) distance — the FalkorDB default.
    #[default]
    Euclidean,
    /// Cosine similarity.
    Cosine,
}

pub(crate) fn generate_create_index_query<P: Display>(
    index_field_type: IndexType,
    entity_type: EntityType,
    label: &str,
    properties: &[P],
    options: Option<&HashMap<String, String>>,
) -> String {
    let properties_string = properties
        .iter()
        .map(|element| format!("l.{}", element))
        .collect::<Vec<_>>()
        .join(", ");

    let pattern = match entity_type {
        EntityType::Node => format!("(l:{})", label),
        EntityType::Edge => format!("()-[l:{}]->()", label),
    };

    let idx_type = match index_field_type {
        IndexType::Range => "",
        IndexType::Vector => "VECTOR ",
        IndexType::Fulltext => "FULLTEXT ",
    };

    let options_string = options
        .map(|hashmap| {
            let mut entries = hashmap
                .iter()
                .map(|(key, val)| format!("{key}: {}", format_index_option_value(val)))
                .collect::<Vec<_>>();
            // Sort for deterministic output regardless of HashMap iteration order.
            entries.sort();
            entries.join(", ")
        })
        .map(|options_string| format!(" OPTIONS {{ {} }}", options_string))
        .unwrap_or_default();

    format!(
        "CREATE {idx_type}INDEX FOR {pattern} ON ({}){}",
        properties_string, options_string
    )
}

/// Formats a single index `OPTIONS` value as a Cypher map value.
///
/// FalkorDB's `OPTIONS` map uses unquoted identifier keys and rejects quoted ones, and it expects
/// numeric options (e.g. a vector index `dimension`) to be bare numbers rather than quoted strings.
/// Integer-looking values are therefore emitted verbatim, while everything else is single-quoted
/// with embedded backslashes and single quotes escaped (e.g. `'euclidean'`). The only options
/// FalkorDB accepts today are an integer `dimension` and a string `similarityFunction`, both handled
/// correctly here.
fn format_index_option_value(value: &str) -> String {
    if value.parse::<i64>().is_ok() {
        value.to_string()
    } else {
        let escaped = value.replace('\\', "\\\\").replace('\'', "\\'");
        format!("'{escaped}'")
    }
}

/// Builds the `OPTIONS` map for a vector index from its `dimension` and similarity function.
pub(crate) fn vector_index_options(
    dimension: u32,
    similarity_function: VectorSimilarity,
) -> HashMap<String, String> {
    HashMap::from([
        ("dimension".to_string(), dimension.to_string()),
        (
            "similarityFunction".to_string(),
            similarity_function.to_string(),
        ),
    ])
}

pub(crate) fn generate_drop_index_query<P: Display>(
    index_field_type: IndexType,
    entity_type: EntityType,
    label: &str,
    properties: &[P],
) -> String {
    let properties_string = properties
        .iter()
        .map(|element| format!("e.{}", element))
        .collect::<Vec<_>>()
        .join(", ");

    let pattern = match entity_type {
        EntityType::Node => format!("(e:{})", label),
        EntityType::Edge => format!("()-[e:{}]->()", label),
    };

    let idx_type = match index_field_type {
        IndexType::Range => "",
        IndexType::Vector => "VECTOR",
        IndexType::Fulltext => "FULLTEXT",
    }
    .to_string();

    format!(
        "DROP {idx_type} INDEX for {pattern} ON ({})",
        properties_string
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_create_index_query_range_node() {
        let query = generate_create_index_query(
            IndexType::Range,
            EntityType::Node,
            "Person",
            &["name"],
            None,
        );
        assert!(query.contains("CREATE INDEX"));
        assert!(query.contains("(l:Person)"));
        assert!(query.contains("l.name"));
    }

    #[test]
    fn test_generate_create_index_query_range_edge() {
        let query = generate_create_index_query(
            IndexType::Range,
            EntityType::Edge,
            "KNOWS",
            &["since"],
            None,
        );
        assert!(query.contains("CREATE INDEX"));
        assert!(query.contains("()-[l:KNOWS]->()"));
        assert!(query.contains("l.since"));
    }

    #[test]
    fn test_generate_create_index_query_vector() {
        let query = generate_create_index_query(
            IndexType::Vector,
            EntityType::Node,
            "Item",
            &["embedding"],
            None,
        );
        assert!(query.contains("CREATE VECTOR INDEX"));
        assert!(query.contains("(l:Item)"));
        assert!(query.contains("l.embedding"));
    }

    #[test]
    fn test_generate_create_index_query_fulltext() {
        let query = generate_create_index_query(
            IndexType::Fulltext,
            EntityType::Node,
            "Document",
            &["content"],
            None,
        );
        assert!(query.contains("CREATE FULLTEXT INDEX"));
        assert!(query.contains("(l:Document)"));
        assert!(query.contains("l.content"));
    }

    #[test]
    fn test_generate_create_index_query_with_options() {
        let mut options = HashMap::new();
        options.insert("option1".to_string(), "value1".to_string());

        let query = generate_create_index_query(
            IndexType::Range,
            EntityType::Node,
            "Test",
            &["field"],
            Some(&options),
        );
        assert!(query.contains("OPTIONS"));
        assert!(query.contains("option1"));
        assert!(query.contains("value1"));
    }

    #[test]
    fn test_vector_index_options_are_valid_cypher() {
        let options = vector_index_options(128, VectorSimilarity::Euclidean);
        let query = generate_create_index_query(
            IndexType::Vector,
            EntityType::Node,
            "Item",
            &["embedding"],
            Some(&options),
        );
        assert!(query.contains("CREATE VECTOR INDEX"));
        // Keys are unquoted, the numeric dimension is bare, and the function name is single-quoted —
        // the form FalkorDB accepts. Sorting makes the option order deterministic.
        assert!(
            query.contains("OPTIONS { dimension: 128, similarityFunction: 'euclidean' }"),
            "{query}"
        );
        // The previous, server-rejected quoted-key form must never be produced.
        assert!(!query.contains("'dimension'"));
    }

    #[test]
    fn test_format_index_option_value() {
        assert_eq!(format_index_option_value("128"), "128");
        assert_eq!(format_index_option_value("-3"), "-3");
        assert_eq!(format_index_option_value("euclidean"), "'euclidean'");
        // Non-integer numerics are treated as strings (quoted), keeping the emitter simple and safe.
        assert_eq!(format_index_option_value("1.5"), "'1.5'");
        // Embedded single quotes and backslashes are escaped.
        assert_eq!(format_index_option_value("a'b\\c"), "'a\\'b\\\\c'");
    }

    #[test]
    fn test_vector_similarity_display() {
        assert_eq!(VectorSimilarity::Euclidean.to_string(), "euclidean");
        assert_eq!(VectorSimilarity::Cosine.to_string(), "cosine");
        assert_eq!(VectorSimilarity::default(), VectorSimilarity::Euclidean);
    }

    #[test]
    fn test_generate_create_index_query_multiple_properties() {
        let query = generate_create_index_query(
            IndexType::Range,
            EntityType::Node,
            "Person",
            &["firstName", "lastName"],
            None,
        );
        assert!(query.contains("l.firstName"));
        assert!(query.contains("l.lastName"));
    }

    #[test]
    fn test_generate_drop_index_query_range_node() {
        let query =
            generate_drop_index_query(IndexType::Range, EntityType::Node, "Person", &["name"]);
        assert!(query.contains("DROP"));
        assert!(query.contains("INDEX"));
        assert!(query.contains("(e:Person)"));
        assert!(query.contains("e.name"));
    }

    #[test]
    fn test_generate_drop_index_query_range_edge() {
        let query =
            generate_drop_index_query(IndexType::Range, EntityType::Edge, "KNOWS", &["since"]);
        assert!(query.contains("DROP"));
        assert!(query.contains("INDEX"));
        assert!(query.contains("()-[e:KNOWS]->()"));
        assert!(query.contains("e.since"));
    }

    #[test]
    fn test_generate_drop_index_query_vector() {
        let query =
            generate_drop_index_query(IndexType::Vector, EntityType::Node, "Item", &["embedding"]);
        assert!(query.contains("DROP VECTOR INDEX"));
        assert!(query.contains("(e:Item)"));
        assert!(query.contains("e.embedding"));
    }

    #[test]
    fn test_generate_drop_index_query_fulltext() {
        let query = generate_drop_index_query(
            IndexType::Fulltext,
            EntityType::Node,
            "Document",
            &["content"],
        );
        assert!(query.contains("DROP FULLTEXT INDEX"));
        assert!(query.contains("(e:Document)"));
        assert!(query.contains("e.content"));
    }

    #[test]
    fn test_generate_drop_index_query_multiple_properties() {
        let query = generate_drop_index_query(
            IndexType::Range,
            EntityType::Node,
            "Person",
            &["firstName", "lastName"],
        );
        assert!(query.contains("e.firstName"));
        assert!(query.contains("e.lastName"));
    }
}
