/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{EntityType, FalkorResult, GraphSchema, IndexType};
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
) -> FalkorResult<String> {
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

    let options_string = match options {
        Some(hashmap) => {
            let mut entries = hashmap
                .iter()
                .map(|(key, val)| Ok(format!("{key}: {}", format_index_option_value(val)?)))
                .collect::<FalkorResult<Vec<_>>>()?;
            // Sort for deterministic output regardless of HashMap iteration order.
            entries.sort();
            format!(" OPTIONS {{ {} }}", entries.join(", "))
        }
        None => String::new(),
    };

    Ok(format!(
        "CREATE {idx_type}INDEX FOR {pattern} ON ({}){}",
        properties_string, options_string
    ))
}

/// Formats a single index `OPTIONS` value as a Cypher map value.
///
/// FalkorDB's `OPTIONS` map uses unquoted identifier keys and rejects quoted ones, and it expects
/// numeric options (e.g. a vector index `dimension`) to be bare numbers rather than quoted strings.
/// Integer-looking values are therefore emitted verbatim, while everything else is encoded as a
/// single-quoted Cypher string literal via the same escaping as query parameters (see
/// [`encode_str`](crate::value::param::encode_str)), so option values stay consistent with
/// `to_cypher_param`. The only options FalkorDB accepts today are an integer `dimension` and a
/// string `similarityFunction`, both handled correctly here.
fn format_index_option_value(value: &str) -> FalkorResult<String> {
    if value.parse::<i64>().is_ok() {
        Ok(value.to_string())
    } else {
        let mut out = String::new();
        crate::value::param::encode_str(value, &mut out)?;
        Ok(out)
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
        )
        .unwrap();
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
        )
        .unwrap();
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
        )
        .unwrap();
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
        )
        .unwrap();
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
        )
        .unwrap();
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
        )
        .unwrap();
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
        assert_eq!(format_index_option_value("128").unwrap(), "128");
        assert_eq!(format_index_option_value("-3").unwrap(), "-3");
        assert_eq!(
            format_index_option_value("euclidean").unwrap(),
            "'euclidean'"
        );
        // Non-integer numerics are treated as strings (quoted), keeping the emitter simple and safe.
        assert_eq!(format_index_option_value("1.5").unwrap(), "'1.5'");
        // String values use the same Cypher escaping as query parameters — quotes, backslashes, and
        // control characters such as newlines.
        assert_eq!(format_index_option_value("a'b\\c").unwrap(), "'a\\'b\\\\c'");
        assert_eq!(format_index_option_value("a\nb").unwrap(), "'a\\nb'");
        // A NUL byte cannot be encoded, mirroring `to_cypher_param`.
        assert!(format_index_option_value("a\0b").is_err());
    }

    #[test]
    fn test_generate_create_index_query_rejects_unencodable_option() {
        // A NUL byte in an option value cannot be encoded, so query generation fails rather than
        // emitting invalid Cypher — mirroring `to_cypher_param`.
        let options = HashMap::from([("similarityFunction".to_string(), "a\0b".to_string())]);
        assert!(generate_create_index_query(
            IndexType::Vector,
            EntityType::Node,
            "Item",
            &["embedding"],
            Some(&options),
        )
        .is_err());
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
        )
        .unwrap();
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
