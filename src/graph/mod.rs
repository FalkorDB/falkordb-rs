/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{EntityType, FalkorDBError, FalkorResult, GraphSchema, IndexType};
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

/// The keyword, with its trailing space, that an index query names the index type with.
fn index_type_keyword(index_type: IndexType) -> &'static str {
    match index_type {
        IndexType::Range => "",
        IndexType::Vector => "VECTOR ",
        IndexType::Fulltext => "FULLTEXT ",
    }
}

/// Writes a label, relationship type or property name for an index query, in backticks unless it
/// is a Cypher identifier, so that a name such as `first name` neither breaks the query nor changes
/// what it does.
fn index_name(
    kind: &'static str,
    name: &str,
) -> FalkorResult<String> {
    let mut out = String::new();
    crate::value::param::encode_name(name, &mut out).map_err(|message| {
        FalkorDBError::InvalidIdentifier {
            kind,
            name: name.to_string(),
            message,
        }
    })?;
    Ok(out)
}

/// The `FOR <pattern> ON (<properties>)` part of an index query, binding the entity to `alias`.
fn index_target<P: Display>(
    entity_type: EntityType,
    label: &str,
    properties: &[P],
    alias: char,
) -> FalkorResult<String> {
    if properties.is_empty() {
        return Err(FalkorDBError::EmptyIndexProperties);
    }

    let pattern = match entity_type {
        EntityType::Node => format!("({alias}:{})", index_name("label", label)?),
        EntityType::Edge => format!(
            "()-[{alias}:{}]->()",
            index_name("relationship type", label)?
        ),
    };

    let properties = properties
        .iter()
        .map(|property| {
            index_name("property", &property.to_string()).map(|name| format!("{alias}.{name}"))
        })
        .collect::<FalkorResult<Vec<_>>>()?
        .join(", ");

    Ok(format!("FOR {pattern} ON ({properties})"))
}

pub(crate) fn generate_create_index_query<P: Display>(
    index_field_type: IndexType,
    entity_type: EntityType,
    label: &str,
    properties: &[P],
    options: Option<&HashMap<String, String>>,
) -> FalkorResult<String> {
    let target = index_target(entity_type, label, properties, 'l')?;

    let options_string = match options {
        Some(hashmap) => {
            let mut entries = hashmap
                .iter()
                .map(|(key, val)| {
                    // Keys are interpolated unquoted, so they must be Cypher identifiers; reject
                    // anything else rather than emit invalid (or injectable) Cypher.
                    if !crate::value::param::is_bare_identifier(key) {
                        return Err(FalkorDBError::InvalidIndexOption {
                            key: key.clone(),
                            message: "must be a Cypher identifier ([A-Za-z_][A-Za-z0-9_]*)"
                                .to_string(),
                        });
                    }
                    Ok(format!("{key}: {}", format_index_option_value(val)?))
                })
                .collect::<FalkorResult<Vec<_>>>()?;
            // Sort for deterministic output regardless of HashMap iteration order.
            entries.sort();
            format!(" OPTIONS {{ {} }}", entries.join(", "))
        }
        None => String::new(),
    };

    Ok(format!(
        "CREATE {}INDEX {target}{options_string}",
        index_type_keyword(index_field_type)
    ))
}

/// Formats a single index `OPTIONS` value as a Cypher map value.
///
/// FalkorDB's `OPTIONS` map expects numeric options (e.g. a vector index `dimension`) to be bare
/// numbers rather than quoted strings. Integer-looking values are therefore emitted verbatim, while
/// everything else is encoded as a single-quoted Cypher string literal using the same escaping as
/// query parameters (see [`encode_str`](crate::value::param::encode_str)), so option values stay
/// consistent with `to_cypher_param` and a NUL byte is rejected. The only options FalkorDB accepts
/// today are an integer `dimension` and a string `similarityFunction`, both handled correctly here.
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
) -> FalkorResult<String> {
    Ok(format!(
        "DROP {}INDEX {}",
        index_type_keyword(index_field_type),
        index_target(entity_type, label, properties, 'e')?
    ))
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
        let err = generate_create_index_query(
            IndexType::Vector,
            EntityType::Node,
            "Item",
            &["embedding"],
            Some(&options),
        )
        .unwrap_err();
        assert!(matches!(err, FalkorDBError::ParamEncoding { .. }));
        assert!(err.to_string().contains("NUL byte"));
    }

    #[test]
    fn test_generate_create_index_query_rejects_non_identifier_key() {
        // An option key that is not a Cypher identifier is rejected rather than interpolated raw.
        let options = HashMap::from([("bad-key".to_string(), "4".to_string())]);
        let err = generate_create_index_query(
            IndexType::Vector,
            EntityType::Node,
            "Item",
            &["embedding"],
            Some(&options),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            FalkorDBError::InvalidIndexOption { ref key, .. } if key == "bad-key"
        ));
        // The key is surfaced in the error message.
        assert!(err.to_string().contains("'bad-key'"));
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
    fn test_generate_drop_index_query_formatting() {
        // One space after `DROP` and an uppercase `FOR`, for every index type and entity.
        let drop = |index_type, entity_type, label, properties: &[&str]| {
            generate_drop_index_query(index_type, entity_type, label, properties).unwrap()
        };
        assert_eq!(
            drop(IndexType::Range, EntityType::Node, "Person", &["name"]),
            "DROP INDEX FOR (e:Person) ON (e.name)"
        );
        assert_eq!(
            drop(IndexType::Range, EntityType::Edge, "KNOWS", &["since"]),
            "DROP INDEX FOR ()-[e:KNOWS]->() ON (e.since)"
        );
        assert_eq!(
            drop(IndexType::Vector, EntityType::Node, "Item", &["embedding"]),
            "DROP VECTOR INDEX FOR (e:Item) ON (e.embedding)"
        );
        assert_eq!(
            drop(
                IndexType::Fulltext,
                EntityType::Node,
                "Document",
                &["content"]
            ),
            "DROP FULLTEXT INDEX FOR (e:Document) ON (e.content)"
        );
        assert_eq!(
            drop(
                IndexType::Range,
                EntityType::Node,
                "Person",
                &["firstName", "lastName"]
            ),
            "DROP INDEX FOR (e:Person) ON (e.firstName, e.lastName)"
        );
    }

    #[test]
    fn test_index_queries_quote_names_that_are_not_identifiers() {
        let query = generate_create_index_query(
            IndexType::Range,
            EntityType::Node,
            "Person Of Interest",
            &["first name", "age"],
            None,
        )
        .unwrap();
        assert_eq!(
            query,
            "CREATE INDEX FOR (l:`Person Of Interest`) ON (l.`first name`, l.age)"
        );
        let query = generate_drop_index_query(
            IndexType::Fulltext,
            EntityType::Edge,
            "WORKS WITH",
            &["start date"],
        )
        .unwrap();
        assert_eq!(
            query,
            "DROP FULLTEXT INDEX FOR ()-[e:`WORKS WITH`]->() ON (e.`start date`)"
        );

        // The issue's payloads stay inside the names instead of adding to the query.
        let query = generate_create_index_query(
            IndexType::Range,
            EntityType::Node,
            "Person) RETURN 1 //",
            &["name) RETURN 1 //"],
            None,
        )
        .unwrap();
        assert_eq!(
            query,
            "CREATE INDEX FOR (l:`Person) RETURN 1 //`) ON (l.`name) RETURN 1 //`)"
        );

        // FalkorDB reads backslashes and quotes literally inside backticks, and accepts keywords as
        // bare names.
        let query = generate_drop_index_query(
            IndexType::Range,
            EntityType::Node,
            "back\\",
            &["it's", "MATCH"],
        )
        .unwrap();
        assert_eq!(query, "DROP INDEX FOR (e:`back\\`) ON (e.`it's`, e.MATCH)");
    }

    #[test]
    fn test_index_queries_reject_names_that_cannot_be_quoted() {
        let generators: [fn(EntityType, &str, &str) -> FalkorDBError; 2] = [
            |entity_type, label, property| {
                generate_create_index_query(IndexType::Range, entity_type, label, &[property], None)
                    .unwrap_err()
            },
            |entity_type, label, property| {
                generate_drop_index_query(IndexType::Range, entity_type, label, &[property])
                    .unwrap_err()
            },
        ];
        let backtick = "contains a backtick, which FalkorDB cannot encode";
        let nul = "contains a NUL byte, which FalkorDB cannot encode";
        let cases = [
            (EntityType::Node, "a`b", "name", "label", "a`b", backtick),
            (
                EntityType::Edge,
                "a`b",
                "name",
                "relationship type",
                "a`b",
                backtick,
            ),
            (EntityType::Node, "", "name", "label", "", "is empty"),
            (
                EntityType::Node,
                "Person",
                "x` RETURN 1 //",
                "property",
                "x` RETURN 1 //",
                backtick,
            ),
            (EntityType::Node, "Person", "a\0b", "property", "a\0b", nul),
            (EntityType::Edge, "KNOWS", "", "property", "", "is empty"),
        ];
        for generate in generators {
            for (entity_type, label, property, kind, name, message) in cases {
                assert_eq!(
                    generate(entity_type, label, property),
                    FalkorDBError::InvalidIdentifier {
                        kind,
                        name: name.to_string(),
                        message,
                    }
                );
            }
        }
        assert_eq!(
            generators[0](EntityType::Node, "a`b", "name").to_string(),
            "invalid label name 'a`b': contains a backtick, which FalkorDB cannot encode"
        );
    }

    #[test]
    fn test_index_queries_reject_empty_properties() {
        let no_properties: &[&str] = &[];
        assert_eq!(
            generate_create_index_query(
                IndexType::Range,
                EntityType::Node,
                "Person",
                no_properties,
                None
            ),
            Err(FalkorDBError::EmptyIndexProperties)
        );
        assert_eq!(
            generate_drop_index_query(IndexType::Vector, EntityType::Edge, "KNOWS", no_properties),
            Err(FalkorDBError::EmptyIndexProperties)
        );
    }
}
