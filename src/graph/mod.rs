/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{EntityType, GraphSchema, IndexType};
use std::{collections::HashMap, fmt::Display};

pub(crate) mod blocking;
pub(crate) mod query_builder;

#[cfg(feature = "tokio")]
pub(crate) mod asynchronous;

pub trait HasGraphSchema {
    fn get_graph_schema_mut(&mut self) -> &mut GraphSchema;
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
            hashmap
                .iter()
                .map(|(key, val)| format!("'{key}':'{val}'"))
                .collect::<Vec<_>>()
                .join(",")
        })
        .map(|options_string| format!(" OPTIONS {{ {} }}", options_string))
        .unwrap_or_default();

    format!(
        "CREATE {idx_type}INDEX FOR {pattern} ON ({}){}",
        properties_string, options_string
    )
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
        let query = generate_drop_index_query(
            IndexType::Range,
            EntityType::Node,
            "Person",
            &["name"],
        );
        assert!(query.contains("DROP"));
        assert!(query.contains("INDEX"));
        assert!(query.contains("(e:Person)"));
        assert!(query.contains("e.name"));
    }

    #[test]
    fn test_generate_drop_index_query_range_edge() {
        let query = generate_drop_index_query(
            IndexType::Range,
            EntityType::Edge,
            "KNOWS",
            &["since"],
        );
        assert!(query.contains("DROP"));
        assert!(query.contains("INDEX"));
        assert!(query.contains("()-[e:KNOWS]->()"));
        assert!(query.contains("e.since"));
    }

    #[test]
    fn test_generate_drop_index_query_vector() {
        let query = generate_drop_index_query(
            IndexType::Vector,
            EntityType::Node,
            "Item",
            &["embedding"],
        );
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
