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
        .map(|element| format!("l.{element}"))
        .collect::<Vec<_>>()
        .join(", ");

    let pattern = match entity_type {
        EntityType::Node => format!("(l:{label})"),
        EntityType::Edge => format!("()-[l:{label}]->()"),
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
        .map(|options_string| format!(" OPTIONS {{ {options_string} }}"))
        .unwrap_or_default();

    format!("CREATE {idx_type}INDEX FOR {pattern} ON ({properties_string}){options_string}")
}

pub(crate) fn generate_drop_index_query<P: Display>(
    index_field_type: IndexType,
    entity_type: EntityType,
    label: &str,
    properties: &[P],
) -> String {
    let properties_string = properties
        .iter()
        .map(|element| format!("e.{element}"))
        .collect::<Vec<_>>()
        .join(", ");

    let pattern = match entity_type {
        EntityType::Node => format!("(e:{label})"),
        EntityType::Edge => format!("()-[e:{label}]->()"),
    };

    let idx_type = match index_field_type {
        IndexType::Range => "",
        IndexType::Vector => "VECTOR",
        IndexType::Fulltext => "FULLTEXT",
    }
    .to_string();

    format!("DROP {idx_type} INDEX for {pattern} ON ({properties_string})")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_create_index_query_node_fulltext() {
        let query = generate_create_index_query(
            IndexType::Fulltext,
            EntityType::Node,
            "actor",
            &["Hello"],
            None,
        );
        assert_eq!(query, "CREATE FULLTEXT INDEX FOR (l:actor) ON (l.Hello)");
    }

    #[test]
    fn test_generate_create_index_query_edge_range() {
        let query = generate_create_index_query(
            IndexType::Range,
            EntityType::Edge,
            "follows",
            &["since", "weight"],
            None,
        );
        assert_eq!(
            query,
            "CREATE INDEX FOR ()-[l:follows]->() ON (l.since, l.weight)"
        );
    }

    #[test]
    fn test_generate_drop_index_query() {
        let query =
            generate_drop_index_query(IndexType::Fulltext, EntityType::Node, "actor", &["Hello"]);
        assert_eq!(query, "DROP FULLTEXT INDEX for (e:actor) ON (e.Hello)");
    }
}
