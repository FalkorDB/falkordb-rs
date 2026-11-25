/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! The schema element FalkorDB 6.2.0 and later append to a compact `GRAPH.QUERY` or
//! `GRAPH.RO_QUERY` reply when the request carries `SCHEMA_HASH h` (FalkorDB#3757).
//!
//! The element is always the reply's last top-level value: `{hash: S}` when `h` is the graph's
//! current schema hash `S`, otherwise `{hash: S, labels, relationshipTypes, propertyKeys}`, each
//! list complete and in id order. RESP3 sends it as a map, RESP2 as a flat `[key, value, ...]`
//! array. Older servers ignore the argument and send no element.

use super::GraphSchema;
use crate::{
    parser::{redis_value_as_int, redis_value_as_string, redis_value_as_vec},
    FalkorDBError, FalkorResult,
};

/// The three name lists of a graph, each in id order.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct SchemaNames {
    pub(crate) labels: Vec<String>,
    pub(crate) relationship_types: Vec<String>,
    pub(crate) property_keys: Vec<String>,
}

/// A reply's schema element: the graph's schema hash, and its names when the request's hash was
/// not current.
#[derive(Debug, PartialEq)]
pub(crate) struct SchemaElement {
    pub(crate) hash: i64,
    pub(crate) names: Option<SchemaNames>,
}

fn is_hash_key(value: &redis::Value) -> bool {
    match value {
        redis::Value::BulkString(bytes) => bytes == b"hash",
        redis::Value::SimpleString(text) => text == "hash",
        _ => false,
    }
}

/// Whether `value` is a schema element rather than the statistics that end every other reply. In
/// RESP2 the element is an array of pairs starting with the key `hash`, and no statistics line is
/// ever that bare word.
fn is_schema_element(value: &redis::Value) -> bool {
    match value {
        redis::Value::Map(pairs) => pairs.iter().any(|(key, _)| is_hash_key(key)),
        redis::Value::Array(items) => {
            items.len() % 2 == 0
                && matches!(items.as_slice(), [key, redis::Value::Int(_), ..] if is_hash_key(key))
        }
        _ => false,
    }
}

fn names_list(value: redis::Value) -> FalkorResult<Vec<String>> {
    redis_value_as_vec(value)?
        .into_iter()
        .map(redis_value_as_string)
        .collect()
}

fn parse_schema_element(value: redis::Value) -> FalkorResult<SchemaElement> {
    let malformed = |what: &str| FalkorDBError::ParsingError(format!("schema element: {what}"));
    let mut hash = None;
    let (mut labels, mut relationship_types, mut property_keys) = (None, None, None);
    for (key, value) in value
        .into_map_iter()
        .map_err(|_| malformed("expected a map"))?
    {
        match redis_value_as_string(key)?.as_str() {
            "hash" => hash = Some(redis_value_as_int(value)?),
            "labels" => labels = Some(names_list(value)?),
            "relationshipTypes" => relationship_types = Some(names_list(value)?),
            "propertyKeys" => property_keys = Some(names_list(value)?),
            // A newer server may add keys; they don't change how this reply decodes.
            _ => {}
        }
    }
    let hash = hash.ok_or_else(|| malformed("no hash"))?;
    let names = match (labels, relationship_types, property_keys) {
        (None, None, None) => None,
        (Some(labels), Some(relationship_types), Some(property_keys)) => Some(SchemaNames {
            labels,
            relationship_types,
            property_keys,
        }),
        _ => return Err(malformed("some name lists are missing")),
    };
    Ok(SchemaElement { hash, names })
}

/// Removes the schema element from the end of a reply's top-level array and parses it. `None`
/// means the reply has none: a server older than FalkorDB 6.2.0, or a request without
/// `SCHEMA_HASH`. The rest of the reply then has the shape it always had.
pub(crate) fn take_schema_element(
    res: &mut Vec<redis::Value>
) -> FalkorResult<Option<SchemaElement>> {
    match res.pop_if(|last| is_schema_element(last)) {
        Some(element) => parse_schema_element(element).map(Some),
        None => Ok(None),
    }
}

/// The schema one reply decodes with, and whether it brought names the cache may not have.
pub(crate) struct ReplySchema {
    pub(crate) schema: GraphSchema,
    pub(crate) new_names: bool,
}

/// Picks the schema a reply decodes with. `sent` is the schema whose hash the request carried.
///
/// - Names in the reply: they are what the reply's ids refer to, including names the query
///   added, whatever the cache holds by now.
/// - The hash only, equal to the one sent: the names sent are current.
/// - The hash only, and it is 0: the graph has no names yet. This also covers a request that sent
///   0 because its names came from a refresh, which carries no hash.
///
/// In every case the schema is pinned: an id it lacks is an error, never a reason to refresh.
pub(crate) fn reply_schema(
    sent: &GraphSchema,
    element: SchemaElement,
) -> FalkorResult<ReplySchema> {
    let SchemaElement { hash, names } = element;
    match names {
        Some(names) => Ok(ReplySchema {
            schema: GraphSchema::from_reply_names(&sent.graph_name, hash, names),
            new_names: true,
        }),
        None if sent.hash == Some(hash) => Ok(ReplySchema {
            schema: GraphSchema {
                pinned: true,
                ..sent.snapshot()
            },
            new_names: false,
        }),
        None if hash == 0 => Ok(ReplySchema {
            schema: GraphSchema::from_reply_names(&sent.graph_name, 0, SchemaNames::default()),
            new_names: true,
        }),
        None => Err(FalkorDBError::ParsingError(format!(
            "schema element: hash {hash} came without names, but the request sent {}",
            sent.request_hash()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{client::blocking::create_empty_inner_sync_client, SchemaType};

    fn bulk(text: &str) -> redis::Value {
        redis::Value::BulkString(text.as_bytes().to_vec())
    }

    fn list(names: &[&str]) -> redis::Value {
        redis::Value::Array(names.iter().map(|name| bulk(name)).collect())
    }

    fn names(
        labels: &[&str],
        relationship_types: &[&str],
        property_keys: &[&str],
    ) -> SchemaNames {
        let owned = |names: &[&str]| names.iter().map(|name| name.to_string()).collect();
        SchemaNames {
            labels: owned(labels),
            relationship_types: owned(relationship_types),
            property_keys: owned(property_keys),
        }
    }

    /// The element FalkorDB sends for `(:Person:City)-[:LIVES_IN]` with keys `name, age`, whose
    /// hash is above `i64::MAX` and so arrives negative.
    const HASH: i64 = -1_146_456_808_972_410_223;

    fn resp2_full() -> redis::Value {
        redis::Value::Array(vec![
            bulk("hash"),
            redis::Value::Int(HASH),
            bulk("labels"),
            list(&["Person", "City"]),
            bulk("relationshipTypes"),
            list(&["LIVES_IN"]),
            bulk("propertyKeys"),
            list(&["name", "age"]),
        ])
    }

    fn resp3_full() -> redis::Value {
        redis::Value::Map(vec![
            (bulk("hash"), redis::Value::Int(HASH)),
            (bulk("labels"), list(&["Person", "City"])),
            (bulk("relationshipTypes"), list(&["LIVES_IN"])),
            (bulk("propertyKeys"), list(&["name", "age"])),
        ])
    }

    fn stats() -> redis::Value {
        list(&[
            "Nodes created: 1",
            "Query internal execution time: 0.1 milliseconds",
        ])
    }

    fn take(mut res: Vec<redis::Value>) -> (Vec<redis::Value>, Option<SchemaElement>) {
        let element = take_schema_element(&mut res).expect("parses");
        (res, element)
    }

    fn full_element() -> SchemaElement {
        SchemaElement {
            hash: HASH,
            names: Some(names(&["Person", "City"], &["LIVES_IN"], &["name", "age"])),
        }
    }

    #[test]
    fn takes_full_element_in_both_protocols() {
        for element in [resp2_full(), resp3_full()] {
            let (rest, taken) = take(vec![list(&["n"]), list(&[]), stats(), element]);
            assert_eq!(taken, Some(full_element()));
            assert_eq!(rest, vec![list(&["n"]), list(&[]), stats()]);
        }
    }

    #[test]
    fn takes_hash_only_element_in_both_protocols() {
        let expected = Some(SchemaElement {
            hash: HASH,
            names: None,
        });
        let resp2 = redis::Value::Array(vec![bulk("hash"), redis::Value::Int(HASH)]);
        let resp3 = redis::Value::Map(vec![(
            redis::Value::SimpleString("hash".to_string()),
            redis::Value::Int(HASH),
        )]);
        for element in [resp2, resp3] {
            // A write without RETURN: `[stats, element]`.
            let (rest, taken) = take(vec![stats(), element]);
            assert_eq!(taken, expected);
            assert_eq!(rest, vec![stats()]);
        }
    }

    #[test]
    fn empty_lists_are_names() {
        let element = redis::Value::Array(vec![
            bulk("hash"),
            redis::Value::Int(0),
            bulk("labels"),
            list(&[]),
            bulk("relationshipTypes"),
            list(&[]),
            bulk("propertyKeys"),
            list(&[]),
        ]);
        let (_, taken) = take(vec![stats(), element]);
        assert_eq!(
            taken,
            Some(SchemaElement {
                hash: 0,
                names: Some(SchemaNames::default())
            })
        );
    }

    #[test]
    fn unknown_keys_are_ignored() {
        let element = redis::Value::Map(vec![
            (bulk("future"), list(&["x"])),
            (bulk("hash"), redis::Value::Int(7)),
        ]);
        let (_, taken) = take(vec![stats(), element]);
        assert_eq!(
            taken,
            Some(SchemaElement {
                hash: 7,
                names: None
            })
        );
    }

    #[test]
    fn replies_without_an_element_are_left_alone() {
        // Every shape an older server sends, including `[header, stats]`, which has as many
        // elements as `[stats, element]`.
        for reply in [
            vec![stats()],
            vec![list(&["n"]), stats()],
            vec![list(&["n"]), list(&[]), stats()],
            vec![],
        ] {
            let (rest, taken) = take(reply.clone());
            assert_eq!(taken, None);
            assert_eq!(rest, reply);
        }
        // Statistics with an even number of lines are not mistaken for pairs, and neither is a
        // last value that is not a map or an array of pairs keyed by `hash`.
        for last in [
            list(&["hash", "Nodes created: 1"]),
            redis::Value::Array(vec![redis::Value::Int(1), redis::Value::Int(2)]),
            redis::Value::Int(1),
            redis::Value::Nil,
        ] {
            let (rest, taken) = take(vec![last.clone()]);
            assert_eq!(taken, None);
            assert_eq!(rest, vec![last]);
        }
    }

    #[test]
    fn malformed_elements_are_errors() {
        let malformed = [
            // The hash is not an integer.
            redis::Value::Map(vec![(bulk("hash"), bulk("7"))]),
            // A list holds something other than names.
            redis::Value::Map(vec![
                (bulk("hash"), redis::Value::Int(7)),
                (bulk("labels"), redis::Value::Int(1)),
                (bulk("relationshipTypes"), list(&[])),
                (bulk("propertyKeys"), list(&[])),
            ]),
            redis::Value::Map(vec![
                (bulk("hash"), redis::Value::Int(7)),
                (
                    bulk("labels"),
                    redis::Value::Array(vec![redis::Value::Int(1)]),
                ),
                (bulk("relationshipTypes"), list(&[])),
                (bulk("propertyKeys"), list(&[])),
            ]),
            // Only some of the lists.
            redis::Value::Map(vec![
                (bulk("hash"), redis::Value::Int(7)),
                (bulk("labels"), list(&["A"])),
            ]),
            // A key that is not a string.
            redis::Value::Map(vec![
                (bulk("hash"), redis::Value::Int(7)),
                (redis::Value::Int(1), list(&[])),
            ]),
        ];
        for element in malformed {
            let mut res = vec![stats(), element];
            assert!(take_schema_element(&mut res).is_err());
        }
        // No hash, or not a map at all.
        assert!(
            parse_schema_element(redis::Value::Map(vec![(bulk("labels"), list(&[]))])).is_err()
        );
        assert!(parse_schema_element(redis::Value::Int(1)).is_err());
    }

    fn cached(
        hash: i64,
        cached_names: SchemaNames,
    ) -> GraphSchema {
        let mut schema = GraphSchema::new("g", create_empty_inner_sync_client());
        schema.offer(&GraphSchema::from_reply_names("g", hash, cached_names));
        schema
    }

    fn label(
        schema: &mut GraphSchema,
        id: i64,
    ) -> FalkorResult<String> {
        schema.parse_single_id(id, SchemaType::Labels)
    }

    #[test]
    fn a_reply_with_names_decodes_with_them() {
        let sent = cached(1, names(&["Old"], &[], &[]));
        let mut reply = reply_schema(&sent, full_element()).expect("names");
        assert!(reply.new_names);
        assert_eq!(reply.schema.hash, Some(HASH));
        assert_eq!(label(&mut reply.schema, 0).unwrap(), "Person");
        assert_eq!(label(&mut reply.schema, 1).unwrap(), "City");
        assert_eq!(
            reply
                .schema
                .parse_single_id(0, SchemaType::Relationships)
                .unwrap(),
            "LIVES_IN"
        );
        assert_eq!(
            reply
                .schema
                .parse_single_id(1, SchemaType::Properties)
                .unwrap(),
            "age"
        );
    }

    #[test]
    fn a_hash_only_reply_decodes_with_the_names_sent() {
        let sent = cached(5, names(&["Sent"], &[], &[]));
        let mut reply = reply_schema(
            &sent,
            SchemaElement {
                hash: 5,
                names: None,
            },
        )
        .expect("current");
        assert!(!reply.new_names);
        assert_eq!(label(&mut reply.schema, 0).unwrap(), "Sent");
    }

    #[test]
    fn a_zero_hash_without_names_is_an_empty_schema() {
        // The request sent 0 because its names came from a refresh; the graph has none now.
        let mut sent = GraphSchema::new("g", create_empty_inner_sync_client());
        sent.labels = std::sync::Arc::new([(0, "Refreshed".to_string())].into());
        assert_eq!(sent.request_hash(), 0);
        let mut reply = reply_schema(
            &sent,
            SchemaElement {
                hash: 0,
                names: None,
            },
        )
        .expect("empty");
        assert!(reply.new_names);
        assert_eq!(reply.schema.hash, Some(0));
        assert!(matches!(
            label(&mut reply.schema, 0),
            Err(FalkorDBError::MissingSchemaId(SchemaType::Labels))
        ));
    }

    #[test]
    fn a_hash_only_reply_for_another_hash_is_an_error() {
        let sent = cached(5, names(&["Sent"], &[], &[]));
        assert!(reply_schema(
            &sent,
            SchemaElement {
                hash: 6,
                names: None,
            },
        )
        .is_err());
    }

    #[test]
    fn a_reply_schema_never_refreshes() {
        // `cached` has a client, so a refresh would try to reach the server and fail with a
        // connection error; a pinned schema reports the missing id without trying.
        let sent = cached(5, names(&["Sent"], &[], &[]));
        for element in [
            SchemaElement {
                hash: 5,
                names: None,
            },
            full_element(),
        ] {
            let mut reply = reply_schema(&sent, element).expect("schema");
            assert!(matches!(
                label(&mut reply.schema, 9),
                Err(FalkorDBError::MissingSchemaId(SchemaType::Labels))
            ));
            assert!(reply.schema.take_missing().is_empty());
        }
    }

    /// The cache after offering `reply` names (under `reply_hash`) to a cache holding `cached`.
    fn after_offer(
        cached_entry: Option<(i64, SchemaNames)>,
        reply_hash: i64,
        reply: SchemaNames,
    ) -> GraphSchema {
        let mut cache = match cached_entry {
            Some((hash, cached_names)) => cached(hash, cached_names),
            None => GraphSchema::new("g", create_empty_inner_sync_client()),
        };
        cache.offer(&GraphSchema::from_reply_names("g", reply_hash, reply));
        cache
    }

    #[test]
    fn offer_stores_into_an_empty_cache() {
        let mut cache = after_offer(None, 1, names(&["A"], &[], &[]));
        assert_eq!(cache.hash, Some(1));
        assert_eq!(label(&mut cache, 0).unwrap(), "A");
    }

    #[test]
    fn offer_replaces_names_from_a_refresh() {
        let mut cache = GraphSchema::new("g", create_empty_inner_sync_client());
        cache.labels = std::sync::Arc::new([(0, "Refreshed".to_string())].into());
        cache.offer(&GraphSchema::from_reply_names(
            "g",
            1,
            names(&["Refreshed"], &[], &[]),
        ));
        assert_eq!(cache.hash, Some(1));
    }

    #[test]
    fn offer_keeps_the_same_hash() {
        let cache = cached(1, names(&["A"], &[], &[]));
        let before = std::sync::Arc::clone(&cache.labels);
        let mut cache = cache;
        cache.offer(&GraphSchema::from_reply_names(
            "g",
            1,
            names(&["A"], &[], &[]),
        ));
        assert!(std::sync::Arc::ptr_eq(&before, &cache.labels));
    }

    #[test]
    fn offer_takes_a_grown_schema() {
        let mut cache = after_offer(
            Some((1, names(&["A"], &[], &["k"]))),
            2,
            names(&["A", "B"], &["R"], &["k"]),
        );
        assert_eq!(cache.hash, Some(2));
        assert_eq!(label(&mut cache, 1).unwrap(), "B");
    }

    #[test]
    fn offer_keeps_a_newer_schema_than_the_reply() {
        // A replica that is behind, or two replies that crossed.
        let mut cache = after_offer(
            Some((2, names(&["A", "B"], &["R"], &["k"]))),
            1,
            names(&["A"], &[], &["k"]),
        );
        assert_eq!(cache.hash, Some(2));
        assert_eq!(label(&mut cache, 1).unwrap(), "B");
    }

    #[test]
    fn offer_takes_a_recreated_schema() {
        let mut cache = after_offer(
            Some((2, names(&["Person"], &[], &["name"]))),
            3,
            names(&["Movie"], &[], &["title"]),
        );
        assert_eq!(cache.hash, Some(3));
        assert_eq!(label(&mut cache, 0).unwrap(), "Movie");
        // Also when the new lists are shorter.
        let mut cache = after_offer(
            Some((2, names(&["Person", "City"], &[], &[]))),
            3,
            names(&["Movie"], &[], &[]),
        );
        assert_eq!(label(&mut cache, 0).unwrap(), "Movie");
    }

    #[test]
    fn adopt_reply_offers_only_new_names() {
        let mut cache = cached(1, names(&["A"], &[], &[]));
        let before = std::sync::Arc::clone(&cache.labels);
        cache
            .adopt_reply(
                None,
                SchemaElement {
                    hash: 1,
                    names: None,
                },
            )
            .expect("current");
        assert!(std::sync::Arc::ptr_eq(&before, &cache.labels));

        let mut decoded = cache.adopt_reply(None, full_element()).expect("names");
        assert_eq!(label(&mut decoded, 0).unwrap(), "Person");
        assert_eq!(cache.hash, Some(HASH));
        assert_eq!(label(&mut cache, 0).unwrap(), "Person");
    }

    #[test]
    fn adopt_reply_decodes_with_the_schema_sent_after_the_cache_moved_on() {
        // A batch sends one hash for every query. When an earlier reply moved the cache on, a
        // later hash-only reply still decodes with the names that were sent.
        let mut cache = cached(1, names(&["Sent"], &[], &[]));
        let sent = cache.snapshot();
        cache.offer(&GraphSchema::from_reply_names(
            "g",
            2,
            names(&["Sent", "Added"], &[], &[]),
        ));
        let mut decoded = cache
            .adopt_reply(
                Some(&sent),
                SchemaElement {
                    hash: 1,
                    names: None,
                },
            )
            .expect("current for the request");
        assert_eq!(decoded.hash, Some(1));
        assert!(matches!(
            label(&mut decoded, 1),
            Err(FalkorDBError::MissingSchemaId(SchemaType::Labels))
        ));
        assert_eq!(cache.hash, Some(2));
    }

    #[test]
    fn a_refresh_drops_the_hash() {
        let mut cache = cached(1, names(&["A"], &[], &[]));
        let refresh_reply = redis::Value::Array(vec![
            list(&["label"]),
            redis::Value::Array(vec![list(&["B"])]),
            stats(),
        ]);
        cache
            .apply_refresh_reply(SchemaType::Labels, refresh_reply)
            .expect("applies");
        assert_eq!(cache.hash, None);
        assert_eq!(cache.request_hash(), 0);
    }
}
