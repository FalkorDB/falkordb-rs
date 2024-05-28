/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorValue, SchemaType};
use anyhow::Result;
use std::collections::{HashMap, HashSet};

pub(crate) fn get_refresh_command(schema_type: SchemaType) -> &'static str {
    match schema_type {
        SchemaType::Labels => "DB.LABELS",
        SchemaType::Properties => "DB.PROPERTYKEYS",
        SchemaType::Relationships => "DB.RELATIONSHIPTYPES",
    }
}

pub(crate) fn update_map(
    map_to_update: &mut HashMap<i64, String>,
    keys: FalkorValue,
    id_hashset: Option<&HashSet<i64>>,
) -> Result<Option<HashMap<i64, String>>, FalkorDBError> {
    let new_keys = keys
        .into_vec()?
        .into_iter()
        .enumerate()
        .flat_map(|(idx, item)| {
            Result::<(i64, String), FalkorDBError>::Ok((
                idx as i64,
                item.into_vec()?
                    .into_iter()
                    .next()
                    .ok_or(FalkorDBError::ParsingError)
                    .and_then(|item| item.into_string())?,
            ))
        })
        .collect::<HashMap<i64, String>>();

    *map_to_update = new_keys;

    Ok(match id_hashset {
        None => None,
        Some(id_hashset) => get_relevant_hashmap(id_hashset, map_to_update),
    })
}

pub(crate) fn get_relevant_hashmap(
    id_set: &HashSet<i64>,
    locked_map: &HashMap<i64, String>,
) -> Option<HashMap<i64, String>> {
    let mut id_hashmap = HashMap::new();
    for id in id_set {
        if let Some(id_val) = locked_map.get(id).cloned() {
            id_hashmap.insert(*id, id_val);
            continue;
        }

        return None;
    }

    Some(id_hashmap)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_test_keys() -> FalkorValue {
        FalkorValue::FArray(vec![
            FalkorValue::FArray(vec![FalkorValue::FString("Hello".to_string())]),
            FalkorValue::FArray(vec![FalkorValue::FString("Iterator".to_string())]),
            FalkorValue::FArray(vec![FalkorValue::FString("My-".to_string())]),
            FalkorValue::FArray(vec![FalkorValue::FString("Panic".to_string())]),
        ])
    }

    #[test]
    fn test_update_map() {
        let mut map_to_update = HashMap::from([(5, "Ye Olde Value".to_string())]);
        let relevant_ids =
            update_map(&mut map_to_update, get_test_keys(), None).expect("Could not update map");

        assert!(relevant_ids.is_none());

        assert_eq!(map_to_update.get(&0), Some(&"Hello".to_string()));
        assert_eq!(map_to_update.get(&1), Some(&"Iterator".to_string()));
        assert_eq!(map_to_update.get(&2), Some(&"My-".to_string()));
        assert_eq!(map_to_update.get(&3), Some(&"Panic".to_string()));

        assert_eq!(map_to_update.get(&5), None);
    }

    #[test]
    fn test_update_map_with_relevant_hashmap() {
        let mut map_to_update = HashMap::new();
        let res = update_map(
            &mut map_to_update,
            get_test_keys(),
            Some(&HashSet::from([2, 3, 0])),
        );
        assert!(res.is_ok());

        let relevant_hashmap = res.unwrap();
        assert!(relevant_hashmap.is_some());

        let relevant_hashmap = relevant_hashmap.unwrap();
        assert_eq!(relevant_hashmap.get(&0), Some(&"Hello".to_string()));
        assert_eq!(relevant_hashmap.get(&2), Some(&"My-".to_string()));
        assert_eq!(relevant_hashmap.get(&3), Some(&"Panic".to_string()));

        assert_eq!(relevant_hashmap.get(&1), None);
    }

    #[test]
    fn test_update_no_relevant_ids_still_success() {
        let mut map_to_update = HashMap::new();
        let res = update_map(
            &mut map_to_update,
            get_test_keys(),
            Some(&HashSet::from([2, 5, 0])),
        );
        assert!(res.is_ok());

        let relevant_hashmap = res.unwrap();
        assert!(relevant_hashmap.is_none());
    }

    #[test]
    fn test_get_relevant_hashmap() {
        let hashset = HashSet::from([2, 1, 3, 0]);
        let locked_map = HashMap::from([
            (0, "Hello".to_string()),
            (1, "Darkness".to_string()),
            (2, "My".to_string()),
            (3, "Old".to_string()),
            (4, "Friend".to_string()),
        ]);
        let res = get_relevant_hashmap(&hashset, &locked_map);
        assert!(res.is_some());

        let relevant_hashmap = res.unwrap();
        assert_eq!(relevant_hashmap.get(&0), Some(&"Hello".to_string()));
        assert_eq!(relevant_hashmap.get(&1), Some(&"Darkness".to_string()));
        assert_eq!(relevant_hashmap.get(&2), Some(&"My".to_string()));
        assert_eq!(relevant_hashmap.get(&3), Some(&"Old".to_string()));

        // Was not in the requested hashset:
        assert_eq!(relevant_hashmap.get(&4), None);
    }

    #[test]
    fn test_no_relevant_hashmap() {
        let hashset = HashSet::from([2, 1, 5, 0]);
        let locked_map = HashMap::from([
            (0, "Hello".to_string()),
            (2, "My".to_string()),
            (5, "Old".to_string()),
            (4, "Friend".to_string()),
        ]);
        let res = get_relevant_hashmap(&hashset, &locked_map);
        assert!(res.is_none())
    }
}
