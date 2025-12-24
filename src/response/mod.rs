/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    parser::{parse_header, redis_value_as_untyped_string_vec},
    FalkorResult,
};
use std::str::FromStr;

pub(crate) mod constraint;
pub(crate) mod execution_plan;
pub(crate) mod index;
pub(crate) mod lazy_result_set;
pub(crate) mod slowlog_entry;

#[derive(Copy, Clone, Debug, Eq, PartialEq, strum::IntoStaticStr)]
enum StatisticType {
    #[strum(serialize = "Labels added")]
    LabelsAdded,
    #[strum(serialize = "Labels removed")]
    LabelsRemoved,
    #[strum(serialize = "Nodes created")]
    NodesCreated,
    #[strum(serialize = "Nodes deleted")]
    NodesDeleted,
    #[strum(serialize = "Properties set")]
    PropertiesSet,
    #[strum(serialize = "Properties removed")]
    PropertiesRemoved,
    #[strum(serialize = "Indices created")]
    IndicesCreated,
    #[strum(serialize = "Indices deleted")]
    IndicesDeleted,
    #[strum(serialize = "Relationships created")]
    RelationshipsCreated,
    #[strum(serialize = "Relationships deleted")]
    RelationshipsDeleted,
    #[strum(serialize = "Cached execution")]
    CachedExecution,
    #[strum(serialize = "internal execution time")]
    InternalExecutionTime,
}

/// A response struct which also contains the returned header and stats data
#[derive(Clone, Debug, Default)]
pub struct QueryResult<T> {
    /// Header for the result data, usually contains the scalar aliases for the columns
    pub header: Vec<String>,
    /// The actual data returned from the database
    pub data: T,
    /// Various statistics regarding the request, such as execution time and number of successful operations
    pub stats: Vec<String>,
}

impl<T> QueryResult<T> {
    /// Creates a [`QueryResult`] from the specified data, and raw stats, where raw headers are optional
    ///
    /// # Arguments
    /// * `headers`: a [`redis::Value`] that is expected to be of variant [`redis::Value::Array`], where each element is expected to be of variant [`redis::Value::BulkString`], [`redis::Value::VerbatimString`] or [`redis::Value::SimpleString`]
    /// * `data`: The actual data
    /// * `stats`: a [`redis::Value`] that is expected to be of variant [`redis::Value::Array`], where each element is expected to be of variant [`redis::Value::BulkString`], [`redis::Value::VerbatimString`] or [`redis::Value::SimpleString`]
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "New Falkor Response", skip_all, level = "trace")
    )]
    pub fn from_response(
        headers: Option<redis::Value>,
        data: T,
        stats: redis::Value,
    ) -> FalkorResult<Self> {
        Ok(Self {
            header: match headers {
                Some(headers) => parse_header(headers)?,
                None => vec![],
            },
            data,
            stats: redis_value_as_untyped_string_vec(stats)?,
        })
    }

    fn get_statistics<S>(
        &self,
        stat_type: StatisticType,
    ) -> Option<S>
    where
        S: FromStr,
    {
        for stat in self.stats.iter() {
            if stat.contains(Into::<&'static str>::into(stat_type)) {
                // Splits the statistic string by ': ', then retrieves and parses the statistic value.
                return stat
                    .split(": ")
                    .nth(1)
                    .and_then(|stat_value| stat_value.split(' ').next())
                    .and_then(|res| res.parse().ok());
            }
        }

        None
    }

    /// Returns the number of labels added in this query
    pub fn get_labels_added(&self) -> Option<i64> {
        self.get_statistics(StatisticType::LabelsAdded)
    }

    /// Returns the number of labels removed in this query
    pub fn get_labels_removed(&self) -> Option<i64> {
        self.get_statistics(StatisticType::LabelsRemoved)
    }

    /// Returns the number of nodes created in this query
    pub fn get_nodes_created(&self) -> Option<i64> {
        self.get_statistics(StatisticType::NodesCreated)
    }

    /// Returns the number of nodes deleted in this query
    pub fn get_nodes_deleted(&self) -> Option<i64> {
        self.get_statistics(StatisticType::NodesDeleted)
    }

    /// Returns the number of properties set in this query
    pub fn get_properties_set(&self) -> Option<i64> {
        self.get_statistics(StatisticType::PropertiesSet)
    }

    /// Returns the number of properties removed in this query
    pub fn get_properties_removed(&self) -> Option<i64> {
        self.get_statistics(StatisticType::PropertiesRemoved)
    }

    /// Returns the number of indices created in this query
    pub fn get_indices_created(&self) -> Option<i64> {
        self.get_statistics(StatisticType::IndicesCreated)
    }

    /// Returns the number of indices deleted in this query
    pub fn get_indices_deleted(&self) -> Option<i64> {
        self.get_statistics(StatisticType::IndicesDeleted)
    }

    /// Returns the number of relationships created in this query
    pub fn get_relationship_created(&self) -> Option<i64> {
        self.get_statistics(StatisticType::RelationshipsCreated)
    }

    /// Returns the number of relationships deleted in this query
    pub fn get_relationship_deleted(&self) -> Option<i64> {
        self.get_statistics(StatisticType::RelationshipsDeleted)
    }

    /// Returns whether this query was ran from cache
    pub fn get_cached_execution(&self) -> Option<bool> {
        self.get_statistics(StatisticType::CachedExecution)
            .map(|res: i64| res != 0)
    }

    /// Returns the internal execution time of this query
    pub fn get_internal_execution_time(&self) -> Option<f64> {
        self.get_statistics(StatisticType::InternalExecutionTime)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::open_empty_test_graph;

    #[test]
    fn test_get_statistics() {
        let mut graph = open_empty_test_graph("imdb_stats_test");
        {
            let query_result = graph
                .inner
                .query("CREATE (a:new_node { new_property: 1})-[b:new_relationship]->(a)")
                .execute()
                .expect("Could not run query");

            assert!(query_result.get_internal_execution_time().is_some());
            assert_eq!(query_result.get_nodes_created(), Some(1));
            assert_eq!(query_result.get_relationship_created(), Some(1));
            assert_eq!(query_result.get_properties_set(), Some(1));
        }
        {
            let query_result = graph
                .inner
                .query(
                    "MATCH (a:new_node { new_property: 1})-[b:new_relationship]->(a) DELETE b, a",
                )
                .execute()
                .expect("Could not run query");
            assert_eq!(query_result.get_nodes_deleted(), Some(1));
            assert_eq!(query_result.get_relationship_deleted(), Some(1));
        }

        {
            let query_result = graph
                .inner
                .query("UNWIND range(0, 1000) AS x RETURN x")
                .execute()
                .expect("Could not run query");
            assert_eq!(query_result.get_cached_execution(), Some(false));
        }

        {
            let query_result = graph
                .inner
                .query("UNWIND range(0, 1000) AS x RETURN x")
                .execute()
                .expect("Could not run query");
            assert_eq!(query_result.get_cached_execution(), Some(true));
        }
    }

    #[test]
    fn test_query_result_default() {
        let result: QueryResult<Vec<String>> = QueryResult::default();
        assert!(result.header.is_empty());
        assert!(result.data.is_empty());
        assert!(result.stats.is_empty());
    }

    #[test]
    fn test_query_result_clone() {
        let result = QueryResult {
            header: vec!["col1".to_string()],
            data: vec!["value1".to_string()],
            stats: vec!["Nodes created: 5".to_string()],
        };

        let result_clone = result.clone();
        assert_eq!(result.header, result_clone.header);
        assert_eq!(result.data, result_clone.data);
        assert_eq!(result.stats, result_clone.stats);
    }

    #[test]
    fn test_query_result_debug() {
        let result = QueryResult {
            header: vec!["name".to_string()],
            data: vec!["Alice".to_string()],
            stats: vec!["Query internal execution time: 0.5 milliseconds".to_string()],
        };

        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("name"));
        assert!(debug_str.contains("Alice"));
    }

    #[test]
    fn test_get_labels_added() {
        let result = QueryResult {
            header: vec![],
            data: (),
            stats: vec!["Labels added: 10".to_string()],
        };
        assert_eq!(result.get_labels_added(), Some(10));
    }

    #[test]
    fn test_get_labels_removed() {
        let result = QueryResult {
            header: vec![],
            data: (),
            stats: vec!["Labels removed: 5".to_string()],
        };
        assert_eq!(result.get_labels_removed(), Some(5));
    }

    #[test]
    fn test_get_nodes_created() {
        let result = QueryResult {
            header: vec![],
            data: (),
            stats: vec!["Nodes created: 20".to_string()],
        };
        assert_eq!(result.get_nodes_created(), Some(20));
    }

    #[test]
    fn test_get_nodes_deleted() {
        let result = QueryResult {
            header: vec![],
            data: (),
            stats: vec!["Nodes deleted: 8".to_string()],
        };
        assert_eq!(result.get_nodes_deleted(), Some(8));
    }

    #[test]
    fn test_get_properties_set() {
        let result = QueryResult {
            header: vec![],
            data: (),
            stats: vec!["Properties set: 15".to_string()],
        };
        assert_eq!(result.get_properties_set(), Some(15));
    }

    #[test]
    fn test_get_properties_removed() {
        let result = QueryResult {
            header: vec![],
            data: (),
            stats: vec!["Properties removed: 3".to_string()],
        };
        assert_eq!(result.get_properties_removed(), Some(3));
    }

    #[test]
    fn test_get_indices_created() {
        let result = QueryResult {
            header: vec![],
            data: (),
            stats: vec!["Indices created: 2".to_string()],
        };
        assert_eq!(result.get_indices_created(), Some(2));
    }

    #[test]
    fn test_get_indices_deleted() {
        let result = QueryResult {
            header: vec![],
            data: (),
            stats: vec!["Indices deleted: 1".to_string()],
        };
        assert_eq!(result.get_indices_deleted(), Some(1));
    }

    #[test]
    fn test_get_relationship_created() {
        let result = QueryResult {
            header: vec![],
            data: (),
            stats: vec!["Relationships created: 12".to_string()],
        };
        assert_eq!(result.get_relationship_created(), Some(12));
    }

    #[test]
    fn test_get_relationship_deleted() {
        let result = QueryResult {
            header: vec![],
            data: (),
            stats: vec!["Relationships deleted: 7".to_string()],
        };
        assert_eq!(result.get_relationship_deleted(), Some(7));
    }

    #[test]
    fn test_get_internal_execution_time() {
        let result = QueryResult {
            header: vec![],
            data: (),
            stats: vec!["Query internal execution time: 1.234 milliseconds".to_string()],
        };
        assert_eq!(result.get_internal_execution_time(), Some(1.234));
    }

    #[test]
    fn test_get_statistics_none() {
        let result: QueryResult<()> = QueryResult {
            header: vec![],
            data: (),
            stats: vec!["Some other stat: 100".to_string()],
        };
        assert_eq!(result.get_nodes_created(), None);
    }

    #[test]
    fn test_statistic_type_clone() {
        let stat = StatisticType::NodesCreated;
        let stat_clone = stat;
        assert_eq!(stat, stat_clone);
    }

    #[test]
    fn test_statistic_type_debug() {
        assert!(format!("{:?}", StatisticType::NodesCreated).contains("NodesCreated"));
    }

    #[test]
    fn test_statistic_type_into_static_str() {
        let s: &'static str = StatisticType::NodesCreated.into();
        assert_eq!(s, "Nodes created");

        let s: &'static str = StatisticType::LabelsAdded.into();
        assert_eq!(s, "Labels added");

        let s: &'static str = StatisticType::RelationshipsDeleted.into();
        assert_eq!(s, "Relationships deleted");
    }
}
