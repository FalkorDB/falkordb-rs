/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
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
    /// * `headers`: a [`redis::Value`] that is expected to be of variant [`redis::Value::Bulk`], where each element is expected to be of variant [`redis::Value::Data`] or [`redis::Value::Status`]
    /// * `data`: The actual data
    /// * `stats`: a [`redis::Value`] that is expected to be of variant [`redis::Value::Bulk`], where each element is expected to be of variant [`redis::Value::Data`] or [`redis::Value::Status`]
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
    use crate::test_utils::open_test_graph;

    #[test]
    fn test_get_statistics() {
        let mut graph = open_test_graph("imdb_stats_test");
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
}
