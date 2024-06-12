/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    value::utils::parse_raw_redis_value, EntityType, FalkorDBError, FalkorResult, FalkorValue,
    GraphSchema,
};

/// The type of restriction to apply for the property
#[derive(Copy, Clone, Debug, Eq, PartialEq, strum::EnumString, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ConstraintType {
    /// This property may only appear once on entities of this type and label.
    Unique,
    /// This property must be provided when creating a new entity of this type and label,
    /// and must exist on all entities of this type and label.
    Mandatory,
}

/// The status of this constraint
#[derive(Copy, Clone, Debug, Eq, PartialEq, strum::EnumString, strum::Display)]
pub enum ConstraintStatus {
    /// This constraint is active on all entities of this type and label.
    #[strum(serialize = "OPERATIONAL")]
    Active,
    /// This constraint is still being applied and verified.
    #[strum(serialize = "UNDER CONSTRUCTION")]
    Pending,
    /// This constraint could not be applied, not all entities of this type and label are compliant.
    Failed,
}

/// A constraint applied on all 'properties' of the graph entity 'label' in this graph
#[derive(Clone, Debug, PartialEq)]
pub struct Constraint {
    /// Is this constraint applies the 'unique' modifier or the 'mandatory' modifier
    pub constraint_type: ConstraintType,
    /// The name of this constraint
    pub label: String,
    /// The properties this constraint applies to
    pub properties: Vec<String>,
    /// Whether this constraint applies to nodes or relationships
    pub entity_type: EntityType,
    /// Whether this constraint status is already active, still under construction, or failed construction
    pub status: ConstraintStatus,
}

impl Constraint {
    pub(crate) fn parse(
        value: redis::Value,
        graph_schema: &mut GraphSchema,
    ) -> FalkorResult<Self> {
        let [constraint_type_raw, label_raw, properties_raw, entity_type_raw, status_raw]: [redis::Value; 5] = value.into_sequence()
            .map_err(|_| FalkorDBError::ParsingArray)
            .and_then(|res| res.try_into()
                .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount("Expected exactly 5 elements in constraint object")))?;

        Ok(Self {
            constraint_type: ConstraintType::try_from(
                parse_raw_redis_value(constraint_type_raw, graph_schema)
                    .and_then(|parsed_constraint_type| parsed_constraint_type.into_string())?
                    .as_str(),
            )?,
            label: parse_raw_redis_value(label_raw, graph_schema)
                .and_then(FalkorValue::into_string)?,
            properties: parse_raw_redis_value(properties_raw, graph_schema)
                .and_then(|properties_parsed| properties_parsed.into_vec())
                .map(|properties_vec| {
                    properties_vec
                        .into_iter()
                        .flat_map(FalkorValue::into_string)
                        .collect()
                })?,
            entity_type: EntityType::try_from(
                parse_raw_redis_value(entity_type_raw, graph_schema)
                    .and_then(|parsed_entity_type| parsed_entity_type.into_string())?
                    .as_str(),
            )?,
            status: ConstraintStatus::try_from(
                parse_raw_redis_value(status_raw, graph_schema)
                    .and_then(|parsed_status| parsed_status.into_string())?
                    .as_str(),
            )?,
        })
    }
}
