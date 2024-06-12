/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::redis_ext::redis_value_as_string;
use crate::{EntityType, FalkorDBError, FalkorResult};

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
    pub(crate) fn parse(value: redis::Value) -> FalkorResult<Self> {
        let [constraint_type_raw, label_raw, properties_raw, entity_type_raw, status_raw]: [redis::Value; 5] = value.into_sequence()
            .map_err(|_| FalkorDBError::ParsingArray)
            .and_then(|res| res.try_into()
                .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount("Expected exactly 5 elements in constraint object")))?;

        Ok(Self {
            constraint_type: ConstraintType::try_from(
                redis_value_as_string(constraint_type_raw)?.as_str(),
            )?,
            label: redis_value_as_string(label_raw)?,
            properties: properties_raw
                .into_sequence()
                .map(|data| data.into_iter().flat_map(redis_value_as_string).collect())
                .map_err(|_| FalkorDBError::ParsingArray)?,
            entity_type: EntityType::try_from(redis_value_as_string(entity_type_raw)?.as_str())?,
            status: ConstraintStatus::try_from(redis_value_as_string(status_raw)?.as_str())?,
        })
    }
}
