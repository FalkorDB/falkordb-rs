/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    parser::{
        parse_falkor_enum, redis_value_as_typed_string, redis_value_as_typed_string_vec,
        redis_value_as_vec, SchemaParsable,
    },
    EntityType, FalkorDBError, FalkorResult, GraphSchema,
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

impl SchemaParsable for Constraint {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Constraint", skip_all, level = "info")
    )]
    fn parse(
        value: redis::Value,
        _: &mut GraphSchema,
    ) -> FalkorResult<Self> {
        let [constraint_type_raw, label_raw, properties_raw, entity_type_raw, status_raw]: [redis::Value; 5] = redis_value_as_vec(value)
            .and_then(|res| res.try_into()
                .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount("Expected exactly 5 elements in constraint object")))?;

        Ok(Self {
            constraint_type: parse_falkor_enum(constraint_type_raw)?,
            label: redis_value_as_typed_string(label_raw)?,
            properties: redis_value_as_typed_string_vec(properties_raw)?,
            entity_type: parse_falkor_enum(entity_type_raw)?,
            status: parse_falkor_enum(status_raw)?,
        })
    }
}
