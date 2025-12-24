/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constraint_type_unique() {
        assert_eq!(ConstraintType::Unique.to_string(), "UNIQUE");
    }

    #[test]
    fn test_constraint_type_mandatory() {
        assert_eq!(ConstraintType::Mandatory.to_string(), "MANDATORY");
    }

    #[test]
    fn test_constraint_type_from_string() {
        use std::str::FromStr;
        assert_eq!(
            ConstraintType::from_str("UNIQUE").unwrap(),
            ConstraintType::Unique
        );
        assert_eq!(
            ConstraintType::from_str("MANDATORY").unwrap(),
            ConstraintType::Mandatory
        );
    }

    #[test]
    fn test_constraint_type_clone() {
        let ct = ConstraintType::Unique;
        let ct_clone = ct;
        assert_eq!(ct, ct_clone);
    }

    #[test]
    fn test_constraint_type_debug() {
        assert!(format!("{:?}", ConstraintType::Unique).contains("Unique"));
        assert!(format!("{:?}", ConstraintType::Mandatory).contains("Mandatory"));
    }

    #[test]
    fn test_constraint_status_active() {
        assert_eq!(ConstraintStatus::Active.to_string(), "OPERATIONAL");
    }

    #[test]
    fn test_constraint_status_pending() {
        assert_eq!(ConstraintStatus::Pending.to_string(), "UNDER CONSTRUCTION");
    }

    #[test]
    fn test_constraint_status_failed() {
        assert_eq!(ConstraintStatus::Failed.to_string(), "Failed");
    }

    #[test]
    fn test_constraint_status_clone() {
        let status = ConstraintStatus::Active;
        let status_clone = status;
        assert_eq!(status, status_clone);
    }

    #[test]
    fn test_constraint_status_debug() {
        assert!(format!("{:?}", ConstraintStatus::Active).contains("Active"));
        assert!(format!("{:?}", ConstraintStatus::Pending).contains("Pending"));
        assert!(format!("{:?}", ConstraintStatus::Failed).contains("Failed"));
    }

    #[test]
    fn test_constraint_clone() {
        let constraint = Constraint {
            constraint_type: ConstraintType::Unique,
            label: "Person".to_string(),
            properties: vec!["id".to_string()],
            entity_type: EntityType::Node,
            status: ConstraintStatus::Active,
        };

        let constraint_clone = constraint.clone();
        assert_eq!(constraint, constraint_clone);
    }

    #[test]
    fn test_constraint_debug() {
        let constraint = Constraint {
            constraint_type: ConstraintType::Mandatory,
            label: "User".to_string(),
            properties: vec!["email".to_string()],
            entity_type: EntityType::Node,
            status: ConstraintStatus::Pending,
        };

        let debug_str = format!("{:?}", constraint);
        assert!(debug_str.contains("Mandatory"));
        assert!(debug_str.contains("User"));
        assert!(debug_str.contains("email"));
    }
}
