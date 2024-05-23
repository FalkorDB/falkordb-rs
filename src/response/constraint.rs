/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{
    connection::blocking::BorrowedSyncConnection, EntityType, FalkorDBError, FalkorParsable,
    FalkorValue, SyncGraphSchema,
};
use anyhow::Result;
use std::fmt::{Display, Formatter};

/// The type of restriction to apply for the property
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ConstraintType {
    /// This property may only appear once on entities of this type and label.
    Unique,
    /// This property must be provided when creating a new entity of this type and label,
    /// and must exist on all entities of this type and label.
    Mandatory,
}

impl Display for ConstraintType {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        let str = match self {
            ConstraintType::Unique => "UNIQUE",
            ConstraintType::Mandatory => "MANDATORY",
        };
        f.write_str(str)
    }
}

impl TryFrom<&str> for ConstraintType {
    type Error = FalkorDBError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value.to_uppercase().as_str() {
            "MANDATORY" => Self::Mandatory,
            "UNIQUE" => Self::Unique,
            _ => Err(FalkorDBError::ConstraintType)?,
        })
    }
}

impl TryFrom<String> for ConstraintType {
    type Error = FalkorDBError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<&String> for ConstraintType {
    type Error = FalkorDBError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

/// The status of this constraint
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ConstraintStatus {
    /// This constraint is active on all entities of this type and label.
    Active,
    /// This constraint is still being applied and verified.
    Pending,
    /// This constraint could not be applied, not all entities of this type and label are compliant.
    Failed,
}

impl TryFrom<&str> for ConstraintStatus {
    type Error = FalkorDBError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value.to_uppercase().as_str() {
            "OPERATIONAL" => Self::Active,
            "UNDER CONSTRUCTION" => Self::Pending,
            "FAILED" => Self::Failed,
            _ => Err(FalkorDBError::ConstraintStatus)?,
        })
    }
}

impl TryFrom<String> for ConstraintStatus {
    type Error = FalkorDBError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<&String> for ConstraintStatus {
    type Error = FalkorDBError;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
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

impl FalkorParsable for Constraint {
    fn from_falkor_value(
        value: FalkorValue,
        _graph_schema: &SyncGraphSchema,
        _conn: &mut BorrowedSyncConnection,
    ) -> Result<Self> {
        let [constraint_type_raw, label_raw, properties_raw, entity_type_raw, status_raw]: [FalkorValue;
            5] = value
            .into_vec()?
            .try_into()
            .map_err(|_| FalkorDBError::ParsingArrayToStructElementCount)?;

        let constraint_type = constraint_type_raw.into_string()?.try_into()?;
        let label = label_raw.into_string()?;
        let entity_type = entity_type_raw.into_string()?.try_into()?;
        let status = status_raw.into_string()?.try_into()?;

        let properties_vec = properties_raw.into_vec()?;
        let mut properties = Vec::with_capacity(properties_vec.len());
        for property in properties_vec {
            properties.push(property.into_string()?);
        }

        Ok(Constraint {
            constraint_type,
            label,
            properties,
            entity_type,
            status,
        })
    }
}
