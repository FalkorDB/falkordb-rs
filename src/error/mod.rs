/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

/// A verbose error enum used throughout the client, messages are static string slices.
/// this allows easy [`anyhow`] integration using [`thiserror`]
#[derive(thiserror::Error, Debug)]
pub enum FalkorDBError {
    #[error("The provided connection info is invalid")]
    InvalidConnectionInfo,
    #[error("The connection returned invalid data for this command")]
    InvalidDataReceived,
    #[error("The provided URL scheme points at a database provider that is currently unavailable, make sure the correct feature is enabled")]
    UnavailableProvider,
    #[error("The number of connections for the client has to be between 1 and 32")]
    InvalidConnectionPoolSize,
    #[error("Attempting to use an empty connection object")]
    EmptyConnection,
    #[error("General parsing error")]
    ParsingError,
    #[error("Received malformed header")]
    ParsingHeader,
    #[error("The id received for this label/property/relationship was unknown")]
    ParsingCompactIdUnknown,
    #[error("Unknown type")]
    ParsingUnknownType,
    #[error("Element was not of type Bool")]
    ParsingBool,
    #[error("Element was not of type I64")]
    ParsingI64,
    #[error("Element was not of type F64")]
    ParsingF64,
    #[error("Element was not of type FArray")]
    ParsingFArray,
    #[error("Element was not of type FString")]
    ParsingFString,
    #[error("Element was not of type FEdge")]
    ParsingFEdge,
    #[error("Element was not of type FNode")]
    ParsingFNode,
    #[error("Element was not of type FPath")]
    ParsingFPath,
    #[error("Element was not of type FMap")]
    ParsingFMap,
    #[error("Element was not of type FPoint")]
    ParsingFPoint,
    #[error("Key id was not of type i64")]
    ParsingKeyIdTypeMismatch,
    #[error("Type marker was not of type i64")]
    ParsingTypeMarkerTypeMismatch,
    #[error("Both key id and type marker were not of type i64")]
    ParsingKTVTypes,
    #[error("Field missing or mismatched while parsing index")]
    ParsingIndex,
    #[error("Attempting to parse an FArray into a struct, but the array doesn't have the expected element count")]
    ParsingArrayToStructElementCount,
    #[error("Invalid constraint type, expected 'UNIQUE' or 'MANDATORY'")]
    ConstraintType,
    #[error("Invalid constraint status, expected 'OPERATIONAL', 'UNDER CONSTRUCTION' or 'FAILED'")]
    ConstraintStatus,
    #[error("Invalid Index status, expected 'OPERATIONAL' or 'UNDER CONSTRUCTION'")]
    IndexStatus,
    #[error("Invalid Index field type, expected 'RANGE', 'VECTOR' or 'FULLTEXT'")]
    IndexFieldType,
}
