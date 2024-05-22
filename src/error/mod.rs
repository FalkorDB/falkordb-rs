/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

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
    #[error("Could not form slowlog entry, element count invalid")]
    ParsingSlowlogEntryElementCount,
    #[error("Could not parse node, element count invalid")]
    ParsingNodeElementCount,
}
