/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::SchemaType;

/// A verbose error enum used throughout the client, messages are static string slices.
/// this allows easy error integration using [`thiserror`]
#[derive(thiserror::Error, Debug)]
pub enum FalkorDBError {
    /// A required Id for parsing was not found in the schema.
    #[error("A required Id for parsing was not found in the schema")]
    MissingSchemaId(SchemaType),
    /// Could not connect to Redis Sentinel, or a critical Sentinel operation has failed.
    #[error(
        "Could not connect to Redis Sentinel, or a critical Sentinel operation has failed: {0}"
    )]
    SentinelConnection(String),
    /// Received unsupported number of sentinel masters in list, there can be only one.
    #[error("Received unsupported number of sentinel masters in list, there can be only one")]
    SentinelMastersCount,
    ///This requested returned a connection error, however, we may be able to create a new connection to the server, this operation should probably be retried in a bit.
    #[error("This requested returned a connection error, however, we may be able to create a new connection to the server, this operation should probably be retried in a bit.")]
    ConnectionDown,
    /// An error occurred while sending the request to Redis.
    #[error("An error occurred while sending the request to Redis: {0}")]
    RedisError(String),
    /// An error occurred while parsing the Redis response.
    #[error("An error occurred while parsing the Redis response: {0}")]
    RedisParsingError(String),
    /// The provided connection info is invalid.
    #[error("The provided connection info is invalid")]
    InvalidConnectionInfo,
    /// The connection returned invalid data for this command.
    #[error("The connection returned invalid data for this command")]
    InvalidDataReceived,
    /// The provided URL scheme points at a database provider that is currently unavailable, make sure the correct feature is enabled.
    #[error("The provided URL scheme points at a database provider that is currently unavailable, make sure the correct feature is enabled")]
    UnavailableProvider,
    /// An error occurred when dealing with reference counts or RefCells, perhaps mutual borrows?
    #[error(
        "An error occurred when dealing with reference counts or RefCells, perhaps mutual borrows?"
    )]
    RefCountBooBoo,
    /// The execution plan did not adhere to usual structure, and could not be parsed.
    #[error("The execution plan did not adhere to usual structure, and could not be parsed")]
    CorruptExecutionPlan,
    /// The number of connections for the client has to be between 1 and 32.
    #[error("The number of connections for the client has to be between 1 and 32")]
    InvalidConnectionPoolSize,
    /// Could not connect to the server with the provided address.
    #[error("Could not connect to the server with the provided address")]
    NoConnection,
    /// Attempting to use an empty connection object.
    #[error("Attempting to use an empty connection object")]
    EmptyConnection,
    /// General parsing error.
    #[error("General parsing error: {0}")]
    ParsingError(String),
    /// Received malformed header.
    #[error("Received malformed header")]
    ParsingHeader,
    /// The id received for this label/property/relationship was unknown.
    #[error("The id received for this label/property/relationship was unknown")]
    ParsingCompactIdUnknown,
    /// Unknown type.
    #[error("Unknown type")]
    ParsingUnknownType,
    /// Element was not of type Bool.
    #[error("Element was not of type Bool")]
    ParsingBool,
    /// Could not parse into config value, was not one of the supported types.
    #[error("Could not parse into config value, was not one of the supported types")]
    ParsingConfigValue,
    /// Element was not of type I64.
    #[error("Element was not of type I64")]
    ParsingI64,
    /// Element was not of type F64.
    #[error("Element was not of type F64")]
    ParsingF64,
    /// Element was not of type FArray.
    #[error("Element was not of type FArray")]
    ParsingFArray,
    /// Element was not of type FString.
    #[error("Element was not of type FString")]
    ParsingFString,
    /// Element was not of type FEdge.
    #[error("Element was not of type FEdge")]
    ParsingFEdge,
    /// Element was not of type FNode.
    #[error("Element was not of type FNode")]
    ParsingFNode,
    /// Element was not of type FPath.
    #[error("Element was not of type FPath")]
    ParsingFPath,
    /// Element was not of type FMap.
    #[error("Element was not of type FMap")]
    ParsingFMap,
    /// Element was not of type FPoint.
    #[error("Element was not of type FPoint")]
    ParsingFPoint,
    /// Key id was not of type i64.
    #[error("Key id was not of type i64")]
    ParsingKeyIdTypeMismatch,
    /// Type marker was not of type i64.
    #[error("Type marker was not of type i64")]
    ParsingTypeMarkerTypeMismatch,
    /// Both key id and type marker were not of type i64.
    #[error("Both key id and type marker were not of type i64")]
    ParsingKTVTypes,
    /// Field missing or mismatched while parsing index.
    #[error("Field missing or mismatched while parsing index")]
    ParsingIndex,
    /// Attempting to parse an FArray into a struct, but the array doesn't have the expected element count.
    #[error("Attempting to parse an FArray into a struct, but the array doesn't have the expected element count")]
    ParsingArrayToStructElementCount,
    /// Invalid constraint type, expected 'UNIQUE' or 'MANDATORY'.
    #[error("Invalid constraint type, expected 'UNIQUE' or 'MANDATORY'")]
    ConstraintType,
    /// Invalid constraint status, expected 'OPERATIONAL', 'UNDER CONSTRUCTION' or 'FAILED'.
    #[error("Invalid constraint status, expected 'OPERATIONAL', 'UNDER CONSTRUCTION' or 'FAILED'")]
    ConstraintStatus,
    /// Invalid Index status, expected 'OPERATIONAL' or 'UNDER CONSTRUCTION'.
    #[error("Invalid Index status, expected 'OPERATIONAL' or 'UNDER CONSTRUCTION'")]
    IndexStatus,
    /// Invalid Index field type, expected 'RANGE', 'VECTOR' or 'FULLTEXT'.
    #[error("Invalid Index field type, expected 'RANGE', 'VECTOR' or 'FULLTEXT'")]
    IndexType,
}
