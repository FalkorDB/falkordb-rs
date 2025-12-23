/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::SchemaType;

/// A verbose error enum used throughout the client, messages are static string slices.
/// this allows easy error integration using [`thiserror`]
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum FalkorDBError {
    /// A required ID for parsing was not found in the schema.
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
    #[error("Could not parse the provided connection info: {0}")]
    InvalidConnectionInfo(String),
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
    #[error("Could not parse header: {0}")]
    ParsingHeader(&'static str),
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
    /// Element was not of type F32.
    #[error("Element was not of type F32")]
    ParsingF32,
    /// Element was not of type Vec32.
    #[error("Element was not of type Vec32: {0}")]
    ParsingVec32(String),
    /// Element was not of type Array.
    #[error("Element was not of type Array")]
    ParsingArray,
    /// Element was not of type String.
    #[error("Element was not of type String")]
    ParsingString,
    /// Element was not of type FEdge.
    #[error("Element was not of type FEdge")]
    ParsingFEdge,
    /// Element was not of type FNode.
    #[error("Element was not of type FNode")]
    ParsingFNode,
    /// Element was not of type Path.
    #[error("Element was not of type Path")]
    ParsingPath,
    /// Element was not of type Map.
    #[error("Element was not of type Map")]
    ParsingMap,
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
    /// Attempting to parse an Array into a struct, but the array doesn't have the expected element count.
    #[error("Attempting to parse an Array into a struct, but the array doesn't have the expected element count: {0}")]
    ParsingArrayToStructElementCount(&'static str),
    /// Invalid enum string variant was encountered when parsing
    #[error("Invalid enum string variant was encountered when parsing: {0}")]
    InvalidEnumType(String),
    /// Running in a single-threaded tokio runtime! Running async operations in a blocking context will cause a panic, aborting operation
    #[error("Running in a single-threaded tokio runtime! Running async operations in a blocking context will cause a panic, aborting operation")]
    SingleThreadedRuntime,
    /// No runtime detected, you are trying to run an async operation from a sync context
    #[error("No runtime detected, you are trying to run an async operation from a sync context")]
    NoRuntime,
    /// An error occurred with the embedded FalkorDB server
    #[error("Embedded server error: {0}")]
    EmbeddedServerError(String),
}

impl From<strum::ParseError> for FalkorDBError {
    fn from(value: strum::ParseError) -> Self {
        FalkorDBError::InvalidEnumType(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedded_server_error_display() {
        let error = FalkorDBError::EmbeddedServerError("test error".to_string());
        assert_eq!(error.to_string(), "Embedded server error: test error");
    }

    #[test]
    fn test_embedded_server_error_debug() {
        let error = FalkorDBError::EmbeddedServerError("debug test".to_string());
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("EmbeddedServerError"));
        assert!(debug_str.contains("debug test"));
    }

    #[test]
    fn test_embedded_server_error_equality() {
        let error1 = FalkorDBError::EmbeddedServerError("same".to_string());
        let error2 = FalkorDBError::EmbeddedServerError("same".to_string());
        let error3 = FalkorDBError::EmbeddedServerError("different".to_string());

        assert_eq!(error1, error2);
        assert_ne!(error1, error3);
    }

    #[test]
    fn test_invalid_connection_info_error() {
        let error = FalkorDBError::InvalidConnectionInfo("bad connection".to_string());
        assert!(error.to_string().contains("bad connection"));
    }

    #[test]
    fn test_redis_error() {
        let error = FalkorDBError::RedisError("connection failed".to_string());
        assert!(error.to_string().contains("connection failed"));
    }

    #[test]
    fn test_error_from_strum() {
        // Test the From impl for strum::ParseError
        let parse_error = strum::ParseError::VariantNotFound;
        let falkor_error: FalkorDBError = parse_error.into();
        assert!(matches!(falkor_error, FalkorDBError::InvalidEnumType(_)));
    }

    #[test]
    fn test_unavailable_provider_error() {
        let error = FalkorDBError::UnavailableProvider;
        assert!(error.to_string().contains("unavailable"));
    }

    #[test]
    fn test_no_connection_error() {
        let error = FalkorDBError::NoConnection;
        assert!(error.to_string().contains("Could not connect"));
    }
}
