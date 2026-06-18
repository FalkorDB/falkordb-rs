/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::SchemaType;

/// A verbose error enum used throughout the client, messages are static string slices.
/// this allows easy error integration using [`thiserror`]
#[derive(thiserror::Error, Debug, PartialEq)]
#[non_exhaustive]
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
    /// A background operation did not reach its expected state before the wait timed out.
    #[error("Timed out after {timeout:?} waiting for {operation}")]
    Timeout {
        /// Which background operation timed out.
        operation: crate::WaitOperation,
        /// The timeout that elapsed.
        timeout: std::time::Duration,
    },
    /// A constraint reached the terminal `FAILED` state (e.g. existing data violates it).
    #[error(
        "{constraint_type} constraint on label '{label}' properties {properties:?} failed to be enforced"
    )]
    ConstraintFailed {
        /// The label the constraint applies to.
        label: String,
        /// The properties the constraint applies to.
        properties: Vec<String>,
        /// Whether the failed constraint was unique or mandatory.
        constraint_type: crate::ConstraintType,
    },
    /// Mapping a FalkorDB result into a user type via `serde` failed. This covers both
    /// value-level deserialization (a [`crate::FalkorValue`]) and row-level mapping (a result
    /// row, e.g. a header/value length mismatch).
    #[cfg(feature = "serde")]
    #[error("Failed to deserialize via serde: {0}")]
    SerdeError(String),
    /// A value could not be encoded as a Cypher query-parameter literal (for example a non-finite
    /// float, a string containing a NUL byte, an integer outside the `i64` range, an invalid
    /// parameter name, or a graph entity such as a `Node`/`Edge`/`Path`).
    #[error("invalid query parameter{}: {message}", .parameter.as_deref().map(|p| format!(" '{p}'")).unwrap_or_default())]
    ParamEncoding {
        /// The name of the offending parameter, when known.
        parameter: Option<String>,
        /// A human-readable description of why encoding failed.
        message: String,
    },
    /// A result row did not contain a column with the requested name.
    #[error("result row has no column named '{name}'")]
    MissingColumn {
        /// The requested column name.
        name: String,
    },
    /// A result row was indexed past its number of columns.
    #[error("column index {index} is out of bounds for a row with {len} column(s)")]
    ColumnIndexOutOfBounds {
        /// The requested column index.
        index: usize,
        /// The number of columns in the row.
        len: usize,
    },
    /// A result row's value count did not match the result header's column count.
    #[error("result row shape mismatch: header has {header_len} column(s) but the row has {value_len} value(s)")]
    RowShapeMismatch {
        /// The number of columns in the header.
        header_len: usize,
        /// The number of values in the row.
        value_len: usize,
    },
    /// A [`crate::FalkorValue`] could not be converted into the requested Rust type.
    #[error("expected a value of type {expected}, but got {got}")]
    TypeError {
        /// The Rust type that was requested.
        expected: &'static str,
        /// The [`crate::FalkorValue`] variant that was found.
        got: &'static str,
    },
}

impl FalkorDBError {
    /// A short, actionable remediation hint for common, recognizable errors, or [`None`] when there
    /// is no specific guidance for this error.
    ///
    /// This is purely additive convenience for humans and AI tooling: the error's
    /// [`Display`](std::fmt::Display) / [`Debug`] output is unchanged and the raw message is always
    /// preserved. Hints are fixed `&'static str`s — no text from the underlying error is echoed into
    /// them, so a hint can never leak data from the original message — and unrecognized errors
    /// return `None`. Recognition of server messages is best-effort and version-tolerant: it matches
    /// a few well-known FalkorDB phrases and returns `None` for anything else.
    ///
    /// # Examples
    ///
    /// ```
    /// use falkordb::FalkorDBError;
    ///
    /// // A recognized client-side error gives a hint.
    /// assert!(FalkorDBError::ConnectionDown.mitigation_hint().is_some());
    ///
    /// // An unrecognized message gives `None` — the raw error is still available via `Display`.
    /// let err = FalkorDBError::RedisError("READONLY You can't write against a replica".into());
    /// assert_eq!(err.mitigation_hint(), None);
    /// ```
    #[must_use]
    pub fn mitigation_hint(&self) -> Option<&'static str> {
        match self {
            Self::SingleThreadedRuntime => Some(
                "run async operations on a multi-thread Tokio runtime, e.g. \
                 `#[tokio::main(flavor = \"multi_thread\")]`, not the current-thread runtime",
            ),
            Self::NoRuntime => Some(
                "no Tokio runtime is running — call async APIs from inside a runtime, or use the \
                 synchronous client instead",
            ),
            Self::ConnectionDown => Some(
                "the connection dropped — retry the operation; the client swaps in a fresh \
                 connection for the next attempt",
            ),
            Self::MissingSchemaId(_) => Some(
                "the local schema cache is stale; it normally self-heals on refresh, so retry the \
                 query",
            ),
            Self::UnavailableProvider => Some(
                "the requested provider or read-replica route isn't available — enable the matching \
                 cargo feature (for example `tokio` for async, or `rustls` / `native-tls` for TLS), \
                 and for read-only queries make sure a read replica is configured",
            ),
            Self::RedisError(message) | Self::EmbeddedServerError(message) => {
                server_message_hint(message)
            }
            _ => None,
        }
    }
}

/// Best-effort recognition of well-known FalkorDB *server* error messages.
///
/// [`FalkorDBError::RedisError`] / [`FalkorDBError::EmbeddedServerError`] are mixed buckets — they
/// also carry connection and client errors — so this matches conservatively on lowercased,
/// multi-token phrases and returns [`None`] for anything it does not specifically recognize.
fn server_message_hint(message: &str) -> Option<&'static str> {
    let message = message.to_ascii_lowercase();
    if message.contains("invalid graph operation on empty key")
        || message.contains("key doesn't contains a graph")
        || message.contains("key doesn't contain a graph")
    {
        Some(
            "the graph key is missing or isn't a graph — create the graph with a write query (e.g. \
             `CREATE`) first, or pick a name that doesn't collide with an existing non-graph key",
        )
    } else if message.contains("errmsg:")
        && message.contains("line:")
        && message.contains("column:")
    {
        Some(
            "Cypher syntax error — check the query near the reported line/column, and pass values \
             with `with_param` instead of formatting them into the query string",
        )
    } else if message.contains("query timed out") {
        Some(
            "the query exceeded its timeout — raise it with `QueryBuilder::with_timeout(ms)` (or the \
             batch query's `with_timeout(ms)`)",
        )
    } else if message.contains("wrong number of arguments for 'graph")
        || message.contains("unknown command 'graph")
    {
        Some(
            "the server didn't accept this graph command — make sure it is FalkorDB (not plain \
             Redis) and recent enough",
        )
    } else {
        None
    }
}

#[cfg(feature = "serde")]
impl serde::de::Error for FalkorDBError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        FalkorDBError::SerdeError(msg.to_string())
    }
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

    #[test]
    fn mitigation_hint_for_recognized_variants() {
        assert!(FalkorDBError::SingleThreadedRuntime
            .mitigation_hint()
            .unwrap()
            .contains("multi-thread"));
        assert!(FalkorDBError::NoRuntime
            .mitigation_hint()
            .unwrap()
            .contains("runtime"));
        assert!(FalkorDBError::ConnectionDown
            .mitigation_hint()
            .unwrap()
            .contains("retry"));
        assert!(FalkorDBError::MissingSchemaId(SchemaType::Labels)
            .mitigation_hint()
            .unwrap()
            .contains("retry"));
        assert!(FalkorDBError::UnavailableProvider
            .mitigation_hint()
            .unwrap()
            .contains("feature"));
    }

    #[test]
    fn mitigation_hint_recognizes_server_messages() {
        // Frozen sample strings (not live server output) so the test never depends on a server build.
        let cases = [
            ("Invalid graph operation on empty key", "create the graph"),
            ("key doesn't contains a graph", "create the graph"),
            ("key doesn't contain a graph", "create the graph"),
            (
                "errMsg: syntax error line: 1, column: 5, offset: 4",
                "Cypher syntax",
            ),
            ("Query timed out", "timeout"),
            (
                "ERR wrong number of arguments for 'GRAPH.QUERY' command",
                "FalkorDB",
            ),
            ("ERR unknown command 'GRAPH.QUERY'", "FalkorDB"),
        ];
        for (message, needle) in cases {
            let hint = FalkorDBError::RedisError(message.to_string()).mitigation_hint();
            assert!(
                hint.is_some_and(|h| h.contains(needle)),
                "{message:?} -> {hint:?} should contain {needle:?}"
            );
        }
        // EmbeddedServerError shares the same recognizer.
        assert!(FalkorDBError::EmbeddedServerError("Query timed out".into())
            .mitigation_hint()
            .is_some());
    }

    #[test]
    fn mitigation_hint_is_none_for_unrecognized_errors() {
        // An unrelated message in the mixed `RedisError` bucket must not false-positive.
        assert_eq!(
            FalkorDBError::RedisError("READONLY You can't write against a replica.".into())
                .mitigation_hint(),
            None
        );
        // A variant with no specific guidance.
        assert_eq!(FalkorDBError::InvalidDataReceived.mitigation_hint(), None);
        // A non-graph Redis arity error must not get the graph-command hint.
        assert_eq!(
            FalkorDBError::RedisError("ERR wrong number of arguments for 'AUTH' command".into())
                .mitigation_hint(),
            None
        );
        // An `errMsg:` token without a line/column position is not treated as a Cypher parse error.
        assert_eq!(
            FalkorDBError::RedisError("errMsg: something else entirely".into()).mitigation_hint(),
            None
        );
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn mitigation_hint_never_panics(message in ".*") {
            // For any message, the recognizer must not panic (and returns a 'static hint or None).
            let _ = FalkorDBError::RedisError(message.clone()).mitigation_hint();
            let _ = FalkorDBError::EmbeddedServerError(message).mitigation_hint();
        }
    }
}
