/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Internal helpers for the optional `tracing` (and, later, `metrics`) instrumentation.
//!
//! Everything here is compiled only when an observability feature is enabled, so the hot path does
//! no instrumentation work when they are off. The headline safety property is that **no raw query
//! text or parameter values are ever recorded by default**: spans carry a privacy-safe
//! [`query_fingerprint`] (a hash of the query *template* with literals redacted), and the raw query
//! is recorded only behind the opt-in `with_query_logging` builder flag.

#[cfg(feature = "tokio")]
use crate::ConnectionStrategy;
use crate::FalkorDBError;
use std::sync::OnceLock;

/// Compute a privacy-safe, stable fingerprint of a Cypher query.
///
/// Literals (quoted strings, numbers, `true`/`false`/`null`) are redacted to `?` before hashing, so
/// the fingerprint depends only on the query *shape* — two calls that differ only in their literal
/// or parameter values share a fingerprint, and no sensitive value enters the hash. Redaction is
/// best-effort (a regex, not a full Cypher parser). The value is an FNV-1a hash rendered as 16 hex
/// digits; it is **not** guaranteed stable across crate versions (group within a deployment).
pub(crate) fn query_fingerprint(query: &str) -> String {
    let normalized = redact_literals(query);
    format!("{:016x}", fnv1a_64(normalized.as_bytes()))
}

/// Replace string / numeric / boolean / null literals with `?`. Identifiers, labels, property
/// names, keywords and structure are preserved, so the redacted text captures the query shape.
fn redact_literals(query: &str) -> String {
    static LITERAL: OnceLock<regex::Regex> = OnceLock::new();
    let re = LITERAL.get_or_init(|| {
        // Order matters: match whole quoted strings first so literals inside them are not matched
        // again, then numbers, then the boolean/null keywords (word-bounded, case-insensitive).
        regex::Regex::new(
            r#"'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"|\b\d+(?:\.\d+)?\b|(?i)\b(?:true|false|null)\b"#,
        )
        .expect("the literal-redaction regex is a valid, fixed pattern")
    });
    re.replace_all(query, "?").into_owned()
}

/// FNV-1a 64-bit hash. Deterministic and dependency-free; adequate for a grouping fingerprint.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// A bounded, payload-free label for an error, safe to use as a span field or (later) a metric
/// label. Matches on the variant only — never on any carried `String` — so it can never echo a
/// query, parameter, graph name, or server message.
pub(crate) fn error_kind(error: &FalkorDBError) -> &'static str {
    match error {
        FalkorDBError::ConnectionDown => "connection_down",
        FalkorDBError::NoConnection => "no_connection",
        FalkorDBError::EmptyConnection => "empty_connection",
        FalkorDBError::SentinelConnection(_) => "sentinel_connection",
        FalkorDBError::SentinelMastersCount => "sentinel_masters_count",
        FalkorDBError::Timeout { .. } => "timeout",
        FalkorDBError::ConstraintFailed { .. } => "constraint_failed",
        FalkorDBError::RedisError(_) => "redis_error",
        FalkorDBError::RedisParsingError(_) => "redis_parsing_error",
        FalkorDBError::InvalidConnectionInfo(_) => "invalid_connection_info",
        FalkorDBError::InvalidDataReceived => "invalid_data_received",
        FalkorDBError::UnavailableProvider => "unavailable_provider",
        FalkorDBError::ParamEncoding { .. } => "param_encoding",
        FalkorDBError::TypeError { .. } => "type_error",
        FalkorDBError::EmbeddedServerError(_) => "embedded_server_error",
        FalkorDBError::SingleThreadedRuntime => "single_threaded_runtime",
        FalkorDBError::NoRuntime => "no_runtime",
        // The long tail (parse/type/result-mapping/schema variants and any future additions) is
        // bucketed; the variant name is never sensitive, but one label keeps cardinality bounded.
        _ => "other",
    }
}

/// A bounded label for the active connection strategy. The sync client is always pooled.
#[cfg(feature = "tokio")]
pub(crate) fn strategy_label(strategy: &ConnectionStrategy) -> &'static str {
    match strategy {
        ConnectionStrategy::Pooled { .. } => "pooled",
        ConnectionStrategy::Multiplexed { .. } => "multiplexed",
    }
}

/// The strategy label for the (always pooled) sync client.
pub(crate) const SYNC_STRATEGY: &str = "pooled";

/// Record the request-side operational fields on the current span: the connection strategy, whether
/// the operation is read-only, and the privacy-safe query fingerprint — plus the raw query template
/// **only** when `log_raw` is set (the opt-in `with_query_logging` flag). Parameter values are never
/// recorded — they live in the query preamble, not in `query_template`.
pub(crate) fn record_request(
    strategy: &'static str,
    read_only: bool,
    query_template: &str,
    log_raw: bool,
) {
    let span = tracing::Span::current();
    span.record("db.falkordb.strategy", strategy);
    span.record("db.falkordb.read_only", read_only);
    span.record(
        "db.query.fingerprint",
        query_fingerprint(query_template).as_str(),
    );
    if log_raw {
        span.record("db.query.text", query_template);
    }
}

/// Record the bounded error kind on the current span when an operation fails.
pub(crate) fn record_error(error: &FalkorDBError) {
    tracing::Span::current().record("error.type", error_kind(error));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_stable_for_same_query() {
        assert_eq!(
            query_fingerprint("MATCH (n:Person) RETURN n.name"),
            query_fingerprint("MATCH (n:Person) RETURN n.name"),
        );
    }

    #[test]
    fn fingerprint_is_value_independent_for_inlined_literals() {
        // The whole point of redaction: differing literal values must not change the fingerprint.
        let a = query_fingerprint("MATCH (u:User {email: 'alice@example.com'}) RETURN u");
        let b = query_fingerprint("MATCH (u:User {email: 'bob@other.org'}) RETURN u");
        assert_eq!(
            a, b,
            "inlined string literals must be redacted before hashing"
        );

        let n1 = query_fingerprint("MATCH (n) WHERE n.age > 21 RETURN n");
        let n2 = query_fingerprint("MATCH (n) WHERE n.age > 65 RETURN n");
        assert_eq!(n1, n2, "numeric literals must be redacted before hashing");
    }

    #[test]
    fn fingerprint_distinguishes_query_shape() {
        assert_ne!(
            query_fingerprint("MATCH (n:Person) RETURN n.name"),
            query_fingerprint("MATCH (n:Movie) RETURN n.title"),
        );
    }

    #[test]
    fn redaction_removes_literal_values_and_fingerprint_is_hex() {
        let query = "MATCH (u {ssn: '123-45-6789', name: 'secret'}) RETURN u";
        // The meaningful privacy check: the redacted text (the actual hash *input*) contains no
        // literal values. Asserting on the hash digest itself would be flaky — random hex can
        // contain "123" by chance, and "secret" can never appear in hex regardless.
        let redacted = redact_literals(query);
        assert!(
            !redacted.contains("123-45-6789"),
            "numbers must be redacted: {redacted:?}"
        );
        assert!(
            !redacted.contains("secret"),
            "strings must be redacted: {redacted:?}"
        );
        assert!(
            redacted.contains('?'),
            "literals are replaced with placeholders: {redacted:?}"
        );
        // The fingerprint itself is a fixed-length hex digest.
        let fingerprint = query_fingerprint(query);
        assert_eq!(fingerprint.len(), 16, "fingerprint is 16 hex digits");
        assert!(fingerprint.bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[test]
    fn redaction_preserves_shape_and_strips_literals() {
        assert_eq!(
            redact_literals("MATCH (u:User {age: 30, active: true}) RETURN u.name"),
            "MATCH (u:User {age: ?, active: ?}) RETURN u.name",
        );
    }

    #[test]
    fn error_kind_is_bounded_and_payload_free() {
        // A String-carrying variant must map to a fixed label, never echoing the payload.
        assert_eq!(
            error_kind(&FalkorDBError::RedisError("secret server detail".into())),
            "redis_error",
        );
        assert_eq!(
            error_kind(&FalkorDBError::ConnectionDown),
            "connection_down"
        );
        assert_eq!(
            error_kind(&FalkorDBError::SentinelConnection("host:port".into())),
            "sentinel_connection",
        );
    }

    #[test]
    fn error_kind_is_a_bounded_lowercase_label_for_every_constructed_variant() {
        let cases: Vec<FalkorDBError> = vec![
            FalkorDBError::ConnectionDown,
            FalkorDBError::NoConnection,
            FalkorDBError::EmptyConnection,
            FalkorDBError::InvalidDataReceived,
            FalkorDBError::UnavailableProvider,
            FalkorDBError::SingleThreadedRuntime,
            FalkorDBError::NoRuntime,
            FalkorDBError::SentinelMastersCount,
            FalkorDBError::SentinelConnection("payload".into()),
            FalkorDBError::RedisError("payload".into()),
            FalkorDBError::RedisParsingError("payload".into()),
            FalkorDBError::InvalidConnectionInfo("payload".into()),
            FalkorDBError::EmbeddedServerError("payload".into()),
            FalkorDBError::ParamEncoding {
                parameter: Some("payload".into()),
                message: "payload".into(),
            },
            FalkorDBError::TypeError {
                expected: "Payload",
                got: "Payload",
            },
            FalkorDBError::Timeout {
                operation: crate::WaitOperation::IndexCreation,
                timeout: std::time::Duration::from_secs(1),
            },
            FalkorDBError::ConstraintFailed {
                label: "payload".into(),
                properties: vec!["payload".into()],
                constraint_type: crate::ConstraintType::Unique,
            },
            // Catch-all bucket:
            FalkorDBError::ParsingError("payload".into()),
            FalkorDBError::InvalidEnumType("payload".into()),
            FalkorDBError::ParsingBool,
        ];
        for err in &cases {
            let kind = error_kind(err);
            assert!(!kind.is_empty(), "{err:?} must have a label");
            assert!(
                !kind.to_lowercase().contains("payload"),
                "error_kind for {err:?} must not echo any payload"
            );
            assert!(
                kind.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
                "error_kind for {err:?} must be a lowercase_snake label, got {kind:?}"
            );
        }
    }
}

/// End-to-end check that [`record_request`] / [`record_error`] set the declared span fields — and,
/// critically, that the field names match the `#[instrument(fields(...))]` declarations on the
/// execution seams (a mismatch would silently no-op). Uses a `tracing-subscriber` registry so
/// `Span::current()` resolves correctly.
#[cfg(test)]
mod span_capture_tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::registry::LookupSpan;
    use tracing_subscriber::Layer;

    #[derive(Default)]
    struct Visitor(HashMap<String, String>);

    impl Visit for Visitor {
        fn record_debug(
            &mut self,
            field: &Field,
            value: &dyn std::fmt::Debug,
        ) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
        fn record_str(
            &mut self,
            field: &Field,
            value: &str,
        ) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_bool(
            &mut self,
            field: &Field,
            value: bool,
        ) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
    }

    #[derive(Clone, Default)]
    struct CaptureLayer(Arc<Mutex<HashMap<String, String>>>);

    impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> Layer<S> for CaptureLayer {
        fn on_record(
            &self,
            _id: &tracing::Id,
            values: &tracing::span::Record<'_>,
            _ctx: Context<'_, S>,
        ) {
            let mut visitor = Visitor::default();
            values.record(&mut visitor);
            self.0.lock().unwrap().extend(visitor.0);
        }
    }

    fn capture(body: impl FnOnce()) -> HashMap<String, String> {
        let layer = CaptureLayer::default();
        let recorded = layer.0.clone();
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, body);
        let guard = recorded.lock().unwrap();
        guard.clone()
    }

    #[test]
    fn records_strategy_read_only_fingerprint_and_error_but_not_raw_text() {
        let fields = capture(|| {
            let span = tracing::trace_span!(
                "test",
                db.falkordb.strategy = tracing::field::Empty,
                db.falkordb.read_only = tracing::field::Empty,
                db.query.fingerprint = tracing::field::Empty,
                db.query.text = tracing::field::Empty,
                error.type = tracing::field::Empty,
            );
            let _enter = span.enter();
            super::record_request(
                "multiplexed",
                true,
                "MATCH (n {x: 'secret'}) RETURN n",
                false,
            );
            super::record_error(&crate::FalkorDBError::ConnectionDown);
        });
        assert_eq!(
            fields.get("db.falkordb.strategy").map(String::as_str),
            Some("multiplexed")
        );
        assert_eq!(
            fields.get("db.falkordb.read_only").map(String::as_str),
            Some("true")
        );
        assert!(
            fields.contains_key("db.query.fingerprint"),
            "fingerprint must be recorded"
        );
        assert!(
            !fields.contains_key("db.query.text"),
            "raw query text must be absent unless query logging is enabled"
        );
        assert_eq!(
            fields.get("error.type").map(String::as_str),
            Some("connection_down")
        );
    }

    #[test]
    fn records_raw_text_only_when_query_logging_enabled() {
        let fields = capture(|| {
            let span = tracing::trace_span!(
                "test",
                db.falkordb.strategy = tracing::field::Empty,
                db.falkordb.read_only = tracing::field::Empty,
                db.query.fingerprint = tracing::field::Empty,
                db.query.text = tracing::field::Empty,
            );
            let _enter = span.enter();
            super::record_request("pooled", false, "MATCH (n) RETURN n", true);
        });
        assert_eq!(
            fields.get("db.query.text").map(String::as_str),
            Some("MATCH (n) RETURN n")
        );
    }
}
