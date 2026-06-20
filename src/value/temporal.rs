/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! FalkorDB temporal scalar values.
//!
//! FalkorDB encodes `datetime`, `date`, `time` and `duration` values in the compact protocol as a
//! single signed-integer scalar (type markers 13–16). These newtypes preserve that raw scalar
//! verbatim — the client performs no lossy calendar conversion — so callers keep full fidelity and
//! can interpret the value with whatever date/time library they prefer.
//!
//! As observed from the server: `date` is seconds since the Unix epoch at UTC midnight (negative
//! before 1970), `time`/`localtime` is a number of seconds, and `duration` is a span in seconds.

use crate::{parser::redis_value_as_int, FalkorResult};
use std::fmt;

macro_rules! temporal_scalar {
    ($(#[$meta:meta])* $name:ident, $kind:literal, $marker:literal) => {
        $(#[$meta])*
        #[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name {
            raw: i64,
        }

        impl $name {
            #[doc = concat!(
                "Wraps the raw FalkorDB scalar (compact type marker ", $marker, ") of a `", $kind,
                "` value."
            )]
            pub fn new(raw: i64) -> Self {
                Self { raw }
            }

            #[doc = concat!(
                "Returns the raw integer scalar FalkorDB sent for this `", $kind, "` value."
            )]
            pub fn raw(&self) -> i64 {
                self.raw
            }

            pub(crate) fn parse(value: redis::Value) -> FalkorResult<Self> {
                redis_value_as_int(value).map(Self::new)
            }
        }

        impl fmt::Display for $name {
            fn fmt(
                &self,
                f: &mut fmt::Formatter<'_>,
            ) -> fmt::Result {
                write!(f, "{}", self.raw)
            }
        }
    };
}

temporal_scalar!(
    /// A FalkorDB `datetime` value (compact type marker 13).
    ///
    /// Stores the raw signed-integer scalar FalkorDB sends; the client does not reinterpret it.
    DateTime,
    "datetime",
    "13"
);

temporal_scalar!(
    /// A FalkorDB `date` value (compact type marker 14).
    ///
    /// FalkorDB sends this as seconds since the Unix epoch at UTC midnight; it is negative for
    /// dates before 1970.
    Date,
    "date",
    "14"
);

temporal_scalar!(
    /// A FalkorDB `time` / `localtime` value (compact type marker 15), expressed in seconds.
    Time,
    "time",
    "15"
);

temporal_scalar!(
    /// A FalkorDB `duration` value (compact type marker 16): a span expressed in seconds.
    Duration,
    "duration",
    "16"
);

impl Duration {
    /// Returns this duration as a number of seconds (an alias for [`Duration::raw`]).
    pub fn as_seconds(&self) -> i64 {
        self.raw
    }

    /// Returns this duration as a [`std::time::Duration`], or `None` if it is negative — a
    /// [`std::time::Duration`] cannot represent a negative span.
    pub fn as_std_duration(&self) -> Option<std::time::Duration> {
        u64::try_from(self.raw)
            .ok()
            .map(std::time::Duration::from_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FalkorDBError;

    #[test]
    fn test_new_raw_and_display() {
        let dt = DateTime::new(1_700_000_000);
        assert_eq!(dt.raw(), 1_700_000_000);
        assert_eq!(dt.to_string(), "1700000000");

        let date = Date::new(-697_161_600);
        assert_eq!(date.raw(), -697_161_600);
        assert_eq!(date.to_string(), "-697161600");

        let time = Time::new(3600);
        assert_eq!(time.raw(), 3600);
        assert_eq!(time.to_string(), "3600");
    }

    #[test]
    fn test_parse_reads_int() {
        assert_eq!(
            DateTime::parse(redis::Value::Int(42)).unwrap(),
            DateTime::new(42)
        );
        assert_eq!(Date::parse(redis::Value::Int(-1)).unwrap(), Date::new(-1));
        assert_eq!(Time::parse(redis::Value::Int(7)).unwrap(), Time::new(7));
        assert_eq!(
            Duration::parse(redis::Value::Int(259_200)).unwrap(),
            Duration::new(259_200)
        );
    }

    #[test]
    fn test_parse_rejects_non_int() {
        let err = Duration::parse(redis::Value::SimpleString("nope".to_string())).unwrap_err();
        assert_eq!(err, FalkorDBError::ParsingI64);
    }

    #[test]
    fn test_duration_helpers() {
        let three_days = Duration::new(259_200);
        assert_eq!(three_days.raw(), 259_200);
        assert_eq!(three_days.to_string(), "259200");
        assert_eq!(three_days.as_seconds(), 259_200);
        assert_eq!(
            three_days.as_std_duration(),
            Some(std::time::Duration::from_secs(259_200))
        );

        // A negative span has no std::time::Duration representation.
        let negative = Duration::new(-1);
        assert_eq!(negative.as_seconds(), -1);
        assert_eq!(negative.as_std_duration(), None);
    }

    #[test]
    fn test_ordering_and_default() {
        assert_eq!(Date::default(), Date::new(0));
        assert!(Time::new(1) < Time::new(2));
    }
}
