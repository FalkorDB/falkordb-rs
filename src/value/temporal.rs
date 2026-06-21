/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! FalkorDB temporal scalar values.
//!
//! FalkorDB encodes `datetime`, `date`, `time` and `duration` values in the compact protocol as a
//! single signed-integer scalar (type markers 13–16). These newtypes preserve that scalar verbatim
//! — the client performs no lossy calendar conversion — so callers keep full fidelity and can
//! interpret the value with whatever date/time library they prefer.
//!
//! Each value exposes its scalar as a typed [`Seconds`] (rather than a bare `i64`), and the instant
//! types support a small, type-safe algebra: subtracting two [`DateTime`]s yields a [`Duration`],
//! and a [`DateTime`] can be shifted by a [`Duration`]. Nonsensical combinations (e.g. adding two
//! instants) simply have no impl and fail to compile.
//!
//! As observed from the server: `date` is seconds since the Unix epoch at UTC midnight (negative
//! before 1970), `time`/`localtime` is a number of seconds, and `duration` is a span in seconds.

use crate::{parser::redis_value_as_int, FalkorResult};
use std::fmt;
use std::ops::{Add, Neg, Sub};

/// A signed number of seconds — the scalar carried by every FalkorDB temporal value.
///
/// This newtype keeps a temporal scalar from being silently mixed with an arbitrary integer; call
/// [`Seconds::get`] (or use the [`From`] conversions) for the underlying `i64`. The plain `+`/`-`
/// operators follow `i64` semantics (they panic on overflow in debug builds); the `checked_*`
/// methods return [`None`] on overflow instead.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Seconds(i64);

impl Seconds {
    /// Wraps a raw number of seconds.
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Returns the underlying number of seconds.
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Adds two second counts, returning [`None`] on `i64` overflow.
    pub fn checked_add(
        self,
        rhs: Seconds,
    ) -> Option<Seconds> {
        self.0.checked_add(rhs.0).map(Seconds)
    }

    /// Subtracts two second counts, returning [`None`] on `i64` overflow.
    pub fn checked_sub(
        self,
        rhs: Seconds,
    ) -> Option<Seconds> {
        self.0.checked_sub(rhs.0).map(Seconds)
    }

    /// Negates this second count, returning [`None`] on `i64` overflow (i.e. `i64::MIN`).
    pub fn checked_neg(self) -> Option<Seconds> {
        self.0.checked_neg().map(Seconds)
    }
}

impl fmt::Display for Seconds {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for Seconds {
    fn from(value: i64) -> Self {
        Seconds(value)
    }
}

impl From<Seconds> for i64 {
    fn from(value: Seconds) -> Self {
        value.0
    }
}

impl Add for Seconds {
    type Output = Seconds;
    fn add(
        self,
        rhs: Seconds,
    ) -> Seconds {
        Seconds(self.0 + rhs.0)
    }
}

impl Sub for Seconds {
    type Output = Seconds;
    fn sub(
        self,
        rhs: Seconds,
    ) -> Seconds {
        Seconds(self.0 - rhs.0)
    }
}

impl Neg for Seconds {
    type Output = Seconds;
    fn neg(self) -> Seconds {
        Seconds(-self.0)
    }
}

macro_rules! temporal_scalar {
    ($(#[$meta:meta])* $name:ident, $kind:literal, $marker:literal) => {
        $(#[$meta])*
        #[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name {
            secs: i64,
        }

        impl $name {
            #[doc = concat!(
                "Wraps the raw FalkorDB scalar (compact type marker ", $marker, ") of a `", $kind,
                "` value, in seconds."
            )]
            pub const fn new(seconds: i64) -> Self {
                Self { secs: seconds }
            }

            #[doc = concat!(
                "Returns this `", $kind, "` value's scalar as a typed [`Seconds`]."
            )]
            pub const fn seconds(self) -> Seconds {
                Seconds::new(self.secs)
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
                write!(f, "{}", self.secs)
            }
        }
    };
}

temporal_scalar!(
    /// A FalkorDB `datetime` value (compact type marker 13): seconds since the Unix epoch (UTC).
    ///
    /// Supports type-safe arithmetic: subtract two `DateTime`s for a [`Duration`], or add/subtract a
    /// [`Duration`] to shift the instant.
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
    ///
    /// Supports type-safe arithmetic: add or subtract `Duration`s, negate one, or shift a
    /// [`DateTime`] by it.
    Duration,
    "duration",
    "16"
);

impl DateTime {
    /// Returns the [`Duration`] elapsed from `earlier` until `self`, or [`None`] on `i64` overflow.
    pub fn checked_duration_since(
        self,
        earlier: DateTime,
    ) -> Option<Duration> {
        self.secs.checked_sub(earlier.secs).map(Duration::new)
    }

    /// Shifts this instant forward by `rhs`, returning [`None`] on `i64` overflow.
    pub fn checked_add(
        self,
        rhs: Duration,
    ) -> Option<DateTime> {
        self.secs.checked_add(rhs.secs).map(DateTime::new)
    }

    /// Shifts this instant backward by `rhs`, returning [`None`] on `i64` overflow.
    pub fn checked_sub(
        self,
        rhs: Duration,
    ) -> Option<DateTime> {
        self.secs.checked_sub(rhs.secs).map(DateTime::new)
    }
}

/// `DateTime - DateTime` yields the [`Duration`] between the two instants.
impl Sub<DateTime> for DateTime {
    type Output = Duration;
    fn sub(
        self,
        rhs: DateTime,
    ) -> Duration {
        Duration::new(self.secs - rhs.secs)
    }
}

/// `DateTime + Duration` shifts the instant forward.
impl Add<Duration> for DateTime {
    type Output = DateTime;
    fn add(
        self,
        rhs: Duration,
    ) -> DateTime {
        DateTime::new(self.secs + rhs.secs)
    }
}

/// `DateTime - Duration` shifts the instant backward.
impl Sub<Duration> for DateTime {
    type Output = DateTime;
    fn sub(
        self,
        rhs: Duration,
    ) -> DateTime {
        DateTime::new(self.secs - rhs.secs)
    }
}

/// `Duration + DateTime` shifts the instant forward (commutative with `DateTime + Duration`).
impl Add<DateTime> for Duration {
    type Output = DateTime;
    fn add(
        self,
        rhs: DateTime,
    ) -> DateTime {
        DateTime::new(self.secs + rhs.secs)
    }
}

impl Duration {
    /// Returns this duration as a [`std::time::Duration`], or [`None`] if it is negative — a
    /// [`std::time::Duration`] cannot represent a negative span.
    pub fn as_std_duration(self) -> Option<std::time::Duration> {
        u64::try_from(self.secs)
            .ok()
            .map(std::time::Duration::from_secs)
    }

    /// Adds another [`Duration`], returning [`None`] on `i64` overflow.
    pub fn checked_add(
        self,
        rhs: Duration,
    ) -> Option<Duration> {
        self.secs.checked_add(rhs.secs).map(Duration::new)
    }

    /// Subtracts another [`Duration`], returning [`None`] on `i64` overflow.
    pub fn checked_sub(
        self,
        rhs: Duration,
    ) -> Option<Duration> {
        self.secs.checked_sub(rhs.secs).map(Duration::new)
    }

    /// Negates this duration, returning [`None`] on `i64` overflow (i.e. `i64::MIN`).
    pub fn checked_neg(self) -> Option<Duration> {
        self.secs.checked_neg().map(Duration::new)
    }
}

impl Add for Duration {
    type Output = Duration;
    fn add(
        self,
        rhs: Duration,
    ) -> Duration {
        Duration::new(self.secs + rhs.secs)
    }
}

impl Sub for Duration {
    type Output = Duration;
    fn sub(
        self,
        rhs: Duration,
    ) -> Duration {
        Duration::new(self.secs - rhs.secs)
    }
}

impl Neg for Duration {
    type Output = Duration;
    fn neg(self) -> Duration {
        Duration::new(-self.secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FalkorDBError;

    #[test]
    fn test_new_seconds_and_display() {
        let dt = DateTime::new(1_700_000_000);
        assert_eq!(dt.seconds(), Seconds::new(1_700_000_000));
        assert_eq!(dt.to_string(), "1700000000");

        let date = Date::new(-697_161_600);
        assert_eq!(date.seconds().get(), -697_161_600);
        assert_eq!(date.to_string(), "-697161600");

        let time = Time::new(3600);
        assert_eq!(time.seconds(), Seconds::new(3600));
        assert_eq!(time.to_string(), "3600");

        let dur = Duration::new(259_200);
        assert_eq!(dur.seconds(), Seconds::new(259_200));
        assert_eq!(dur.to_string(), "259200");
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
    fn test_ordering_and_default() {
        assert_eq!(Date::default(), Date::new(0));
        assert!(Time::new(1) < Time::new(2));
    }

    #[test]
    fn test_seconds_scalar() {
        let s = Seconds::new(10);
        assert_eq!(s.get(), 10);
        assert_eq!(s.to_string(), "10");
        assert_eq!(Seconds::from(5), Seconds::new(5));
        assert_eq!(i64::from(Seconds::new(8)), 8);
        assert_eq!(Seconds::default(), Seconds::new(0));

        // Operators.
        assert_eq!(Seconds::new(3) + Seconds::new(4), Seconds::new(7));
        assert_eq!(Seconds::new(7) - Seconds::new(4), Seconds::new(3));
        assert_eq!(-Seconds::new(3), Seconds::new(-3));

        // Checked arithmetic.
        assert_eq!(
            Seconds::new(10).checked_add(Seconds::new(5)),
            Some(Seconds::new(15))
        );
        assert_eq!(Seconds::new(i64::MAX).checked_add(Seconds::new(1)), None);
        assert_eq!(
            Seconds::new(10).checked_sub(Seconds::new(4)),
            Some(Seconds::new(6))
        );
        assert_eq!(Seconds::new(i64::MIN).checked_sub(Seconds::new(1)), None);
        assert_eq!(Seconds::new(3).checked_neg(), Some(Seconds::new(-3)));
        assert_eq!(Seconds::new(i64::MIN).checked_neg(), None);
    }

    #[test]
    fn test_datetime_duration_algebra() {
        // instant - instant = span
        assert_eq!(DateTime::new(100) - DateTime::new(40), Duration::new(60));
        // instant ± span = instant
        assert_eq!(DateTime::new(100) + Duration::new(5), DateTime::new(105));
        assert_eq!(DateTime::new(100) - Duration::new(5), DateTime::new(95));
        // span + instant = instant (commutative)
        assert_eq!(Duration::new(5) + DateTime::new(100), DateTime::new(105));
        // span arithmetic
        assert_eq!(Duration::new(3) + Duration::new(4), Duration::new(7));
        assert_eq!(Duration::new(7) - Duration::new(4), Duration::new(3));
        assert_eq!(-Duration::new(3), Duration::new(-3));
    }

    #[test]
    fn test_datetime_checked_arithmetic() {
        assert_eq!(
            DateTime::new(10).checked_duration_since(DateTime::new(4)),
            Some(Duration::new(6))
        );
        assert_eq!(
            DateTime::new(i64::MIN).checked_duration_since(DateTime::new(1)),
            None
        );
        assert_eq!(
            DateTime::new(10).checked_add(Duration::new(5)),
            Some(DateTime::new(15))
        );
        assert_eq!(DateTime::new(i64::MAX).checked_add(Duration::new(1)), None);
        assert_eq!(
            DateTime::new(10).checked_sub(Duration::new(3)),
            Some(DateTime::new(7))
        );
        assert_eq!(DateTime::new(i64::MIN).checked_sub(Duration::new(1)), None);
    }

    #[test]
    fn test_duration_helpers_and_checked() {
        let three_days = Duration::new(259_200);
        assert_eq!(three_days.seconds().get(), 259_200);
        assert_eq!(
            three_days.as_std_duration(),
            Some(std::time::Duration::from_secs(259_200))
        );
        // A negative span has no std::time::Duration representation.
        assert_eq!(Duration::new(-1).as_std_duration(), None);

        assert_eq!(
            Duration::new(3).checked_add(Duration::new(4)),
            Some(Duration::new(7))
        );
        assert_eq!(Duration::new(i64::MAX).checked_add(Duration::new(1)), None);
        assert_eq!(
            Duration::new(7).checked_sub(Duration::new(4)),
            Some(Duration::new(3))
        );
        assert_eq!(Duration::new(i64::MIN).checked_sub(Duration::new(1)), None);
        assert_eq!(Duration::new(3).checked_neg(), Some(Duration::new(-3)));
        assert_eq!(Duration::new(i64::MIN).checked_neg(), None);
    }
}
