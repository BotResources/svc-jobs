use bc_jobs::domain::policy::Jitter;
use bc_jobs::ports::environment::{Clock, IdFactory, JitterSource};
use chrono::{DateTime, SubsecRound, Utc};
use uuid::Uuid;

const TIMESTAMPTZ_SUBSECOND_DIGITS: u16 = 6;

fn at_stored_precision(instant: DateTime<Utc>) -> DateTime<Utc> {
    instant.trunc_subsecs(TIMESTAMPTZ_SUBSECOND_DIGITS)
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        at_stored_precision(Utc::now())
    }
}

pub struct UuidV7Factory;

impl IdFactory for UuidV7Factory {
    fn next(&self) -> Uuid {
        Uuid::now_v7()
    }
}

pub struct NanosecondJitter;

const BASIS: i64 = 10_000;

impl JitterSource for NanosecondJitter {
    fn draw(&self) -> Jitter {
        let nanoseconds = i64::from(Utc::now().timestamp_subsec_nanos());
        Jitter::from_basis_points(nanoseconds % (BASIS + 1)).unwrap_or(Jitter::MIDPOINT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NANOSECOND_READINGS: &[i64] = &[
        1_786_000_000_468_275_450,
        1_786_000_000_904_456_360,
        1_786_000_000_000_000_001,
        1_786_000_000_999_999_999,
        1_786_000_000_000_000_999,
    ];

    fn survives_a_timestamptz_round_trip(instant: DateTime<Utc>) -> bool {
        DateTime::from_timestamp_micros(instant.timestamp_micros()) == Some(instant)
    }

    #[test]
    fn a_clock_reading_carrying_nanoseconds_is_minted_at_the_precision_the_column_keeps() {
        // Given: the readings a nanosecond-resolution host clock returns
        for nanoseconds in NANOSECOND_READINGS {
            let reading = DateTime::from_timestamp_nanos(*nanoseconds);
            assert!(
                !survives_a_timestamptz_round_trip(reading),
                "this reading carries no sub-microsecond digits, so it proves nothing: {reading}",
            );
            // When: the service mints an instant from it
            let minted = at_stored_precision(reading);
            // Then: what an event carries and what a later read returns are the same instant
            assert!(
                survives_a_timestamptz_round_trip(minted),
                "the minted instant does not survive its own storage round-trip: {minted}",
            );
            assert_eq!(
                minted.timestamp_micros(),
                reading.timestamp_micros(),
                "minting moved the instant instead of dropping the digits it cannot keep",
            );
        }
    }

    #[test]
    fn the_service_clock_mints_no_precision_a_timestamptz_column_would_drop() {
        // Given: the clock every command reads its instants from
        let clock = SystemClock;
        for _ in 0..10_000 {
            // When: it mints an instant an event carries and a column stores
            let minted = clock.now();
            // Then: no sub-microsecond digit ever reaches a subscriber
            assert!(
                survives_a_timestamptz_round_trip(minted),
                "the clock minted sub-microsecond digits Postgres cannot keep: {minted}",
            );
        }
    }
}
