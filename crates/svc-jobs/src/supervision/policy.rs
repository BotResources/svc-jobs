//! When a lost background task is tried again.
//!
//! There is no restart budget: a task that serves the bus is retried for as long
//! as the process lives. A budget turns any outage longer than its sum into a
//! pod that stays NOT READY until an operator restarts it, while `/livez` keeps
//! answering — so nothing ever does. The wait between two attempts doubles from
//! `initial_backoff` up to `max_backoff`, and an attempt that lived at least
//! `stability` starts the doubling over.
//!
//! Each wait is spread over `[backoff / 2, backoff]` so that the many tasks one
//! outage drops do not all hit the bus back in the same instant.

use std::hash::BuildHasher;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct RestartPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub stability: Duration,
}

/// Where a wait falls inside its spread, in basis points: `0` is the shortest
/// wait (half the backoff), `10_000` the full backoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Spread(u32);

impl Spread {
    #[cfg(test)]
    pub const MIN: Self = Self(0);
    #[cfg(test)]
    pub const MAX: Self = Self(BASIS);

    /// A fresh, unpredictable point of the spread.
    pub fn draw() -> Self {
        // `RandomState` is seeded from the operating system's randomness and
        // re-keyed on every construction: hashing a constant through a fresh one
        // is an unpredictable draw, without a dependency for one number.
        let entropy = std::collections::hash_map::RandomState::new().hash_one(0u8);
        Self(u32::try_from(entropy % u64::from(BASIS + 1)).unwrap_or(BASIS / 2))
    }

    #[cfg(test)]
    pub fn at(basis_points: u32) -> Self {
        Self(basis_points.min(BASIS))
    }
}

const BASIS: u32 = 10_000;

impl RestartPolicy {
    /// The ceiling of the wait after `consecutive_restarts` quick failures:
    /// doubling from the initial backoff, flat at the maximum.
    pub(super) fn backoff(&self, consecutive_restarts: u32) -> Duration {
        let factor = 1u32 << consecutive_restarts.min(16);
        self.initial_backoff
            .saturating_mul(factor)
            .min(self.max_backoff)
    }

    /// The wait actually taken: within `[backoff / 2, backoff]`, never above
    /// `max_backoff`.
    pub(super) fn delay(&self, consecutive_restarts: u32, spread: Spread) -> Duration {
        let ceiling = self.backoff(consecutive_restarts);
        let floor = ceiling / 2;
        let span = ceiling - floor;
        floor + span.mul_f64(f64::from(spread.0) / f64::from(BASIS))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RestartPolicy {
        RestartPolicy {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            stability: Duration::from_secs(60),
        }
    }

    #[test]
    fn the_restart_backoff_doubles_up_to_its_ceiling_and_never_overflows() {
        // Given: a policy backing off from 100ms up to 30s
        let policy = policy();

        // When/Then: each consecutive restart doubles the ceiling, then flattens at the maximum
        assert_eq!(policy.backoff(0), Duration::from_millis(100));
        assert_eq!(policy.backoff(1), Duration::from_millis(200));
        assert_eq!(policy.backoff(3), Duration::from_millis(800));
        assert_eq!(policy.backoff(8), Duration::from_millis(25_600));
        assert_eq!(policy.backoff(9), Duration::from_secs(30));

        // Then: a task failing for ever shifts no further than the clamp, whatever the count
        assert_eq!(policy.backoff(u32::MAX), Duration::from_secs(30));
    }

    #[test]
    fn a_jittered_wait_stays_between_half_its_backoff_and_the_full_backoff() {
        // Given: a policy, and every restart count from the first to far past the ceiling
        let policy = policy();
        for restarts in [0, 1, 2, 5, 8, 9, 10, 40, u32::MAX] {
            let ceiling = policy.backoff(restarts);

            // When: the wait is drawn at both ends of the spread and in between
            let shortest = policy.delay(restarts, Spread::MIN);
            let longest = policy.delay(restarts, Spread::MAX);
            let middle = policy.delay(restarts, Spread::at(BASIS / 2));

            // Then: the spread is exactly [backoff / 2, backoff], so no wait ever collapses to
            // zero (a hot loop against a dead bus) nor exceeds the backoff
            assert_eq!(shortest, ceiling / 2, "restart {restarts}");
            assert_eq!(longest, ceiling, "restart {restarts}");
            assert!(
                shortest <= middle && middle <= longest,
                "restart {restarts}"
            );

            // Then: the maximum backoff stays a ceiling, jitter or not
            assert!(longest <= policy.max_backoff, "restart {restarts}");
        }
    }

    #[test]
    fn every_drawn_wait_falls_inside_the_spread() {
        // Given: a task long past the ceiling, its wait drawn many times
        let policy = policy();
        let ceiling = policy.backoff(20);
        let mut distinct = std::collections::BTreeSet::new();
        for _ in 0..1_000 {
            // When: the production draw picks a point of the spread
            let wait = policy.delay(20, Spread::draw());

            // Then: it is within [max / 2, max]
            assert!(wait >= ceiling / 2 && wait <= ceiling, "{wait:?}");
            distinct.insert(wait);
        }

        // Then: the draws actually spread — tasks dropped together do not retry in lockstep
        assert!(
            distinct.len() > 100,
            "{} distinct waits in 1000 draws",
            distinct.len()
        );
    }
}
