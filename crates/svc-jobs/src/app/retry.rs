use std::future::Future;

use crate::error::ServiceError;

pub async fn on_write_race<T, F, Fut>(attempts: usize, mut attempt: F) -> Result<T, ServiceError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ServiceError>>,
{
    let mut outcome = Err(ServiceError::write_race());
    for _ in 0..attempts {
        outcome = attempt().await;
        if !matches!(&outcome, Err(error) if error.is_write_race()) {
            return outcome;
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::error::Contention;

    async fn attempts_made(attempts: usize, failure: fn() -> ServiceError) -> (usize, bool) {
        let made = AtomicUsize::new(0);
        let outcome: Result<(), ServiceError> = on_write_race(attempts, || async {
            made.fetch_add(1, Ordering::SeqCst);
            Err(failure())
        })
        .await;
        (
            made.load(Ordering::SeqCst),
            matches!(outcome, Err(ServiceError::Contended(_))),
        )
    }

    #[tokio::test]
    async fn a_lost_write_race_is_attempted_again_until_the_budget_is_spent() {
        assert_eq!(attempts_made(5, ServiceError::write_race).await, (5, true));
    }

    #[tokio::test]
    async fn a_tree_lock_timeout_is_not_attempted_again() {
        let timeout = || ServiceError::Contended(Contention::LockTimeout);
        assert_eq!(attempts_made(5, timeout).await, (1, true));
    }

    #[tokio::test]
    async fn a_race_that_clears_ends_the_attempts() {
        let made = AtomicUsize::new(0);
        let outcome = on_write_race(5, || async {
            if made.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(ServiceError::write_race())
            } else {
                Ok(7)
            }
        })
        .await;
        assert_eq!(outcome.ok(), Some(7));
        assert_eq!(made.load(Ordering::SeqCst), 3);
    }
}
