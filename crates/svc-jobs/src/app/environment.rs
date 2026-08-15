use bc_jobs::domain::policy::Jitter;
use bc_jobs::ports::environment::{Clock, IdFactory, JitterSource};
use chrono::{DateTime, Utc};
use uuid::Uuid;

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
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
