use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::policy::Jitter;

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub trait IdFactory: Send + Sync {
    fn next(&self) -> Uuid;
}

pub trait JitterSource: Send + Sync {
    fn draw(&self) -> Jitter;
}
