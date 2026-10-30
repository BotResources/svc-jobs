use super::answer::ResultRefusal;
use super::kept_payload::UploadRefusal;
use super::saved_state::SavedStateRefusal;
use super::work::WorkRefusal;
use crate::domain::mail::refusal::{BadAddress, MailRefusal};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerRefusal {
    Work(WorkRefusal),
    Mail(MailRefusal),
    Upload(UploadRefusal),
    Result(ResultRefusal),
    SavedState(SavedStateRefusal),
    NoKeptPayload,
}

impl RunnerRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Work(refusal) => refusal.code(),
            Self::Mail(refusal) => refusal.code(),
            Self::Upload(refusal) => refusal.code(),
            Self::Result(refusal) => refusal.code(),
            Self::SavedState(refusal) => refusal.code(),
            Self::NoKeptPayload => "no_kept_payload",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::Work(refusal) => refusal.message(),
            Self::Mail(refusal) => refusal.message(),
            Self::Upload(refusal) => refusal.message(),
            Self::Result(refusal) => refusal.message(),
            Self::SavedState(refusal) => refusal.message(),
            Self::NoKeptPayload => "The Job has no payload kept by Jobs.",
        }
    }

    pub fn bad_addresses(&self) -> &[BadAddress] {
        match self {
            Self::Mail(MailRefusal::BadAddresses(bad)) => bad,
            _ => &[],
        }
    }
}

impl From<WorkRefusal> for RunnerRefusal {
    fn from(refusal: WorkRefusal) -> Self {
        Self::Work(refusal)
    }
}

impl From<MailRefusal> for RunnerRefusal {
    fn from(refusal: MailRefusal) -> Self {
        Self::Mail(refusal)
    }
}

impl From<UploadRefusal> for RunnerRefusal {
    fn from(refusal: UploadRefusal) -> Self {
        Self::Upload(refusal)
    }
}

impl From<ResultRefusal> for RunnerRefusal {
    fn from(refusal: ResultRefusal) -> Self {
        Self::Result(refusal)
    }
}

impl From<SavedStateRefusal> for RunnerRefusal {
    fn from(refusal: SavedStateRefusal) -> Self {
        Self::SavedState(refusal)
    }
}
