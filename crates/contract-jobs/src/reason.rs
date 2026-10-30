use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Reason<T> {
    Known(T),
    Unknown(String),
}

impl<T> Reason<T> {
    pub fn known(&self) -> Option<&T> {
        match self {
            Self::Known(reason) => Some(reason),
            Self::Unknown(_) => None,
        }
    }
}

impl<T> From<T> for Reason<T> {
    fn from(reason: T) -> Self {
        Self::Known(reason)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::ClaimRefusal;
    use crate::runner_surface::SurfaceReason;

    #[test]
    fn a_reason_outside_the_set_keeps_its_word_and_is_never_read_as_another_reason() {
        let unknown: Reason<SurfaceReason> =
            serde_json::from_value(serde_json::json!("brand_new")).unwrap();
        assert_eq!(unknown, Reason::Unknown("brand_new".to_owned()));
        assert_eq!(unknown.known(), None);
        assert_eq!(serde_json::to_value(&unknown).unwrap(), "brand_new");
        let unknown: Reason<ClaimRefusal> =
            serde_json::from_value(serde_json::json!("job_not_of_run")).unwrap();
        assert_eq!(unknown.known(), None);
    }

    #[test]
    fn a_reason_of_the_set_reads_as_itself() {
        let known: Reason<ClaimRefusal> =
            serde_json::from_value(serde_json::json!("busy")).unwrap();
        assert_eq!(known, Reason::Known(ClaimRefusal::Busy));
        assert_eq!(serde_json::to_value(&known).unwrap(), "busy");
    }
}
