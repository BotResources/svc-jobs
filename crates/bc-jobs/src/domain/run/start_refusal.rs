use crate::domain::unreadable_request::UNREADABLE_REQUEST;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartRefusal {
    RunUnknown,
    InstanceUnknown,
    RunHeld,
    RunReservedElsewhere,
    RunNotPending,
    Malformed,
}

impl StartRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::RunUnknown => "run_unknown",
            Self::InstanceUnknown => "instance_unknown",
            Self::RunHeld => "run_held",
            Self::RunReservedElsewhere => "run_reserved_elsewhere",
            Self::RunNotPending => "run_not_pending",
            Self::Malformed => "malformed",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::RunUnknown => "Jobs holds no such run.",
            Self::InstanceUnknown => {
                "Jobs has no presence for the instance that sent the request yet."
            }
            Self::RunHeld => "The run is held by an instance under another start.",
            Self::RunReservedElsewhere => "A claim reserved the run for another instance.",
            Self::RunNotPending => "The run has ended.",
            Self::Malformed => UNREADABLE_REQUEST,
        }
    }
}
