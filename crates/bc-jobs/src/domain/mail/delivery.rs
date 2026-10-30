use super::refusal::{BadAddress, BadAddressReason, MailRefusal};
use crate::domain::directory::listing::{DirectoryEntry, RestartObstacles, RestartStanding};
use crate::domain::ids::{AgentId, JobId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecipientKind {
    Direct,
    Cc,
}

impl RecipientKind {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Direct => "DIRECT",
            Self::Cc => "CC",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldAddress {
    pub entry: DirectoryEntry,
    pub initial_job_id: JobId,
    pub obstacles: RestartObstacles,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressStanding {
    Unknown,
    Removed,
    Held(HeldAddress),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recipient {
    pub address: AgentId,
    pub kind: RecipientKind,
    pub standing: AddressStanding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefusedRestart {
    pub address: AgentId,
}

impl RefusedRestart {
    pub fn code(&self) -> &'static str {
        "restart_refused_type_retired"
    }

    pub fn message(&self) -> &'static str {
        "The agent restarts on a retired runner type: the mail stays unread at this address."
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    pub address: AgentId,
    pub kind: RecipientKind,
    pub rings_live_run: bool,
    pub starts_restart: bool,
    pub refused_restart: Option<RefusedRestart>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deliveries(pub Vec<Delivery>);

pub fn decide_deliveries(
    sender_initial_job: JobId,
    recipients: &[Recipient],
) -> Result<Deliveries, MailRefusal> {
    let mut deliveries = Vec::with_capacity(recipients.len());
    let mut bad = Vec::new();
    for recipient in recipients {
        match decide_one(sender_initial_job, recipient) {
            Ok(delivery) => deliveries.push(delivery),
            Err(reason) => bad.push(BadAddress {
                address: recipient.address,
                reason,
            }),
        }
    }
    if bad.is_empty() {
        Ok(Deliveries(deliveries))
    } else {
        Err(MailRefusal::BadAddresses(bad))
    }
}

fn decide_one(
    sender_initial_job: JobId,
    recipient: &Recipient,
) -> Result<Delivery, BadAddressReason> {
    let held = match &recipient.standing {
        AddressStanding::Unknown => return Err(BadAddressReason::RecipientUnknown),
        AddressStanding::Removed => return Err(BadAddressReason::RecipientRemoved),
        AddressStanding::Held(held) => held,
    };
    if held.entry.mail_address().is_none() {
        return Err(BadAddressReason::RecipientUnknown);
    }
    if held.initial_job_id != sender_initial_job {
        return Err(BadAddressReason::OtherTree);
    }
    let delivery = |rings_live_run, starts_restart, refused_restart| Delivery {
        address: recipient.address,
        kind: recipient.kind,
        rings_live_run,
        starts_restart: starts_restart && recipient.kind == RecipientKind::Direct,
        refused_restart,
    };
    match held.entry.restart_standing(held.obstacles) {
        RestartStanding::Running => Ok(delivery(true, false, None)),
        RestartStanding::NoSavedState | RestartStanding::ParentJobTerminal => {
            Err(BadAddressReason::RecipientCannotRestart)
        }
        RestartStanding::TypeRetired => {
            let refused = (recipient.kind == RecipientKind::Direct).then_some(RefusedRestart {
                address: recipient.address,
            });
            Ok(delivery(false, false, refused))
        }
        RestartStanding::RestartInFlight => Ok(delivery(true, false, None)),
        RestartStanding::Allowed => Ok(delivery(false, true, None)),
    }
}

#[cfg(test)]
mod tests;
