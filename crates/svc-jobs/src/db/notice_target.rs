use bc_jobs::domain::ids::{AgentId, RunId};
use bc_jobs::domain::mail::inbox::ItemKind;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupKey(String);

impl DedupKey {
    pub fn of_text(text: String) -> Self {
        Self(text)
    }

    pub fn of_command(command: Uuid) -> Self {
        Self(command.to_string())
    }

    pub fn text(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subject {
    Run,
    Address(AgentId),
    AddressOnce { address: AgentId, key: DedupKey },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoticeFor {
    pub run_id: RunId,
    pub kind: ItemKind,
    pub subject: Subject,
}

impl NoticeFor {
    pub fn call(run_id: RunId, address: Option<AgentId>) -> Self {
        Self {
            run_id,
            kind: ItemKind::Notice,
            subject: address.map_or(Subject::Run, Subject::Address),
        }
    }

    pub fn refusal(run_id: RunId, address: AgentId, key: Option<DedupKey>) -> Self {
        Self {
            run_id,
            kind: ItemKind::Notice,
            subject: match key {
                Some(key) => Subject::AddressOnce { address, key },
                None => Subject::Address(address),
            },
        }
    }

    pub fn mail_notice(run_id: RunId, address: AgentId, key: DedupKey) -> Self {
        Self {
            run_id,
            kind: ItemKind::MailNotice,
            subject: Subject::AddressOnce { address, key },
        }
    }

    pub fn address(&self) -> Option<AgentId> {
        match &self.subject {
            Subject::Run => None,
            Subject::Address(address) | Subject::AddressOnce { address, .. } => Some(*address),
        }
    }

    pub fn key(&self) -> Option<&str> {
        match &self.subject {
            Subject::AddressOnce { key, .. } => Some(key.text()),
            Subject::Run | Subject::Address(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run() -> RunId {
        RunId::new(Uuid::now_v7()).expect("a v7 run id")
    }

    #[test]
    fn a_dedup_key_exists_only_together_with_an_address() {
        let address = AgentId::new(Uuid::now_v7());
        assert_eq!(NoticeFor::call(run(), None).key(), None);
        assert_eq!(NoticeFor::call(run(), None).address(), None);
        let once = NoticeFor::mail_notice(run(), address, DedupKey::of_text("k".to_owned()));
        assert_eq!((once.address(), once.key()), (Some(address), Some("k")));
        let plain = NoticeFor::refusal(run(), address, None);
        assert_eq!((plain.address(), plain.key()), (Some(address), None));
    }
}
