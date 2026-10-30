use std::collections::HashSet;

use super::delivery::RecipientKind;
use super::refusal::MailRefusal;
use crate::domain::ids::{AgentId, ItemId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ItemNotYours;

pub fn guard_address_is_of_the_run(
    address: AgentId,
    addresses_of_run: &[AgentId],
) -> Result<(), MailRefusal> {
    if addresses_of_run.contains(&address) {
        Ok(())
    } else {
        Err(MailRefusal::AddressNotYours)
    }
}

pub fn guard_reads_mail(
    address: AgentId,
    addresses_of_run: &[AgentId],
    recipient_kind: Option<RecipientKind>,
) -> Result<RecipientKind, MailRefusal> {
    guard_address_is_of_the_run(address, addresses_of_run)?;
    recipient_kind.ok_or(MailRefusal::MailNotYours)
}

pub fn guard_marks_only_own_items(
    requested: &[ItemId],
    reachable: &HashSet<ItemId>,
) -> Result<(), ItemNotYours> {
    if requested.iter().all(|item| reachable.contains(item)) {
        Ok(())
    } else {
        Err(ItemNotYours)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn address() -> AgentId {
        AgentId::new(Uuid::now_v7())
    }

    #[test]
    fn an_address_of_another_run_is_not_yours_before_the_mail_is_looked_at() {
        assert_eq!(
            guard_reads_mail(address(), &[address()], Some(RecipientKind::Direct)),
            Err(MailRefusal::AddressNotYours)
        );
    }

    #[test]
    fn a_mail_that_was_not_sent_to_the_address_is_not_yours_and_the_refusal_names_no_subject() {
        let mine = address();
        let refusal = guard_reads_mail(mine, &[mine], None).unwrap_err();
        assert_eq!(refusal, MailRefusal::MailNotYours);
        assert!(!refusal.message().contains("subject"));
    }

    #[test]
    fn a_direct_and_a_cc_recipient_both_read() {
        let mine = address();
        for kind in [RecipientKind::Direct, RecipientKind::Cc] {
            assert_eq!(guard_reads_mail(mine, &[mine], Some(kind)), Ok(kind));
        }
    }

    #[test]
    fn one_foreign_item_refuses_the_whole_marker() {
        let own = ItemId::new(Uuid::now_v7()).unwrap();
        let foreign = ItemId::new(Uuid::now_v7()).unwrap();
        let reachable = HashSet::from([own]);
        assert_eq!(guard_marks_only_own_items(&[own], &reachable), Ok(()));
        assert_eq!(
            guard_marks_only_own_items(&[own, foreign], &reachable),
            Err(ItemNotYours)
        );
    }
}
