use super::inbox::{Notice, NoticeType};
use super::refusal::MailRefusal;
use crate::domain::ids::{AgentId, MailId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailNoticeKey {
    pub mail_id: MailId,
    pub refusal: &'static str,
    pub bad_addresses: Vec<(AgentId, &'static str)>,
}

impl MailRefusal {
    pub fn leaves_a_mail_notice(&self) -> bool {
        match self {
            Self::AuthorBlank
            | Self::AuthorTooLong
            | Self::SourceConversationInvalid
            | Self::AttachmentNotUploaded { .. }
            | Self::MailIdReused
            | Self::BadAddresses(_) => true,
            Self::SenderUnknown
            | Self::SenderNotOfRun
            | Self::AddressNotYours
            | Self::MailNotYours
            | Self::MailContentErased => false,
        }
    }

    pub fn notice_key(&self, mail_id: MailId) -> MailNoticeKey {
        let mut bad_addresses: Vec<(AgentId, &'static str)> = match self {
            Self::BadAddresses(bad) => bad
                .iter()
                .map(|bad| (bad.address, bad.reason.code()))
                .collect(),
            _ => Vec::new(),
        };
        bad_addresses.sort_by_key(|(address, code)| (address.as_uuid(), *code));
        MailNoticeKey {
            mail_id,
            refusal: self.code(),
            bad_addresses,
        }
    }

    pub fn notice_words(&self) -> String {
        match self {
            Self::MailIdReused => {
                "The mail id already belongs to a mail that Jobs accepted.".to_owned()
            }
            Self::BadAddresses(bad) => {
                let named: Vec<String> = bad
                    .iter()
                    .map(|bad| format!("{} ({})", bad.address.as_uuid(), bad.reason.message()))
                    .collect();
                format!(
                    "At least one recipient cannot receive the mail: {}",
                    named.join(", ")
                )
            }
            other => other.message().to_owned(),
        }
    }
}

pub fn mail_notice(mail_id: MailId, refusal: &MailRefusal) -> Option<Notice> {
    refusal.leaves_a_mail_notice().then(|| Notice {
        notice_type: NoticeType::Refused,
        job_id: None,
        mail_id: Some(mail_id),
        position: None,
        failure_report: None,
        message: Some(refusal.notice_words()),
        answer: None,
        result_id: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ids::AgentId;
    use crate::domain::mail::refusal::{BadAddress, BadAddressReason};
    use uuid::Uuid;

    fn mail() -> MailId {
        MailId::new(Uuid::now_v7())
    }

    #[test]
    fn a_refusal_of_the_mail_itself_leaves_a_notice_that_names_the_mail() {
        let refused = mail();
        for refusal in [
            MailRefusal::AuthorBlank,
            MailRefusal::AuthorTooLong,
            MailRefusal::SourceConversationInvalid,
            MailRefusal::MailIdReused,
            MailRefusal::BadAddresses(vec![]),
        ] {
            let notice = mail_notice(refused, &refusal).expect("a notice");
            assert_eq!(notice.mail_id, Some(refused));
            assert_eq!(notice.job_id, None);
            assert_eq!(notice.notice_type, NoticeType::Refused);
            assert!(notice.message.is_some());
        }
    }

    #[test]
    fn a_refusal_that_says_nothing_about_a_sent_mail_leaves_no_notice() {
        for refusal in [
            MailRefusal::SenderUnknown,
            MailRefusal::SenderNotOfRun,
            MailRefusal::AddressNotYours,
            MailRefusal::MailNotYours,
        ] {
            assert!(mail_notice(mail(), &refusal).is_none(), "{refusal:?}");
        }
    }

    #[test]
    fn a_reused_id_says_that_the_id_belongs_to_a_mail_that_jobs_accepted() {
        let notice = mail_notice(mail(), &MailRefusal::MailIdReused).unwrap();
        assert!(notice.message.unwrap().contains("accepted"));
    }

    #[test]
    fn a_bad_recipient_is_named_with_its_reason() {
        let address = AgentId::new(Uuid::now_v7());
        let refusal = MailRefusal::BadAddresses(vec![BadAddress {
            address,
            reason: BadAddressReason::RecipientRemoved,
        }]);
        let words = mail_notice(mail(), &refusal).unwrap().message.unwrap();
        assert!(words.contains(&address.as_uuid().to_string()));
        assert!(words.contains(BadAddressReason::RecipientRemoved.message()));
    }

    #[test]
    fn the_same_refusal_of_the_same_mail_has_one_key_and_another_reason_or_another_address_has_another()
     {
        let id = mail();
        let (first, second) = (AgentId::new(Uuid::now_v7()), AgentId::new(Uuid::now_v7()));
        let bad = |address| {
            MailRefusal::BadAddresses(vec![BadAddress {
                address,
                reason: BadAddressReason::RecipientUnknown,
            }])
        };
        assert_eq!(bad(first).notice_key(id), bad(first).notice_key(id));
        assert_ne!(bad(first).notice_key(id), bad(second).notice_key(id));
        assert_ne!(
            bad(first).notice_key(id),
            MailRefusal::MailIdReused.notice_key(id)
        );
        assert_ne!(
            MailRefusal::MailIdReused.notice_key(id),
            MailRefusal::MailIdReused.notice_key(mail())
        );
    }

    #[test]
    fn the_order_in_which_bad_recipients_are_found_does_not_change_the_key() {
        let id = mail();
        let (first, second) = (AgentId::new(Uuid::now_v7()), AgentId::new(Uuid::now_v7()));
        let bad = |address, reason| BadAddress { address, reason };
        let one = MailRefusal::BadAddresses(vec![
            bad(first, BadAddressReason::RecipientUnknown),
            bad(second, BadAddressReason::OtherTree),
        ]);
        let other = MailRefusal::BadAddresses(vec![
            bad(second, BadAddressReason::OtherTree),
            bad(first, BadAddressReason::RecipientUnknown),
        ]);
        assert_eq!(one.notice_key(id), other.notice_key(id));
    }

    #[test]
    fn a_mail_notice_carries_no_subject_and_no_job() {
        let notice = mail_notice(mail(), &MailRefusal::AuthorBlank).unwrap();
        let stored = serde_json::to_value(&notice).unwrap();
        assert!(stored.get("subject").is_none());
        assert!(stored.get("job_id").is_none());
    }
}
