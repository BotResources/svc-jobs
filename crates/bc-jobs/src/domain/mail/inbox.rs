use crate::error::JobsError;

pub use super::notice::{Notice, NoticeSubject, NoticeType};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ItemKind {
    Mail,
    CcNotice,
    Notice,
    MailNotice,
}

impl ItemKind {
    pub const WAKING: [Self; 2] = [Self::Mail, Self::MailNotice];

    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Mail => "MAIL",
            Self::CcNotice => "CC_NOTICE",
            Self::Notice => "NOTICE",
            Self::MailNotice => "MAIL_NOTICE",
        }
    }

    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            "MAIL" => Ok(Self::Mail),
            "CC_NOTICE" => Ok(Self::CcNotice),
            "NOTICE" => Ok(Self::Notice),
            "MAIL_NOTICE" => Ok(Self::MailNotice),
            other => Err(JobsError::UnknownEnumValue {
                field: "item_kind",
                value: other.to_owned(),
            }),
        }
    }

    pub fn guard_notice_shape(self, notice: Option<&Notice>) -> Result<(), JobsError> {
        let fits = match (self, notice) {
            (Self::Mail | Self::CcNotice, None) => true,
            (Self::Notice, Some(notice)) => notice.mail_id().is_none(),
            (Self::MailNotice, Some(notice)) => {
                notice.notice_type == NoticeType::Refused && notice.mail_id().is_some()
            }
            _ => false,
        };
        if fits {
            Ok(())
        } else {
            Err(JobsError::CorruptState {
                reason_code: "inbox_item_shape",
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ids::MailId;
    use crate::fixtures::job_id;
    use uuid::Uuid;

    #[test]
    fn an_item_kind_maps_totally_to_the_database() {
        for kind in [
            ItemKind::Mail,
            ItemKind::CcNotice,
            ItemKind::Notice,
            ItemKind::MailNotice,
        ] {
            assert_eq!(ItemKind::from_db_str(kind.as_db_str()), Ok(kind));
        }
        assert_eq!(ItemKind::MailNotice.as_db_str(), "MAIL_NOTICE");
        assert!(ItemKind::from_db_str("OTHER").is_err());
    }

    #[test]
    fn a_mail_and_a_mail_notice_are_the_item_kinds_that_wake() {
        assert_eq!(ItemKind::WAKING, [ItemKind::Mail, ItemKind::MailNotice]);
    }

    #[test]
    fn a_row_loads_only_when_its_kind_and_its_notice_fit() {
        let job = Notice::of_job(NoticeType::Refused, job_id());
        let mail = Notice::of_mail(NoticeType::Refused, MailId::new(Uuid::now_v7()));
        let mail_started = Notice::of_mail(NoticeType::Started, MailId::new(Uuid::now_v7()));
        assert!(ItemKind::Mail.guard_notice_shape(None).is_ok());
        assert!(ItemKind::CcNotice.guard_notice_shape(None).is_ok());
        assert!(ItemKind::Notice.guard_notice_shape(Some(&job)).is_ok());
        assert!(ItemKind::MailNotice.guard_notice_shape(Some(&mail)).is_ok());
        for (kind, notice) in [
            (ItemKind::Mail, Some(&job)),
            (ItemKind::Notice, None),
            (ItemKind::Notice, Some(&mail)),
            (ItemKind::MailNotice, None),
            (ItemKind::MailNotice, Some(&job)),
            (ItemKind::MailNotice, Some(&mail_started)),
        ] {
            assert!(kind.guard_notice_shape(notice).is_err(), "{kind:?}");
        }
    }
}
