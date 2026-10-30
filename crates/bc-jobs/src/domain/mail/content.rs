use super::author::{MailAuthor, SourceConversation};
use super::refusal::MailRefusal;
use crate::domain::ids::{AgentId, AttachmentId, MailId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentRef {
    pub attachment_id: AttachmentId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailContent {
    pub mail_id: MailId,
    pub from: AgentId,
    pub author: MailAuthor,
    pub source_conversation: SourceConversation,
    pub subject: String,
    pub body: String,
    pub to: Vec<AgentId>,
    pub cc: Vec<AgentId>,
    pub attachments: Vec<AttachmentRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendAdmission {
    New,
    Absorbed,
}

impl MailContent {
    pub fn canonical(mut self) -> Self {
        self.to.sort();
        self.to.dedup();
        self.cc.sort();
        self.cc.dedup();
        let direct = self.to.clone();
        self.cc.retain(|address| !direct.contains(address));
        self.attachments
            .sort_by_key(|attachment| attachment.attachment_id);
        self.attachments
            .dedup_by_key(|attachment| attachment.attachment_id);
        self
    }

    pub fn admit(
        existing: Option<&MailContent>,
        incoming: &MailContent,
    ) -> Result<SendAdmission, MailRefusal> {
        match existing {
            None => Ok(SendAdmission::New),
            Some(known) if known == incoming => Ok(SendAdmission::Absorbed),
            Some(_) => Err(MailRefusal::MailIdReused),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn agent() -> AgentId {
        AgentId::new(Uuid::now_v7())
    }

    fn mail(to: Vec<AgentId>, cc: Vec<AgentId>) -> MailContent {
        MailContent {
            mail_id: MailId::new(Uuid::now_v7()),
            from: agent(),
            author: MailAuthor::new("Amelie").unwrap(),
            source_conversation: SourceConversation::new(Uuid::now_v7()).unwrap(),
            subject: "subject".to_owned(),
            body: "body".to_owned(),
            to,
            cc,
            attachments: vec![],
        }
        .canonical()
    }

    #[test]
    fn the_same_mail_sent_again_is_absorbed_whatever_the_order_of_its_recipients() {
        let (a, b) = (agent(), agent());
        let first = mail(vec![a, b], vec![]);
        let mut again = first.clone();
        again.to = vec![b, a];
        assert_eq!(
            MailContent::admit(Some(&first), &again.canonical()),
            Ok(SendAdmission::Absorbed)
        );
    }

    #[test]
    fn a_known_mail_id_with_another_mail_is_refused_for_each_field() {
        let first = mail(vec![agent()], vec![]);
        let changed: Vec<MailContent> = vec![
            MailContent {
                subject: "other".to_owned(),
                ..first.clone()
            },
            MailContent {
                body: "other".to_owned(),
                ..first.clone()
            },
            MailContent {
                author: MailAuthor::new("Bruno").unwrap(),
                ..first.clone()
            },
            MailContent {
                source_conversation: SourceConversation::new(Uuid::now_v7()).unwrap(),
                ..first.clone()
            },
            MailContent {
                to: vec![agent()],
                ..first.clone()
            },
            MailContent {
                cc: vec![agent()],
                ..first.clone()
            },
            MailContent {
                from: agent(),
                ..first.clone()
            },
            MailContent {
                attachments: vec![AttachmentRef {
                    attachment_id: AttachmentId::new(Uuid::now_v7()),
                    name: "a".to_owned(),
                }],
                ..first.clone()
            },
        ];
        for other in changed {
            assert_eq!(
                MailContent::admit(Some(&first), &other),
                Err(MailRefusal::MailIdReused)
            );
        }
    }

    #[test]
    fn an_unknown_mail_id_is_new() {
        assert_eq!(
            MailContent::admit(None, &mail(vec![agent()], vec![])),
            Ok(SendAdmission::New)
        );
    }

    #[test]
    fn a_recipient_named_direct_and_cc_receives_the_mail_once_as_direct() {
        let a = agent();
        let canonical = mail(vec![a, a], vec![a]);
        assert_eq!(canonical.to, vec![a]);
        assert!(canonical.cc.is_empty());
    }
}
