use super::refusal::MailRefusal;
use crate::domain::ids::AgentId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentBody {
    pub name: String,
    pub content_type: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAttachment {
    pub uploader: AgentId,
    pub body: AttachmentBody,
    pub mail_sent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachmentAdmission {
    Absorb,
    Store,
    Refuse(MailRefusal),
}

pub fn decide_attachment_upload(
    stored: Option<&StoredAttachment>,
    from: AgentId,
    incoming: &AttachmentBody,
) -> AttachmentAdmission {
    let Some(known) = stored else {
        return AttachmentAdmission::Store;
    };
    if known.uploader != from {
        return AttachmentAdmission::Refuse(MailRefusal::MailIdReused);
    }
    if &known.body == incoming {
        return AttachmentAdmission::Absorb;
    }
    if known.mail_sent {
        return AttachmentAdmission::Refuse(MailRefusal::MailIdReused);
    }
    AttachmentAdmission::Store
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn agent() -> AgentId {
        AgentId::new(Uuid::now_v7())
    }

    fn body(content: &str) -> AttachmentBody {
        AttachmentBody {
            name: "n".to_owned(),
            content_type: "text/plain".to_owned(),
            content: content.to_owned(),
        }
    }

    fn stored(uploader: AgentId, content: &str, mail_sent: bool) -> StoredAttachment {
        StoredAttachment {
            uploader,
            body: body(content),
            mail_sent,
        }
    }

    #[test]
    fn a_first_attachment_is_stored_and_an_identical_repeat_by_its_uploader_is_absorbed() {
        let uploader = agent();
        assert_eq!(
            decide_attachment_upload(None, uploader, &body("a")),
            AttachmentAdmission::Store
        );
        for sent in [false, true] {
            assert_eq!(
                decide_attachment_upload(Some(&stored(uploader, "a", sent)), uploader, &body("a")),
                AttachmentAdmission::Absorb
            );
        }
    }

    #[test]
    fn another_content_replaces_an_unsent_attachment_of_the_same_uploader_only() {
        let uploader = agent();
        assert_eq!(
            decide_attachment_upload(
                Some(&stored(uploader, "a", false)),
                uploader,
                &body("changed")
            ),
            AttachmentAdmission::Store
        );
        assert_eq!(
            decide_attachment_upload(
                Some(&stored(uploader, "a", true)),
                uploader,
                &body("changed")
            ),
            AttachmentAdmission::Refuse(MailRefusal::MailIdReused)
        );
    }

    #[test]
    fn an_attachment_id_held_by_another_uploader_is_never_touched() {
        for content in ["a", "changed"] {
            for sent in [false, true] {
                assert_eq!(
                    decide_attachment_upload(
                        Some(&stored(agent(), "a", sent)),
                        agent(),
                        &body(content)
                    ),
                    AttachmentAdmission::Refuse(MailRefusal::MailIdReused)
                );
            }
        }
    }
}
