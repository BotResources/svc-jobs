use crate::domain::ids::{AgentId, AttachmentId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BadAddressReason {
    OtherTree,
    RecipientUnknown,
    RecipientRemoved,
    RecipientCannotRestart,
}

impl BadAddressReason {
    pub fn code(&self) -> &'static str {
        match self {
            Self::OtherTree => "other_tree",
            Self::RecipientUnknown => "recipient_unknown",
            Self::RecipientRemoved => "recipient_removed",
            Self::RecipientCannotRestart => "recipient_cannot_restart",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::OtherTree => "The address works on another Initial Job.",
            Self::RecipientUnknown => "Jobs holds no mail address like this one.",
            Self::RecipientRemoved => "The address was removed with its Run.",
            Self::RecipientCannotRestart => {
                "The agent of the address is finished and a restart of it cannot start."
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadAddress {
    pub address: AgentId,
    pub reason: BadAddressReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MailRefusal {
    AuthorBlank,
    AuthorTooLong,
    SourceConversationInvalid,
    SenderUnknown,
    SenderNotOfRun,
    AttachmentNotUploaded { attachment_id: AttachmentId },
    MailIdReused,
    BadAddresses(Vec<BadAddress>),
    AddressNotYours,
    MailNotYours,
}

impl MailRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::AuthorBlank => "author_blank",
            Self::AuthorTooLong => "author_too_long",
            Self::SourceConversationInvalid => "source_conversation_invalid",
            Self::SenderUnknown => "sender_unknown",
            Self::SenderNotOfRun => "sender_not_of_run",
            Self::AttachmentNotUploaded { .. } => "attachment_not_uploaded",
            Self::MailIdReused => "mail_id_reused",
            Self::BadAddresses(_) => "bad_addresses",
            Self::AddressNotYours => "address_not_yours",
            Self::MailNotYours => "mail_not_yours",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::AuthorBlank => {
                "The author of a mail has at least one character that is not a space."
            }
            Self::AuthorTooLong => "The author of a mail has at most 256 characters.",
            Self::SourceConversationInvalid => "The source conversation of a mail is a UUIDv7.",
            Self::SenderUnknown => "Jobs holds no mail address like the sender address.",
            Self::SenderNotOfRun => {
                "The sender address is not an address of the agents of the Run."
            }
            Self::AttachmentNotUploaded { .. } => {
                "An attachment that the mail names was not uploaded under the mail id."
            }
            Self::MailIdReused => "The mail id belongs to another mail.",
            Self::BadAddresses(_) => "At least one recipient cannot receive the mail.",
            Self::AddressNotYours => "The address is not an address of the agents of the Run.",
            Self::MailNotYours => "The mail was not sent to this address.",
        }
    }
}
