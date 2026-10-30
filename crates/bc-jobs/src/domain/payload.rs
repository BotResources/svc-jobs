use serde::{Deserialize, Serialize};

use crate::error::JobsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadAttachment {
    pub name: String,
    pub content_type: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "form", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobPayload {
    Inline {
        message: String,
        attachments: Vec<PayloadAttachment>,
    },
    Kept,
    Reference {
        service: String,
        document_id: String,
    },
}

impl JobPayload {
    pub fn inline_content_size(message: &str, attachments: &[PayloadAttachment]) -> usize {
        message.len()
            + attachments
                .iter()
                .map(|attachment| {
                    attachment.name.len() + attachment.content_type.len() + attachment.content.len()
                })
                .sum::<usize>()
    }

    pub fn inline_size(&self) -> usize {
        match self {
            Self::Inline {
                message,
                attachments,
            } => Self::inline_content_size(message, attachments),
            Self::Kept | Self::Reference { .. } => 0,
        }
    }

    pub fn guard_inline_within(&self, maximum: usize) -> Result<(), JobsError> {
        let size = self.inline_size();
        if size > maximum {
            Err(JobsError::PayloadTooLarge { size, maximum })
        } else {
            Ok(())
        }
    }

    pub fn is_kept(&self) -> bool {
        matches!(self, Self::Kept)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CallMode {
    Sync,
    Background,
}

impl CallMode {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Sync => "SYNC",
            Self::Background => "BACKGROUND",
        }
    }

    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            "SYNC" => Ok(Self::Sync),
            "BACKGROUND" => Ok(Self::Background),
            other => Err(JobsError::UnknownEnumValue {
                field: "call_mode",
                value: other.to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_inline_payload_counts_against_the_inline_bound() {
        let inline = JobPayload::Inline {
            message: "hello".to_owned(),
            attachments: vec![PayloadAttachment {
                name: "a".to_owned(),
                content_type: "text/plain".to_owned(),
                content: "xyz".to_owned(),
            }],
        };
        assert_eq!(inline.inline_size(), 5 + 1 + 10 + 3);
        assert_eq!(JobPayload::Kept.inline_size(), 0);
        assert_eq!(
            inline.guard_inline_within(18),
            Err(JobsError::PayloadTooLarge {
                size: 19,
                maximum: 18
            })
        );
        assert_eq!(inline.guard_inline_within(19), Ok(()));
    }
}
