use uuid::Uuid;

use super::refusal::MailRefusal;

pub const AUTHOR_MAX_CHARACTERS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailAuthor(String);

impl MailAuthor {
    pub fn new(value: impl Into<String>) -> Result<Self, MailRefusal> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(MailRefusal::AuthorBlank);
        }
        if value.chars().count() > AUTHOR_MAX_CHARACTERS {
            return Err(MailRefusal::AuthorTooLong);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceConversation(Uuid);

impl SourceConversation {
    pub fn new(value: Uuid) -> Result<Self, MailRefusal> {
        if value.get_version_num() == 7 {
            Ok(Self(value))
        } else {
            Err(MailRefusal::SourceConversationInvalid)
        }
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_author_keeps_the_text_the_sender_gave() {
        let author = MailAuthor::new("  Amelie  ").unwrap();
        assert_eq!(author.as_str(), "  Amelie  ");
    }

    #[test]
    fn an_author_with_nothing_but_spaces_or_none_is_blank() {
        assert_eq!(MailAuthor::new(""), Err(MailRefusal::AuthorBlank));
        assert_eq!(MailAuthor::new(" \t\n"), Err(MailRefusal::AuthorBlank));
    }

    #[test]
    fn an_author_counts_characters_not_bytes() {
        assert!(MailAuthor::new("é".repeat(AUTHOR_MAX_CHARACTERS)).is_ok());
        assert_eq!(
            MailAuthor::new("é".repeat(AUTHOR_MAX_CHARACTERS + 1)),
            Err(MailRefusal::AuthorTooLong)
        );
    }

    #[test]
    fn a_source_conversation_is_a_uuid_v7() {
        assert!(SourceConversation::new(Uuid::now_v7()).is_ok());
        assert_eq!(
            SourceConversation::new(
                Uuid::parse_str("6f8c3b4e-1f2a-4b6d-8c1e-2f3a4b5c6d7e").unwrap()
            ),
            Err(MailRefusal::SourceConversationInvalid)
        );
        assert_eq!(
            SourceConversation::new(Uuid::nil()),
            Err(MailRefusal::SourceConversationInvalid)
        );
    }
}
