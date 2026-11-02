use uuid::Uuid;

pub const ERASED_TEXT: &str = "erased";
pub const ERASED_CONVERSATION_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_7000_8000_0000_0000_0000);

pub fn mail_is_between_removed_addresses(
    sender_removed: bool,
    recipients_removed: &[bool],
) -> bool {
    sender_removed && recipients_removed.iter().all(|removed| *removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mail_is_erased_only_when_its_sender_and_every_recipient_are_removed() {
        assert!(mail_is_between_removed_addresses(true, &[true, true]));
        assert!(mail_is_between_removed_addresses(true, &[]));
        assert!(!mail_is_between_removed_addresses(false, &[true, true]));
        assert!(!mail_is_between_removed_addresses(true, &[true, false]));
        assert!(!mail_is_between_removed_addresses(false, &[false]));
    }
    use crate::domain::mail::author::{MailAuthor, SourceConversation};

    #[test]
    fn the_erased_values_of_a_mail_still_pass_the_checks_that_load_a_mail() {
        assert_eq!(MailAuthor::new(ERASED_TEXT).unwrap().as_str(), ERASED_TEXT);
        assert_eq!(
            SourceConversation::new(ERASED_CONVERSATION_ID)
                .unwrap()
                .as_uuid(),
            ERASED_CONVERSATION_ID
        );
    }

    #[test]
    fn the_erased_conversation_is_the_fixed_uuid_the_database_writes() {
        assert_eq!(
            ERASED_CONVERSATION_ID.to_string(),
            "00000000-0000-7000-8000-000000000000"
        );
    }
}
