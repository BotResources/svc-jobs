use serde::{Deserialize, Serialize};

use crate::error::JobsError;

const SEGMENT_MAXIMUM: usize = 64;
const TEXT_MAXIMUM: usize = 512;
const MESSAGE_MAXIMUM: usize = 8192;

fn segment(field: &'static str, value: &str) -> Result<String, JobsError> {
    if value.is_empty() {
        return Err(JobsError::BlankValue { field });
    }
    if value.len() > SEGMENT_MAXIMUM {
        return Err(JobsError::ValueTooLong {
            field,
            length: value.len(),
            maximum: SEGMENT_MAXIMUM,
        });
    }
    let head_is_alphanumeric = value
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_alphanumeric());
    let tail_is_clean = value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_' || character == '-');
    if head_is_alphanumeric && tail_is_clean {
        Ok(value.to_owned())
    } else {
        Err(JobsError::InvalidSegment {
            field,
            value: value.to_owned(),
        })
    }
}

fn lowercase_code(field: &'static str, value: &str) -> Result<String, JobsError> {
    let segment = segment(field, value)?;
    if segment.chars().all(|character| {
        character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
    }) {
        Ok(segment)
    } else {
        Err(JobsError::InvalidSegment {
            field,
            value: value.to_owned(),
        })
    }
}

fn text(field: &'static str, value: &str, maximum: usize) -> Result<String, JobsError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(JobsError::BlankValue { field });
    }
    if trimmed.len() > maximum {
        return Err(JobsError::ValueTooLong {
            field,
            length: trimmed.len(),
            maximum,
        });
    }
    Ok(trimmed.to_owned())
}

macro_rules! validated_string {
    ($name:ident, $field:literal, $constructor:expr) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl AsRef<str>) -> Result<Self, JobsError> {
                let build: fn(&'static str, &str) -> Result<String, JobsError> = $constructor;
                build($field, value.as_ref()).map(Self)
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = JobsError;

            fn try_from(value: String) -> Result<Self, JobsError> {
                Self::new(value)
            }
        }
    };
}

validated_string!(RunnerTypeKey, "runner_type", segment);
validated_string!(ProducerKey, "producer", segment);
validated_string!(InstanceKey, "instance_key", segment);
validated_string!(ReasonCode, "reason_code", lowercase_code);
validated_string!(DisplayName, "display_name", |field, value| text(
    field,
    value,
    TEXT_MAXIMUM
));
validated_string!(RunnerVersion, "runner_version", |field, value| text(
    field,
    value,
    TEXT_MAXIMUM
));
validated_string!(StepLabel, "step_label", |field, value| text(
    field,
    value,
    TEXT_MAXIMUM
));
validated_string!(LogMessage, "log_message", |field, value| text(
    field,
    value,
    MESSAGE_MAXIMUM
));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_runner_type_carrying_a_subject_separator_is_refused() {
        // Given: a runner type that would split the `jobs.trigger.{runner_type}` subject
        let candidate = "worker.python";
        // When: it is constructed
        let result = RunnerTypeKey::new(candidate);
        // Then: the key is refused before it can ever address a wrong subject
        assert_eq!(
            result,
            Err(JobsError::InvalidSegment {
                field: "runner_type",
                value: candidate.to_owned()
            })
        );
    }

    #[test]
    fn a_runner_type_carrying_a_wildcard_is_refused() {
        // Given: a NATS wildcard smuggled into a routing key
        // When/Then: both wildcard forms are refused
        assert!(RunnerTypeKey::new("worker-*").is_err());
        assert!(RunnerTypeKey::new(">").is_err());
    }

    #[test]
    fn a_reason_code_must_be_a_code_not_a_sentence() {
        // Given: a human-readable failure explanation
        // When: it is offered as a reason code
        // Then: it is refused — reasons travel as codes, never as language
        assert!(ReasonCode::new("The provider timed out").is_err());
        assert_eq!(
            ReasonCode::new("provider_timeout").unwrap().as_str(),
            "provider_timeout"
        );
    }

    #[test]
    fn a_step_label_is_display_text_and_keeps_its_words() {
        // Given: a runner-supplied progression label with surrounding whitespace
        // When: it becomes a step label
        let label = StepLabel::new("  Fetching source documents  ").unwrap();
        // Then: it is trimmed but otherwise untouched — it is content, not a key
        assert_eq!(label.as_str(), "Fetching source documents");
    }

    #[test]
    fn blank_display_text_is_refused() {
        // Given: whitespace only
        // When/Then: every text value object refuses it
        assert_eq!(
            DisplayName::new("   "),
            Err(JobsError::BlankValue {
                field: "display_name"
            })
        );
    }
}
