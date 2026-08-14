use std::fmt;

const MAXIMUM: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentError {
    Blank {
        field: &'static str,
    },
    TooLong {
        field: &'static str,
        length: usize,
        maximum: usize,
    },
    Invalid {
        field: &'static str,
        value: String,
    },
}

impl SegmentError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Blank { .. } => "blank_segment",
            Self::TooLong { .. } => "segment_too_long",
            Self::Invalid { .. } => "invalid_segment",
        }
    }
}

impl fmt::Display for SegmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for SegmentError {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubjectSegment(String);

impl SubjectSegment {
    pub fn new(field: &'static str, value: impl AsRef<str>) -> Result<Self, SegmentError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(SegmentError::Blank { field });
        }
        if value.len() > MAXIMUM {
            return Err(SegmentError::TooLong {
                field,
                length: value.len(),
                maximum: MAXIMUM,
            });
        }
        let head_is_alphanumeric = value
            .chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphanumeric());
        let tail_is_clean = value.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '-'
        });
        if head_is_alphanumeric && tail_is_clean {
            Ok(Self(value.to_owned()))
        } else {
            Err(SegmentError::Invalid {
                field,
                value: value.to_owned(),
            })
        }
    }

    pub fn runner_type(value: impl AsRef<str>) -> Result<Self, SegmentError> {
        Self::new("runner_type", value)
    }

    pub fn instance_key(value: impl AsRef<str>) -> Result<Self, SegmentError> {
        Self::new("instance_key", value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SubjectSegment {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_runner_type_carrying_a_subject_separator_is_refused() {
        // Given: a runner type that would split `jobs.trigger.{runner_type}` into two segments
        let candidate = "worker.python";
        // When: it is offered to the transport contract
        let result = SubjectSegment::runner_type(candidate);
        // Then: it never reaches a subject builder
        assert_eq!(
            result,
            Err(SegmentError::Invalid {
                field: "runner_type",
                value: candidate.to_owned()
            })
        );
    }

    #[test]
    fn a_segment_carrying_a_wildcard_is_refused() {
        // Given: the two NATS wildcards, which would widen a subject to a whole family
        // When/Then: neither may address the runner transport
        assert!(SubjectSegment::runner_type("worker-*").is_err());
        assert!(SubjectSegment::runner_type(">").is_err());
    }

    #[test]
    fn an_instance_key_read_back_from_the_presence_bucket_is_validated_too() {
        // Given: a key a runner wrote itself into the presence bucket
        // When: it is turned into a segment
        // Then: the separator is refused on that half as well
        assert!(SubjectSegment::instance_key("pod-7.evil").is_err());
        assert_eq!(
            SubjectSegment::instance_key("pod-7").unwrap().as_str(),
            "pod-7"
        );
    }

    #[test]
    fn a_blank_or_oversized_segment_is_refused() {
        // Given: an empty key and one longer than a subject token may be
        // When/Then: both are refused, naming the field at fault
        assert_eq!(
            SubjectSegment::runner_type(""),
            Err(SegmentError::Blank {
                field: "runner_type"
            })
        );
        assert_eq!(
            SubjectSegment::runner_type("a".repeat(MAXIMUM + 1)),
            Err(SegmentError::TooLong {
                field: "runner_type",
                length: MAXIMUM + 1,
                maximum: MAXIMUM
            })
        );
    }
}
