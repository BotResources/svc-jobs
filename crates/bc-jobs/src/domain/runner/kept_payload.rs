use crate::domain::payload::PayloadAttachment;
use crate::domain::requester::Requester;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeptPayload {
    pub message: String,
    pub attachments: Vec<PayloadAttachment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPayload {
    pub uploader: Requester,
    pub payload: KeptPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadRefusal {
    JobExistsNotYours,
    PayloadAlreadyUploaded,
}

impl UploadRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::JobExistsNotYours => "job_exists_not_yours",
            Self::PayloadAlreadyUploaded => "payload_already_uploaded",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::JobExistsNotYours => {
                "The Job exists and its Requester is not the one that uploads."
            }
            Self::PayloadAlreadyUploaded => {
                "The Job id already has a payload, and the payload of a Job never changes."
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadAdmission {
    Store,
    Absorbed,
}

pub fn decide_upload(
    uploader: &Requester,
    incoming: &KeptPayload,
    job_requester: Option<&Requester>,
    stored: Option<&StoredPayload>,
) -> Result<UploadAdmission, UploadRefusal> {
    if let Some(requester) = job_requester
        && requester != uploader
    {
        return Err(UploadRefusal::JobExistsNotYours);
    }
    match stored {
        None => Ok(UploadAdmission::Store),
        Some(known) if &known.uploader == uploader && &known.payload == incoming => {
            Ok(UploadAdmission::Absorbed)
        }
        Some(_) => Err(UploadRefusal::PayloadAlreadyUploaded),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ownership::ActorRef;
    use crate::fixtures::run_id;
    use uuid::Uuid;

    fn payload(message: &str) -> KeptPayload {
        KeptPayload {
            message: message.to_owned(),
            attachments: vec![],
        }
    }

    fn service() -> Requester {
        Requester::Service(Some(ActorRef::new(Uuid::now_v7())))
    }

    #[test]
    fn a_first_upload_for_a_job_id_that_does_not_exist_is_stored() {
        assert_eq!(
            decide_upload(&service(), &payload("m"), None, None),
            Ok(UploadAdmission::Store)
        );
    }

    #[test]
    fn an_identical_repeated_upload_by_the_same_uploader_is_absorbed() {
        let uploader = Requester::Run(run_id());
        let stored = StoredPayload {
            uploader,
            payload: payload("m"),
        };
        assert_eq!(
            decide_upload(&uploader, &payload("m"), None, Some(&stored)),
            Ok(UploadAdmission::Absorbed)
        );
    }

    #[test]
    fn any_other_upload_for_a_job_id_that_has_a_payload_is_refused() {
        let uploader = Requester::Run(run_id());
        let stored = StoredPayload {
            uploader,
            payload: payload("m"),
        };
        assert_eq!(
            decide_upload(&uploader, &payload("other"), None, Some(&stored)),
            Err(UploadRefusal::PayloadAlreadyUploaded)
        );
        assert_eq!(
            decide_upload(
                &Requester::Run(run_id()),
                &payload("m"),
                None,
                Some(&stored)
            ),
            Err(UploadRefusal::PayloadAlreadyUploaded)
        );
    }

    #[test]
    fn a_job_that_exists_takes_an_upload_only_from_its_requester() {
        let requester = Requester::Run(run_id());
        assert_eq!(
            decide_upload(&requester, &payload("m"), Some(&requester), None),
            Ok(UploadAdmission::Store)
        );
        assert_eq!(
            decide_upload(&service(), &payload("m"), Some(&requester), None),
            Err(UploadRefusal::JobExistsNotYours)
        );
        assert_eq!(
            decide_upload(
                &service(),
                &payload("m"),
                Some(&Requester::Service(None)),
                None
            ),
            Err(UploadRefusal::JobExistsNotYours)
        );
    }
}
