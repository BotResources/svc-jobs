use crate::domain::directory::registration::RunStanding;
use crate::domain::ids::JobId;

pub struct HeldRun<'a> {
    pub initial_job_id: JobId,
    pub runner_type: &'a str,
    pub instance_key: Option<&'a str>,
}

pub fn standing_of_started_run(
    held: HeldRun<'_>,
    named_runner_type: Option<&str>,
    named_instance_key: &str,
) -> RunStanding {
    let type_matches = named_runner_type.is_none_or(|named| named == held.runner_type);
    if type_matches && held.instance_key == Some(named_instance_key) {
        RunStanding::Started {
            initial_job_id: held.initial_job_id,
        }
    } else {
        RunStanding::StartedOnAnotherInstance
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::job_id;

    fn standing(
        held_key: Option<&str>,
        named_type: Option<&str>,
        named_key: &str,
    ) -> (RunStanding, JobId) {
        let tree = job_id();
        let held = HeldRun {
            initial_job_id: tree,
            runner_type: "alpha",
            instance_key: held_key,
        };
        (standing_of_started_run(held, named_type, named_key), tree)
    }

    #[test]
    fn the_run_is_held_only_when_the_key_matches_and_a_named_type_matches() {
        let cases = [
            (Some("i1"), None, "i1", true),
            (Some("i1"), Some("alpha"), "i1", true),
            (Some("i1"), Some("beta"), "i1", false),
            (Some("i1"), None, "i2", false),
            (Some("i1"), Some("alpha"), "i2", false),
            (Some("i1"), Some("beta"), "i2", false),
            (None, None, "i1", false),
            (None, Some("alpha"), "i1", false),
        ];
        for (held_key, named_type, named_key, held) in cases {
            let (found, tree) = standing(held_key, named_type, named_key);
            let expected = if held {
                RunStanding::Started {
                    initial_job_id: tree,
                }
            } else {
                RunStanding::StartedOnAnotherInstance
            };
            assert_eq!(
                found, expected,
                "held {held_key:?}, named {named_type:?} {named_key}"
            );
        }
    }
}
