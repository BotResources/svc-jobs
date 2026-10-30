use uuid::Uuid;

use crate::domain::ids::RunId;
use crate::domain::payload::CallMode;
use crate::error::JobsError;

pub fn guard_caller_address_is_named(
    mode: CallMode,
    address: Option<Uuid>,
) -> Result<Option<Uuid>, JobsError> {
    match (mode, address) {
        (CallMode::Background, None) => Err(JobsError::CallerAddressNotHeld),
        (CallMode::Background, address) => Ok(address),
        (CallMode::Sync, _) => Ok(None),
    }
}

pub fn guard_caller_address(
    mode: CallMode,
    address: Option<Uuid>,
    caller: RunId,
    holder: Option<RunId>,
) -> Result<(), JobsError> {
    if guard_caller_address_is_named(mode, address)?.is_none() {
        return Ok(());
    }
    match holder {
        Some(held_by) if held_by == caller => Ok(()),
        Some(_) => Err(JobsError::CallerAddressNotOfRun),
        None => Err(JobsError::CallerAddressNotHeld),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::run_id;

    #[test]
    fn a_background_call_must_name_an_address_held_by_its_own_run() {
        let caller = run_id();
        let address = Some(Uuid::now_v7());
        assert_eq!(
            guard_caller_address(CallMode::Background, address, caller, Some(caller)),
            Ok(())
        );
        assert_eq!(
            guard_caller_address(CallMode::Background, None, caller, None),
            Err(JobsError::CallerAddressNotHeld)
        );
        assert_eq!(
            guard_caller_address(CallMode::Background, address, caller, None),
            Err(JobsError::CallerAddressNotHeld)
        );
        assert_eq!(
            guard_caller_address(CallMode::Background, address, caller, Some(run_id())),
            Err(JobsError::CallerAddressNotOfRun)
        );
    }

    #[test]
    fn a_synchronous_call_names_no_address_and_needs_none() {
        let caller = run_id();
        assert_eq!(
            guard_caller_address(CallMode::Sync, Some(Uuid::now_v7()), caller, None),
            Ok(())
        );
    }
}
