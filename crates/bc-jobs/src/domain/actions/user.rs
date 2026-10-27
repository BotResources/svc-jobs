use crate::domain::access::PrincipalFacts;
use crate::domain::priority::UserMaxPriority;
use crate::error::JobsError;

impl UserMaxPriority {
    /// Setting or clearing a user's maximum: super users only, whatever the
    /// user's current maximum.
    pub fn guard_govern(&self, principal: &PrincipalFacts) -> Result<(), JobsError> {
        principal.guard_super_user()
    }
}
