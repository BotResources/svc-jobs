use br_core_auth::Passport;
use br_test_harness::PassportBuilder;
use uuid::Uuid;

pub fn platform_admin() -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(true)
        .claim("display_name", "Platform Administrator")
        .claim("email", "admin@botresources.ai")
        .build()
}

pub fn deactivated_super_user() -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(true)
        .active(false)
        .claim("display_name", "Deactivated Administrator")
        .build()
}

pub fn ordinary_member() -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(false)
        .claim("display_name", "Ordinary Member")
        .claim("email", "member@botresources.ai")
        .claim("org_id", Uuid::now_v7().to_string())
        .build()
}

pub fn impersonating_member(admin: &Passport) -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(false)
        .impersonator(admin.actor_id())
        .claim("display_name", "Escalation Attempt")
        .build()
}

pub fn impersonating_admin() -> (Passport, Uuid) {
    let operator_id = Uuid::now_v7();
    let passport = PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(true)
        .impersonator(operator_id)
        .claim("display_name", "Impersonated Administrator")
        .build();
    (passport, operator_id)
}

pub fn runner_access() -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .claim("scopes", vec!["jobs:runner"])
        .build_service()
}

pub fn machine_caller() -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(true)
        .build_service()
}
