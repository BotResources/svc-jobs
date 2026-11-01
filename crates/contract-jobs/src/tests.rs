use super::*;

#[test]
fn the_service_key_is_the_registered_service_name() {
    assert_eq!(SERVICE_KEY, "jobs");
}
