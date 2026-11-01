use crate::EVT_RUN_ITEM_ARRIVED_V2;
use crate::event_v2::{
    EVENT_TYPE_ITEM_ARRIVED, EVENT_V2_SCHEMA_VERSION, ItemKind, RunItemArrived, RunItemArrivedV2,
};
use serde_json::json;
use uuid::Uuid;

const KINDS: [(ItemKind, &str); 4] = [
    (ItemKind::Mail, "MAIL"),
    (ItemKind::CcNotice, "CC_NOTICE"),
    (ItemKind::Notice, "NOTICE"),
    (ItemKind::MailNotice, "MAIL_NOTICE"),
];

fn exhaustive(kind: ItemKind) -> &'static str {
    match kind {
        ItemKind::Mail => "MAIL",
        ItemKind::CcNotice => "CC_NOTICE",
        ItemKind::Notice => "NOTICE",
        ItemKind::MailNotice => "MAIL_NOTICE",
    }
}

#[test]
fn the_doorbell_of_version_two_has_its_own_subject_type_name_and_schema_version() {
    assert_eq!(
        EVT_RUN_ITEM_ARRIVED_V2,
        "integration.evt.jobs.run.item_arrived.v2"
    );
    assert_eq!(EVENT_TYPE_ITEM_ARRIVED, "run.item_arrived");
    assert_eq!(EVENT_V2_SCHEMA_VERSION, 2);
}

#[test]
fn the_doorbell_of_version_two_for_a_mail_notice_of_an_address_reads_and_writes_the_offer_shape() {
    let run_id = Uuid::now_v7();
    let address = Uuid::now_v7();
    let item_id = Uuid::now_v7();
    let wire = json!({
        "run_id": run_id.to_string(),
        "address": address.to_string(),
        "item_id": item_id.to_string(),
        "item_kind": "MAIL_NOTICE",
    });
    let doorbell: RunItemArrivedV2 = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(
        doorbell,
        RunItemArrivedV2 {
            run_id,
            address: Some(address),
            item_id,
            item_kind: ItemKind::MailNotice,
        }
    );
    assert_eq!(serde_json::to_value(&doorbell).unwrap(), wire);
}

#[test]
fn a_doorbell_for_the_run_itself_carries_no_address_and_writes_none() {
    let wire = json!({
        "run_id": Uuid::now_v7().to_string(),
        "item_id": Uuid::now_v7().to_string(),
        "item_kind": "NOTICE",
    });
    let doorbell: RunItemArrivedV2 = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(doorbell.address, None);
    assert_eq!(serde_json::to_value(&doorbell).unwrap(), wire);
    let null_address = json!({
        "run_id": Uuid::now_v7().to_string(),
        "address": null,
        "item_id": Uuid::now_v7().to_string(),
        "item_kind": "MAIL",
    });
    let read: RunItemArrivedV2 = serde_json::from_value(null_address).unwrap();
    assert_eq!(read.address, None);
}

#[test]
fn both_doorbells_require_the_run_the_item_id_and_the_kind() {
    let complete = json!({
        "run_id": Uuid::now_v7().to_string(),
        "item_id": Uuid::now_v7().to_string(),
        "item_kind": "MAIL",
    });
    for field in ["run_id", "item_id", "item_kind"] {
        let mut incomplete = complete.clone();
        incomplete.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<RunItemArrivedV2>(incomplete.clone()).is_err(),
            "v2 without {field}"
        );
        assert!(
            serde_json::from_value::<RunItemArrived>(incomplete).is_err(),
            "v1 without {field}"
        );
    }
}

#[test]
fn the_item_kind_is_a_closed_set_of_four_words() {
    for (kind, word) in KINDS {
        assert_eq!(exhaustive(kind), word);
        assert_eq!(serde_json::to_value(kind).unwrap(), word);
        assert_eq!(
            serde_json::from_value::<ItemKind>(json!(word)).unwrap(),
            kind
        );
    }
    for outside in ["mail", "MAIL-NOTICE", "REFUSED", "MAILNOTICE", ""] {
        assert!(
            serde_json::from_value::<ItemKind>(json!(outside)).is_err(),
            "'{outside}'"
        );
        let doorbell = json!({
            "run_id": Uuid::now_v7().to_string(),
            "item_id": Uuid::now_v7().to_string(),
            "item_kind": outside,
        });
        assert!(serde_json::from_value::<RunItemArrivedV2>(doorbell).is_err());
    }
    assert!(serde_json::from_value::<ItemKind>(json!(3)).is_err());
    assert!(serde_json::from_value::<ItemKind>(json!(null)).is_err());
}

#[test]
fn the_doorbell_of_version_one_keeps_its_shape_and_shares_the_item_kind_vocabulary() {
    let run_id = Uuid::now_v7();
    let item_id = Uuid::now_v7();
    let wire = json!({
        "run_id": run_id.to_string(),
        "item_id": item_id.to_string(),
        "item_kind": "CC_NOTICE",
    });
    let v1: RunItemArrived = serde_json::from_value(wire.clone()).unwrap();
    let v2: RunItemArrivedV2 = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(v1.run_id, v2.run_id);
    assert_eq!(v1.item_id, v2.item_id);
    assert_eq!(v1.item_kind, v2.item_kind);
    assert_eq!(serde_json::to_value(&v1).unwrap(), wire);
    let mail_notice = json!({
        "run_id": run_id.to_string(),
        "item_id": item_id.to_string(),
        "item_kind": "MAIL_NOTICE",
    });
    let v1_mail_notice: RunItemArrived = serde_json::from_value(mail_notice).unwrap();
    assert_eq!(v1_mail_notice.item_kind, ItemKind::MailNotice);
}
