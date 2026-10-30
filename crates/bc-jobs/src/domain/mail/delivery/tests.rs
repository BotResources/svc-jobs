use super::*;
use crate::domain::directory::AgentState;
use crate::domain::keys::RunnerTypeKey;
use crate::fixtures::{job_id, run_id};
use uuid::Uuid;

#[test]
fn a_refused_restart_names_its_wire_code_and_words() {
    let refused = RefusedRestart {
        address: AgentId::new(uuid::Uuid::now_v7()),
    };
    assert_eq!(refused.code(), "restart_refused_type_retired");
    assert!(refused.message().contains("retired runner type"));
}

fn held(
    tree: JobId,
    state: AgentState,
    can_restart: bool,
    obstacles: RestartObstacles,
) -> AddressStanding {
    AddressStanding::Held(HeldAddress {
        entry: DirectoryEntry {
            agent_id: AgentId::new(Uuid::now_v7()),
            run_id: run_id(),
            runner_type: RunnerTypeKey::new("analyst").unwrap(),
            state,
            has_inbox: true,
            declares_restart: can_restart,
            holds_saved_state: can_restart,
        },
        initial_job_id: tree,
        obstacles,
    })
}

const CLEAR: RestartObstacles = RestartObstacles {
    runner_type_retired: false,
    parent_job_terminal: false,
    restart_job_live: false,
};

fn to(kind: RecipientKind, standing: AddressStanding) -> Recipient {
    Recipient {
        address: AgentId::new(Uuid::now_v7()),
        kind,
        standing,
    }
}

fn reasons(result: Result<Deliveries, MailRefusal>) -> Vec<BadAddressReason> {
    match result {
        Err(MailRefusal::BadAddresses(bad)) => bad.iter().map(|b| b.reason).collect(),
        other => panic!("expected bad addresses, got {other:?}"),
    }
}

#[test]
fn a_running_agent_of_the_tree_receives_the_mail_and_its_live_run_is_rung() {
    let tree = job_id();
    let recipient = to(
        RecipientKind::Direct,
        held(tree, AgentState::Running, false, CLEAR),
    );
    let Deliveries(plan) = decide_deliveries(tree, &[recipient]).unwrap();
    assert!(plan[0].rings_live_run);
    assert_eq!(plan[0].refused_restart, None);
}

#[test]
fn one_bad_recipient_refuses_the_whole_mail_and_names_every_bad_address() {
    let tree = job_id();
    let good = to(
        RecipientKind::Direct,
        held(tree, AgentState::Running, false, CLEAR),
    );
    let other_tree = to(
        RecipientKind::Direct,
        held(job_id(), AgentState::Running, false, CLEAR),
    );
    let unknown = to(RecipientKind::Cc, AddressStanding::Unknown);
    let removed = to(RecipientKind::Direct, AddressStanding::Removed);
    let result = decide_deliveries(tree, &[good, other_tree, unknown, removed]);
    assert_eq!(
        reasons(result),
        vec![
            BadAddressReason::OtherTree,
            BadAddressReason::RecipientUnknown,
            BadAddressReason::RecipientRemoved
        ]
    );
}

#[test]
fn a_finished_agent_that_cannot_restart_refuses_a_direct_and_a_cc_recipient_alike() {
    let tree = job_id();
    for kind in [RecipientKind::Direct, RecipientKind::Cc] {
        let recipient = to(kind, held(tree, AgentState::Finished, false, CLEAR));
        assert_eq!(
            reasons(decide_deliveries(tree, &[recipient])),
            vec![BadAddressReason::RecipientCannotRestart]
        );
    }
}

#[test]
fn a_finished_agent_under_a_terminal_parent_refuses_even_when_its_type_is_retired() {
    let tree = job_id();
    let both = RestartObstacles {
        runner_type_retired: true,
        parent_job_terminal: true,
        ..CLEAR
    };
    let recipient = to(
        RecipientKind::Direct,
        held(tree, AgentState::Finished, true, both),
    );
    assert_eq!(
        reasons(decide_deliveries(tree, &[recipient])),
        vec![BadAddressReason::RecipientCannotRestart]
    );
}

#[test]
fn a_live_restart_job_accepts_the_mail_without_starting_another_restart() {
    let tree = job_id();
    let in_flight = RestartObstacles {
        restart_job_live: true,
        ..CLEAR
    };
    let recipient = to(
        RecipientKind::Direct,
        held(tree, AgentState::Finished, true, in_flight),
    );
    let Deliveries(plan) = decide_deliveries(tree, &[recipient]).unwrap();
    assert!(plan[0].rings_live_run);
    assert!(!plan[0].starts_restart);
    assert_eq!(plan[0].refused_restart, None);
}

#[test]
fn a_retired_type_delivers_the_mail_unread_and_names_the_refused_restart_for_a_direct_recipient() {
    let tree = job_id();
    let retired = RestartObstacles {
        runner_type_retired: true,
        ..CLEAR
    };
    let direct = to(
        RecipientKind::Direct,
        held(tree, AgentState::Finished, true, retired),
    );
    let cc = to(
        RecipientKind::Cc,
        held(tree, AgentState::Finished, true, retired),
    );
    let direct_address = direct.address;
    let Deliveries(plan) = decide_deliveries(tree, &[direct, cc]).unwrap();
    assert!(!plan[0].rings_live_run);
    assert_eq!(
        plan[0].refused_restart,
        Some(RefusedRestart {
            address: direct_address
        })
    );
    assert_eq!(plan[1].refused_restart, None);
}

#[test]
fn a_finished_restartable_agent_with_no_obstacle_receives_the_mail_unread() {
    let tree = job_id();
    let recipient = to(
        RecipientKind::Direct,
        held(tree, AgentState::Finished, true, CLEAR),
    );
    let Deliveries(plan) = decide_deliveries(tree, &[recipient]).unwrap();
    assert!(!plan[0].rings_live_run);
    assert!(plan[0].starts_restart);
    assert_eq!(plan[0].refused_restart, None);
}

#[test]
fn a_cc_notice_never_starts_a_restart() {
    let tree = job_id();
    let recipient = to(
        RecipientKind::Cc,
        held(tree, AgentState::Finished, true, CLEAR),
    );
    let Deliveries(plan) = decide_deliveries(tree, &[recipient]).unwrap();
    assert!(!plan[0].starts_restart);
}
