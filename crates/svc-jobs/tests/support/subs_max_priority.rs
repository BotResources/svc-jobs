use uuid::Uuid;

const AFFORDANCES: &str = "affordances { action allowed reasonCode params }";

pub fn my_max_priority_changed() -> String {
    r#"subscription {
  jobsMyMaxPriorityChanged {
    __typename
    ... on JobsMyMaxPrioritySnapshot { maxPriority }
    ... on JobsMyMaxPriorityDelta { maxPriority }
  }
}"#
    .to_owned()
}

pub fn user_max_priorities_changed(user_ids: &[Uuid]) -> String {
    let ids = user_ids
        .iter()
        .map(|id| format!("\"{id}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"subscription {{
  jobsUserMaxPrioritiesChanged(userIds: [{ids}]) {{
    __typename
    ... on JobsUserMaxPrioritiesSnapshot {{
      cursor
      userMaxPriorities {{ userId maxPriority isDefault {AFFORDANCES} }}
    }}
    ... on JobsUserMaxPriorityDelta {{
      cursor
      userMaxPriority {{ userId maxPriority isDefault {AFFORDANCES} }}
    }}
  }}
}}"#
    )
}
