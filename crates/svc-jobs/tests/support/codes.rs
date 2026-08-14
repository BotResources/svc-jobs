use std::collections::BTreeSet;

pub fn is_stable_reason(code: &str) -> bool {
    let mut characters = code.chars();
    let starts_well = matches!(characters.next(), Some(first) if first.is_ascii_lowercase());
    starts_well
        && code.len() > 1
        && characters.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

pub fn assert_stable_reason(code: &str, what: &str) {
    assert!(
        is_stable_reason(code),
        "{what}: a domain reason code is a stable snake_case machine code the producer can branch \
         on, never prose and never a display string: {code}"
    );
}

pub fn assert_pairwise_distinct(codes: &[(&str, String)]) {
    let unique: BTreeSet<&str> = codes.iter().map(|(_, code)| code.as_str()).collect();
    assert_eq!(
        unique.len(),
        codes.len(),
        "each refusal cause owes the producer its own code — a producer cannot tell 'reissue with \
         another id' from 'lower your max_attempts' when several causes answer the same code: \
         {codes:?}"
    );
}
