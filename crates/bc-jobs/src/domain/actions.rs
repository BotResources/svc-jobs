//! What an actor may do to a `jobs` entity, and why not when they may not.
//!
//! An action verdict is three things: the stable action name, the allow/deny
//! decision, and — when denied — a stable reason **code** plus its structured
//! params. Never a rendered sentence: the frontend localizes the code, and a
//! sentence built here would be an English string the frontend cannot translate.
//!
//! This is domain logic, not presentation. It reads the entity's state and the
//! actor's permissions and answers; the GraphQL layer only carries the answer.
