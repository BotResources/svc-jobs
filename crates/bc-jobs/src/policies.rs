//! Pure reactions to `jobs` facts: `event -> intent`.
//!
//! A policy matches a domain event and returns *what should happen* — a
//! notification intent, a side-effect descriptor — as data. It never sends
//! anything. The projector in `svc-jobs` is what executes an intent, and
//! that separation is the only reason a policy is testable by calling it.
//!
//! If a function here needs a client, a pool, or an `await`, it is not a policy;
//! it belongs on the other side of a port.
