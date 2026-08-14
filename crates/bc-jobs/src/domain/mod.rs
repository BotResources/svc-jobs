//! The `jobs` model and what may be done to it.
//!
//! Split in two on purpose. [`model`] is the state and its invariants; [`actions`]
//! is the verdict — given this entity in this state and this actor, which actions
//! are open and, when one is not, which stable reason code says why. The edge
//! turns that verdict into affordances, so the frontend never has to re-derive
//! "the button is disabled when the status is suspended".

pub mod actions;
pub mod model;
