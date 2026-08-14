//! The traits through which `jobs` reaches anything it does not own.
//!
//! Defined here, implemented in `svc-jobs`, injected as `Arc<dyn Port>` at
//! the composition root. The direction is the whole point of the hexagon: the
//! domain names what it needs, and infrastructure arrives to satisfy it.
//!
//! Segregate them. One trait per concern — an emitter, a read side, a KV
//! staging surface — rather than one fat `JobsPorts`, so a least-privilege
//! adapter cannot inherit a method its role has no right to call. That is a
//! type-level guarantee, and it is worth more than the extra trait.
