//! Where connector instances run (§33 §5, OR-16).
//!
//! * [`kube`] — the production fleet on Kubernetes (CN2 Task 26): the fenced
//!   instance and split leases native tasks are scheduled by, the `flow`
//!   Deployment's scale, and the objects `KubeRuntime` renders for the
//!   fallback runtimes.
//!
//! `NativeRuntime` (`runtime/native.rs`, CN2 Task 4's integration) and the
//! fallback renderers and supervisors (`runtime/{camel,debezium,iggy}.rs`, CN1
//! Task 3 and CN2 Task 22) are not built yet (R4.1, R26.1).

pub mod kube;
