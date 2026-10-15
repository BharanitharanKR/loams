//! Test support shipped with the SDK: an in-memory Fabric and checkpoint
//! store ([`memory`]), and a reference source and sink ([`reference`]) used by
//! the SDK's tests and by the conformance kit's self-test (Task 6).

pub mod memory;
pub mod reference;
