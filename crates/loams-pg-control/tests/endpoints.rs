//! Endpoints and the compute state machine (PG2 Task 11) on the local
//! (embedded) store, a fake runtime and a fake `compute_ctl`.
//! `endpoints_tikv` runs the same cases on TiKV.

#[macro_use]
mod common;

use loams_pg_control::store::conformance::{Factory, local_factory};

/// The backend the cases run on.
fn factory() -> Factory {
    local_factory(env!("CARGO_TARGET_TMPDIR"))
}

#[macro_use]
#[path = "endpoints/env.rs"]
mod env;

#[path = "endpoints/cases.rs"]
mod cases;
