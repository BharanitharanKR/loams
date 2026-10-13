//! Endpoints and the compute state machine (PG2 Task 11) on TiKV: the
//! cases of `endpoints`. Each prints `skipped:` when `LOAMS_TEST_PD` is
//! unset.

#[macro_use]
mod common;

use loams_pg_control::store::conformance::{Factory, tikv_factory};

/// The backend the cases run on.
fn factory() -> Factory {
    tikv_factory()
}

#[macro_use]
#[path = "endpoints/env.rs"]
mod env;

#[path = "endpoints/cases.rs"]
mod cases;
