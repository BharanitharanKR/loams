//! The door (design §54 §10.3 steps 4 and 12): the Tailscale sidecars are
//! the stack's only way in for clients (the engine, the House and the
//! console listen on loopback or on internal networks), so stopping them
//! closes the stack to clients while it is half upgraded.

use std::path::Path;

use crate::compose;
use crate::errors::Result;
use crate::runtime::Runtime;

/// The sidecars that let clients in.
pub const SIDECARS: [&str; 2] = ["tailscale", "house-tailscale"];

/// Stops the sidecars the stack enables.
pub fn close(runtime: &dyn Runtime, dir: &Path, enabled: &[String]) -> Result<()> {
    compose::stop(runtime, dir, &compose::present(enabled, &SIDECARS))
}

/// Starts them again.
pub fn open(runtime: &dyn Runtime, dir: &Path, enabled: &[String]) -> Result<()> {
    compose::up(runtime, dir, &compose::present(enabled, &SIDECARS))
}
