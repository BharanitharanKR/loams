//! `loams-house-mine merge` (HE0 Task 2). Not built yet.

/// Arguments of `merge`.
#[derive(Debug, clap::Args)]
pub struct Args {}

/// Runs `merge`.
///
/// # Errors
///
/// Always, until HE0 Task 2 builds it.
pub fn run(_args: &Args) -> anyhow::Result<()> {
    anyhow::bail!("`merge` is not built yet (HE0 Task 2)")
}
