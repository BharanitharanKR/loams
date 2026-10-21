//! `loams-house-mine extract` (HE0 Task 2). Not built yet.

/// Arguments of `extract`.
#[derive(Debug, clap::Args)]
pub struct Args {}

/// Runs `extract`.
///
/// # Errors
///
/// Always, until HE0 Task 2 builds it.
pub fn run(_args: &Args) -> anyhow::Result<()> {
    anyhow::bail!("`extract` is not built yet (HE0 Task 2)")
}
