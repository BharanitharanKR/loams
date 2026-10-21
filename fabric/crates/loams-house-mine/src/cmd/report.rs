//! `loams-house-mine report` (HE0 Task 2). Not built yet.

/// Arguments of `report`.
#[derive(Debug, clap::Args)]
pub struct Args {}

/// Runs `report`.
///
/// # Errors
///
/// Always, until HE0 Task 2 builds it.
pub fn run(_args: &Args) -> anyhow::Result<()> {
    anyhow::bail!("`report` is not built yet (HE0 Task 2)")
}
