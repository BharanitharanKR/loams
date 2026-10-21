//! `loams-house-mine scrub` (HE0 Task 2). Not built yet.

/// Arguments of `scrub`.
#[derive(Debug, clap::Args)]
pub struct Args {}

/// Runs `scrub`.
///
/// # Errors
///
/// Always, until HE0 Task 2 builds it.
pub fn run(_args: &Args) -> anyhow::Result<()> {
    anyhow::bail!("`scrub` is not built yet (HE0 Task 2)")
}
