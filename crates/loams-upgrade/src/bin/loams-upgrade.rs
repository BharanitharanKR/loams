//! `loams-upgrade`: `loams upgrade` as its own small binary (no engine
//! inside), for the upgrade gate in CI (UP1 Task 20) and for hosts that only
//! upgrade stacks. Same flags, outputs and exit codes as `loams upgrade`.

use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "loams-upgrade",
    version,
    about = "Upgrade a self-hosted Loams compose stack one release at a time (design §54 §10)"
)]
struct Cli {
    #[command(flatten)]
    upgrade: loams_upgrade::UpgradeArgs,
}

fn main() -> ExitCode {
    loams_upgrade::run(&Cli::parse().upgrade)
}
