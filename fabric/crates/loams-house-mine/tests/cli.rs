//! The command line's shape: every subcommand parses.

use clap::{CommandFactory as _, Parser as _};
use loams_house_mine::cli::{Cli, Command};

#[test]
fn cli_is_consistent() {
    Cli::command().debug_assert();
}

#[test]
fn every_subcommand_parses() {
    for name in ["extract", "merge", "scrub", "report"] {
        let cli = Cli::try_parse_from(["loams-house-mine", name]);
        assert!(cli.is_ok(), "{name}: {:?}", cli.err());
    }
    assert!(matches!(
        Cli::try_parse_from(["loams-house-mine", "extract"]).map(|c| c.command),
        Ok(Command::Extract(_))
    ));
}
