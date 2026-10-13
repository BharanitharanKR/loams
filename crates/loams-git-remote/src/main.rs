//! `git-remote-loams`: the serverless remote helper (design §36 §6.2, D395;
//! plan GT1 Task 51, the morning version's Task 12 and old GT1 Task 9).
//!
//! git runs `git-remote-loams <remote> <address>` for a `loams::<address>`
//! URL and speaks the gitremote-helpers protocol on stdin and stdout. The
//! helper needs no server: it reads refs from the repository's WAL in the
//! bucket, fetches whole stored packs, and pushes by uploading one `.lpk`
//! and committing one ref transaction through its own `BucketRefLog`. The
//! helper is its own sequencer for the length of one `git push`, fenced by
//! the create-only segment PUT (§36 §4.2, Ruling 2), so any number of
//! helpers and servers can write one repository.
//!
//! `git` itself is run as a process (`pack-objects`, `index-pack`,
//! `merge-base`, `rev-parse`), never linked (D394).

mod fetch;
mod local;
mod protocol;
mod push;
mod url;

use std::fmt;
use std::process::ExitCode;

/// What stops the helper. Each prints as git's `fatal:` line without the
/// `fatal: ` prefix.
#[derive(Debug, thiserror::Error)]
pub enum HelperError {
    /// The address is not `<store-url>/ns/<ns>/repos/<repo>`.
    #[error("loams: expected <store-url>/ns/<ns>/repos/<repo_id>, got {address}")]
    BadAddress {
        /// The address as git gave it.
        address: String,
    },
    /// No repository at the address (Ruling 7).
    #[error("repository '{address}' not found")]
    NotFound {
        /// The address as git gave it.
        address: String,
    },
    /// The bucket or the repository's log failed.
    #[error("loams: {what}: {reason}")]
    Store {
        /// What the helper was doing.
        what: String,
        /// Why it failed.
        reason: String,
    },
    /// A `git` process failed.
    #[error("loams: git {what}: {reason}")]
    Git {
        /// The git command.
        what: String,
        /// Its error.
        reason: String,
    },
    /// The local repository's files.
    #[error("loams: {what}: {source}")]
    Io {
        /// What the helper was doing.
        what: String,
        /// The error.
        #[source]
        source: std::io::Error,
    },
    /// git sent something the helper does not understand.
    #[error("loams: {0}")]
    Protocol(String),
}

impl HelperError {
    /// A store or log failure while doing `what`.
    pub fn store(what: impl Into<String>, err: impl fmt::Display) -> Self {
        Self::Store {
            what: what.into(),
            reason: err.to_string(),
        }
    }

    /// A local file failure while doing `what`.
    pub fn io(what: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            what: what.into(),
            source,
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // git passes `<remote> <address>`; with a bare URL the remote is the URL.
    let (remote, address) = match args.as_slice() {
        [remote, address] => (remote.clone(), address.clone()),
        [address] => (address.clone(), address.clone()),
        _ => {
            eprintln!("usage: git-remote-loams <remote> <address>");
            return ExitCode::from(129);
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("fatal: loams: start the runtime: {err}");
            return ExitCode::from(128);
        }
    };
    let stdin = std::io::stdin().lock();
    let stdout = std::io::stdout().lock();
    match protocol::Session::new(remote, address)
        .and_then(|mut session| session.serve(&runtime, stdin, stdout))
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("fatal: {err}");
            ExitCode::from(128)
        }
    }
}
