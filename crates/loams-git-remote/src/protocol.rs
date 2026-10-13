//! The gitremote-helpers protocol (old GT1 Task 9 steps 2–7): commands on
//! stdin, answers on stdout, progress on stderr.
//!
//! - `capabilities` → `fetch`, `push`, `option`.
//! - `option <name> <value>` → `ok` for `verbosity`, `progress`, `atomic`,
//!   `push-option` and `dry-run`; `unsupported` otherwise.
//! - `list` / `list for-push` → the refs at the WAL's tail, and `@<target>
//!   HEAD`. A missing repository fails both with `repository '<address>'
//!   not found`, except `list for-push` with `LOAMS_GIT_CREATE_ON_PUSH=1`,
//!   which lists nothing and creates the repository at the push (Ruling 7).
//! - `fetch` lines, then a blank line → the packs the repository lacks.
//! - `push` lines, then a blank line → `ok <dst>` or `error <dst> <why>`.

use std::io::{BufRead, Write};
use std::sync::Arc;

use loams_git::format::pb;
use loams_git::{
    BucketRefLog, NamespaceId, RefName, RefSnapshot, RepoId, RepoPaths, RepoState, SequencerConfig,
    StoreBlobStore, StoreWalStore, SystemClock, WalStore, latest_state,
};
use loams_store::Store;

use crate::HelperError;
use crate::fetch::fetch;
use crate::local::Local;
use crate::push::{PushOptions, PushSpec, Statuses, check_ref_count, dry_run, push};
use crate::url::Address;

/// The branch `HEAD` names in a repository a push creates, when the push
/// names no branch.
const DEFAULT_BRANCH: &str = "refs/heads/main";

/// The repository the address names.
#[derive(Clone, Debug)]
enum Target {
    /// It exists.
    Exists(RepoId),
    /// It does not, and the next push creates it (`LOAMS_GIT_CREATE_ON_PUSH=1`).
    Create(RepoId),
}

/// One helper process: one remote of one local repository.
#[derive(Debug)]
pub struct Session {
    address: Address,
    local: Local,
    store: Option<Store>,
    target: Option<Target>,
    /// The state `list` answered from: `fetch` downloads its packs.
    listed: Option<Arc<RefSnapshot>>,
    verbosity: u32,
    options: PushOptions,
}

impl Session {
    /// A session for `remote` at `address`.
    pub fn new(remote: String, address: String) -> Result<Self, HelperError> {
        let address = Address::parse(&address)?;
        let local = Local::open(&remote);
        Ok(Self {
            address,
            local,
            store: None,
            target: None,
            listed: None,
            verbosity: 1,
            options: PushOptions {
                lose_answers: faults::lose_answers(),
                ..PushOptions::default()
            },
        })
    }

    /// Answers git's commands until it closes stdin or sends a blank line.
    pub fn serve(
        &mut self,
        runtime: &tokio::runtime::Runtime,
        input: impl BufRead,
        mut out: impl Write,
    ) -> Result<(), HelperError> {
        let mut lines = input.lines();
        let mut next = move || -> Result<Option<String>, HelperError> {
            lines
                .next()
                .transpose()
                .map_err(|err| HelperError::io("read from git", err))
        };
        while let Some(line) = next()? {
            let answer = match line.as_str() {
                "" => return Ok(()),
                "capabilities" => "fetch\npush\noption\n\n".to_string(),
                "list" => runtime.block_on(self.list(false))?,
                "list for-push" => runtime.block_on(self.list(true))?,
                _ if line.starts_with("option ") => self.option(&line["option ".len()..]),
                _ if line.starts_with("fetch ") => {
                    // The batch ends at a blank line; the wanted refs are
                    // all in the packs, so the lines need no reading.
                    while next()?.is_some_and(|l| !l.is_empty()) {}
                    runtime.block_on(self.fetch())?;
                    "\n".to_string()
                }
                _ if line.starts_with("push ") => {
                    let mut specs = vec![PushSpec::parse(&line["push ".len()..])?];
                    while let Some(more) = next()?.filter(|l| !l.is_empty()) {
                        let arg = more.strip_prefix("push ").ok_or_else(|| {
                            HelperError::Protocol(format!("expected a push line, got {more:?}"))
                        })?;
                        specs.push(PushSpec::parse(arg)?);
                    }
                    let statuses = runtime.block_on(self.push(&specs))?;
                    let mut text = String::new();
                    for (dst, status) in statuses {
                        match status {
                            None => text.push_str(&format!("ok {dst}\n")),
                            Some(reason) => text.push_str(&format!("error {dst} {reason}\n")),
                        }
                    }
                    text.push('\n');
                    text
                }
                other => {
                    return Err(HelperError::Protocol(format!(
                        "unsupported command {other:?}"
                    )));
                }
            };
            out.write_all(answer.as_bytes())
                .and_then(|()| out.flush())
                .map_err(|err| HelperError::io("write to git", err))?;
        }
        Ok(())
    }

    fn option(&mut self, arg: &str) -> String {
        let (name, value) = arg.split_once(' ').unwrap_or((arg, ""));
        let flag = value == "true";
        match name {
            "verbosity" => match value.parse() {
                Ok(level) => self.verbosity = level,
                Err(_) => return "error verbosity takes a number\n".into(),
            },
            "progress" => self.options.progress = flag,
            "atomic" => self.options.atomic = flag,
            "dry-run" => self.options.dry_run = flag,
            "push-option" => self.options.push_options.push(unquote(value)),
            _ => return "unsupported\n".into(),
        }
        "ok\n".into()
    }

    fn store(&mut self) -> Result<Store, HelperError> {
        if let Some(store) = &self.store {
            return Ok(store.clone());
        }
        let store = faults::wrap(self.address.open_store()?)?;
        self.store = Some(store.clone());
        Ok(store)
    }

    fn not_found(&self) -> HelperError {
        HelperError::NotFound {
            address: self.address.raw.clone(),
        }
    }

    async fn target(&mut self, for_push: bool) -> Result<Target, HelperError> {
        if let Some(target) = &self.target {
            return Ok(target.clone());
        }
        let store = self.store()?;
        let target = match self.address.resolve(&store).await? {
            Some(repo) => Target::Exists(repo),
            None if for_push && create_on_push() => Target::Create(self.address.new_repo_id()?),
            None => return Err(self.not_found()),
        };
        self.target = Some(target.clone());
        Ok(target)
    }

    fn paths(&self, repo: &RepoId) -> RepoPaths {
        RepoPaths::new(self.address.ns, repo)
    }

    async fn list(&mut self, for_push: bool) -> Result<String, HelperError> {
        let snapshot = match self.target(for_push).await? {
            Target::Exists(repo) => {
                let wal = StoreWalStore::new(self.store()?, self.paths(&repo));
                let state = latest_state(&wal).await.map_err(|err| match err {
                    loams_git::RefError::NotFound => self.not_found(),
                    other => HelperError::store("read the remote refs", other),
                })?;
                Arc::new(state.snapshot())
            }
            Target::Create(_) => Arc::new(RepoState::empty(pb::ObjectFormat::Sha1).snapshot()),
        };
        let mut text = String::new();
        for (name, oid) in &snapshot.refs {
            text.push_str(&format!("{oid} {name}\n"));
        }
        // A symbolic ref whose target is missing (an empty repository's
        // HEAD) is left out: git then reports an empty repository.
        for (name, target) in &snapshot.symrefs {
            if snapshot.refs.contains_key(target) {
                text.push_str(&format!("@{target} {name}\n"));
            }
        }
        text.push('\n');
        self.listed = Some(snapshot);
        Ok(text)
    }

    async fn fetch(&mut self) -> Result<(), HelperError> {
        let repo = match self.target(false).await? {
            Target::Exists(repo) => repo,
            Target::Create(_) => return Ok(()),
        };
        let store = self.store()?;
        let snapshot = match &self.listed {
            Some(snapshot) => Arc::clone(snapshot),
            None => {
                let wal = StoreWalStore::new(store.clone(), self.paths(&repo));
                let state = latest_state(&wal)
                    .await
                    .map_err(|err| HelperError::store("read the remote refs", err))?;
                Arc::new(state.snapshot())
            }
        };
        let progress = self.options.progress;
        fetch(
            &store,
            self.address.ns,
            &repo,
            &snapshot,
            &self.local,
            progress,
        )
        .await?;
        Ok(())
    }

    async fn push(&mut self, specs: &[PushSpec]) -> Result<Statuses, HelperError> {
        check_ref_count(specs)?;
        let target = self.target(true).await?;
        let store = self.store()?;
        if self.options.dry_run {
            let snapshot = match &target {
                Target::Exists(repo) => {
                    let wal = StoreWalStore::new(store.clone(), self.paths(repo));
                    let state = latest_state(&wal)
                        .await
                        .map_err(|err| HelperError::store("read the remote refs", err))?;
                    state.snapshot()
                }
                Target::Create(_) => RepoState::empty(pb::ObjectFormat::Sha1).snapshot(),
            };
            return dry_run(&self.local, &snapshot, specs);
        }
        let repo = match target {
            Target::Exists(repo) => repo,
            Target::Create(repo) => {
                self.create(&store, &repo, specs).await?;
                self.target = Some(Target::Exists(repo.clone()));
                repo
            }
        };
        let principal = self.local.principal(self.address.scheme())?;
        let paths = self.paths(&repo);
        let wal: Arc<dyn WalStore> = Arc::new(StoreWalStore::new(store.clone(), paths.clone()));
        let blobs = Arc::new(StoreBlobStore::new(store, paths));
        let log = BucketRefLog::open(
            wal,
            blobs.clone(),
            SequencerConfig::new(tenant(self.address.ns), self.address.ns, repo),
            Arc::new(SystemClock),
        )
        .await
        .map_err(|err| HelperError::store("open the remote's log", err))?;
        let statuses = push(
            &log,
            blobs.as_ref(),
            &self.local,
            &principal,
            specs,
            &self.options,
        )
        .await;
        // Publishes the hint for the next reader, whatever the answer.
        log.shutdown().await;
        statuses
    }

    /// Creates the repository a push names (Ruling 7): checkpoint 0 with
    /// `HEAD` on the first branch the push creates (`refs/heads/main` if
    /// none). A racing creator's checkpoint is as good as ours.
    async fn create(
        &self,
        store: &Store,
        repo: &RepoId,
        specs: &[PushSpec],
    ) -> Result<(), HelperError> {
        let head = specs
            .iter()
            .filter(|spec| spec.src.is_some())
            .find_map(|spec| {
                spec.dst
                    .starts_with("refs/heads/")
                    .then(|| RefName::parse(&spec.dst).ok())
                    .flatten()
            })
            .unwrap_or_else(|| RefName::parse(DEFAULT_BRANCH).expect("a valid ref name"));
        let mut state = RepoState::empty(pb::ObjectFormat::Sha1);
        state.symrefs.insert("HEAD".into(), head);
        let wal = StoreWalStore::new(store.clone(), self.paths(repo));
        if let Err(err) = wal.put_checkpoint(&state.to_checkpoint(0)).await {
            // Another first push may have written a different checkpoint 0.
            if wal
                .latest_checkpoint(loams_git::Seq(u64::MAX))
                .await
                .ok()
                .flatten()
                .is_none()
            {
                return Err(HelperError::store("create the repository", err));
            }
        }
        if self.verbosity > 0 {
            eprintln!(
                "loams: created repository {repo} in namespace {}",
                self.address.ns
            );
        }
        Ok(())
    }
}

/// `<org>/<ns>`, the events' `tenantid`: `local/<ns>` until MT1 names an
/// organisation (ruling T13.4).
fn tenant(ns: NamespaceId) -> String {
    format!("local/{ns}")
}

/// Whether a push may create a missing repository (Ruling 7).
fn create_on_push() -> bool {
    std::env::var("LOAMS_GIT_CREATE_ON_PUSH").is_ok_and(|v| v == "1")
}

/// A push option as git sends it: C-quoted when it holds special
/// characters.
fn unquote(value: &str) -> String {
    let Some(inner) = value
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    else {
        return value.to_string();
    };
    let mut out = String::new();
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some(other) => out.push(other),
            None => {}
        }
    }
    out
}

/// The `faults` test hook (tests/faults.rs). `LOAMS_GIT_FAULTS=<seed>:<rate>`
/// makes each create-only PUT lose its acknowledgement with probability
/// `rate` (the write lands, the answer is an error);
/// `LOAMS_GIT_FAULTS=lose-answer` drops the first answer of every commit.
#[cfg(feature = "faults")]
mod faults {
    use std::sync::Arc;

    use loams_store::{Fault, FaultyStore, Op, Store};
    use rand::{Rng, SeedableRng};

    use crate::HelperError;

    /// Create-only PUTs a seeded schedule covers.
    const SCHEDULED: u64 = 4096;

    fn setting() -> Option<String> {
        std::env::var("LOAMS_GIT_FAULTS").ok()
    }

    pub fn lose_answers() -> bool {
        setting().as_deref() == Some("lose-answer")
    }

    pub fn wrap(store: Store) -> Result<Store, HelperError> {
        let Some((seed, rate)) = setting().and_then(|s| {
            let (seed, rate) = s.split_once(':')?;
            Some((seed.parse::<u64>().ok()?, rate.parse::<f64>().ok()?))
        }) else {
            return Ok(store);
        };
        let faulty = FaultyStore::new(Arc::clone(store.inner()));
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut gap = 0;
        for _ in 0..SCHEDULED {
            gap += 1;
            if rng.random_bool(rate.clamp(0.0, 1.0)) {
                faulty.inject_nth(Op::PutCreate, gap, Fault::ErrorAfterApply);
                gap = 0;
            }
        }
        Ok(Store::new(Arc::new(faulty)))
    }
}

/// Without the `faults` feature the hook is inert.
#[cfg(not(feature = "faults"))]
mod faults {
    use loams_store::Store;

    use crate::HelperError;

    pub fn lose_answers() -> bool {
        false
    }

    pub fn wrap(store: Store) -> Result<Store, HelperError> {
        Ok(store)
    }
}

#[cfg(test)]
mod tests {
    use super::unquote;

    #[test]
    fn push_options_are_unquoted() {
        assert_eq!(unquote("ci.skip"), "ci.skip");
        assert_eq!(unquote("\"a b\\\"c\\n\""), "a b\"c\n");
    }
}
