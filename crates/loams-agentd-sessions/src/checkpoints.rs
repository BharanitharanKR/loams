//! Per-turn checkpoints and restore (design §53 §6.5 G6, G7; plan SD1
//! Task 10, Ruling 6).
//!
//! At each turn's start the daemon writes a hidden commit of the chat's
//! checkout at `refs/loams/checkpoints/<chatId>/turn/<n>/pre`, and at its end
//! one at `…/post`. This is T3 Code's scheme (`apps/server/src/checkpointing`
//! and `vcs/GitVcsDriver.ts`'s `captureCheckpoint` / `restoreCheckpoint`),
//! rewritten in Rust:
//!
//! - the commit is written through a temporary index in the git common
//!   directory (`GIT_INDEX_FILE`, `read-tree HEAD`, `add -A`, `write-tree`,
//!   `commit-tree -p HEAD`, `update-ref`), so the user's index, HEAD and
//!   branches are never touched; untracked files that are not ignored are
//!   included, ignored files are not;
//! - the refs live in the common directory, so a linked worktree's chat
//!   keeps its checkpoints beside the main checkout's;
//! - `<n>` counts the chat's turns from 1, read back from the refs, so the
//!   numbering survives a restart; the commit's subject carries the id of the
//!   user message that started the turn (the wire's `turnId`), and a turn
//!   already checkpointed for that message is not written twice (a steer
//!   that falls back to a fresh dispatch, a startup retry).
//!
//! Restore puts the checkout back to a turn's `pre` commit (`git restore
//! --source <c> --worktree --staged -- .`, `git clean -fd`, then the index
//! back to HEAD) after the caller's diff checksum is checked against the
//! checkout's current one. It works only in the chat's own checkout, which
//! the daemon resolves from the chat; no path comes from the caller.

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use loams_agentd_proto::checkpoints::{
    CHECKPOINT_MISSING, Checkpoint, CheckpointList, REF_PREFIX, STALE_DIFF,
};

use crate::EngineError;
use crate::repos::Repos;

/// How long a turn waits for its `pre` checkpoint before it goes ahead
/// without one.
const CAPTURE_TIMEOUT: Duration = Duration::from_secs(30);

/// The identity of the hidden commits. They are never on a branch, and the
/// user's own identity is never needed (or set) for them.
const IDENTITY_NAME: &str = "Loams checkpoint";
const IDENTITY_EMAIL: &str = "checkpoint@loams.invalid";

/// Git's fsync settings for the objects and refs a checkpoint writes: an
/// unclean shutdown must not leave a zero-byte ref behind (T3 Code's
/// `durableWrite`).
const DURABLE: &[&str] = &[
    "-c",
    "core.fsync=objects,reference",
    "-c",
    "core.fsyncMethod=fsync",
];

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Whether `chat_id` can name a ref directory as it is: letters, digits,
/// `-`, `_` and `.`, not leading with `.`, no `..`, not ending `.lock`.
/// Chat ids are UUIDs or client-minted slugs; anything else gets no
/// checkpoints.
pub fn ref_safe_chat_id(chat_id: &str) -> bool {
    !chat_id.is_empty()
        && chat_id.len() <= 128
        && chat_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        && !chat_id.starts_with('.')
        && !chat_id.ends_with('.')
        && !chat_id.contains("..")
        && !chat_id.ends_with(".lock")
}

/// Whether a message id fits the commit subject's last word.
fn subject_safe(message_id: &str) -> bool {
    !message_id.is_empty()
        && message_id.len() <= 256
        && !message_id
            .chars()
            .any(|c| c.is_whitespace() || c.is_control())
}

/// `refs/loams/checkpoints/<chatId>/`.
fn chat_prefix(chat_id: &str) -> String {
    format!("{REF_PREFIX}/{chat_id}/")
}

/// `refs/loams/checkpoints/<chatId>/turn/<n>/<phase>`.
pub fn turn_ref(chat_id: &str, turn: u32, phase: Phase) -> String {
    format!("{REF_PREFIX}/{chat_id}/turn/{turn}/{}", phase.as_str())
}

/// A turn's start or end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Pre,
    Post,
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Phase::Pre => "pre",
            Phase::Post => "post",
        }
    }
}

/// One checkpoint commit, as read back from its ref.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Point {
    oid: String,
    at_secs: i64,
    message_id: String,
}

/// One turn's refs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Turn {
    pre: Option<Point>,
    post: Option<Point>,
}

/// The turn in progress per chat, for its `post`.
#[derive(Debug, Clone)]
struct Current {
    root: PathBuf,
    turn: u32,
    message_id: String,
}

struct Inner {
    repos: Repos,
    /// Serializes one chat's checkpoint writes, restores and deletes.
    chat_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    current: Mutex<HashMap<String, Current>>,
}

/// The checkpoint store: the turn hook of the sessions engine and the engine
/// side of `ListCheckpoints` and `RestoreCheckpoint`.
#[derive(Clone)]
pub struct Checkpoints {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Checkpoints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Checkpoints").finish_non_exhaustive()
    }
}

impl Checkpoints {
    pub fn new(repos: Repos) -> Self {
        Self {
            inner: Arc::new(Inner {
                repos,
                chat_locks: Mutex::new(HashMap::new()),
                current: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn chat_lock(&self, chat_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        lock(&self.inner.chat_locks)
            .entry(chat_id.to_owned())
            .or_default()
            .clone()
    }

    /// The checkout root of `cwd`, or `None` outside a git checkout.
    pub async fn checkout_root(&self, cwd: &Path) -> Option<PathBuf> {
        self.inner
            .repos
            .checkout_identity(cwd)
            .await
            .ok()
            .map(|identity| identity.root)
    }

    /// Write the `pre` checkpoint of the turn `message_id` starts in `cwd`.
    /// Returns its turn number; `None` when `cwd` is not a checkout or the
    /// ids cannot name a checkpoint. A turn already written for the same
    /// message is not written again.
    ///
    /// # Errors
    ///
    /// A git failure.
    pub async fn capture_turn_start(
        &self,
        chat_id: &str,
        cwd: &Path,
        message_id: &str,
    ) -> Result<Option<u32>, EngineError> {
        if !ref_safe_chat_id(chat_id) || !subject_safe(message_id) {
            return Ok(None);
        }
        let Some(root) = self.checkout_root(cwd).await else {
            return Ok(None);
        };
        let chat_lock = self.chat_lock(chat_id);
        let _guard = chat_lock.lock().await;
        let turns = read_turns(&root, chat_id).await?;
        let existing = turns.iter().find_map(|(turn, refs)| {
            refs.pre
                .as_ref()
                .is_some_and(|pre| pre.message_id == message_id)
                .then_some(*turn)
        });
        let turn = match existing {
            Some(turn) => turn,
            None => {
                let turn = turns.keys().next_back().copied().unwrap_or(0) + 1;
                let subject = format!("loams checkpoint turn {turn} pre {message_id}");
                write_checkpoint(&root, &turn_ref(chat_id, turn, Phase::Pre), &subject).await?;
                turn
            }
        };
        lock(&self.inner.current).insert(
            chat_id.to_owned(),
            Current {
                root,
                turn,
                message_id: message_id.to_owned(),
            },
        );
        Ok(Some(turn))
    }

    /// Write the `post` checkpoint of the chat's turn in progress, if any.
    ///
    /// # Errors
    ///
    /// A git failure.
    pub async fn capture_turn_end(&self, chat_id: &str) -> Result<Option<u32>, EngineError> {
        let chat_lock = self.chat_lock(chat_id);
        let _guard = chat_lock.lock().await;
        let Some(current) = lock(&self.inner.current).get(chat_id).cloned() else {
            return Ok(None);
        };
        let git_ref = turn_ref(chat_id, current.turn, Phase::Post);
        let subject = format!(
            "loams checkpoint turn {} post {}",
            current.turn, current.message_id
        );
        // A turn that ended twice (a quiesce park, then the run's exit) keeps
        // its first end.
        if resolve_commit(&current.root, &git_ref).await.is_some() {
            return Ok(Some(current.turn));
        }
        write_checkpoint(&current.root, &git_ref, &subject).await?;
        Ok(Some(current.turn))
    }

    /// The chat's checkpoints in the checkout `root`, oldest turn first.
    ///
    /// # Errors
    ///
    /// A git failure.
    pub async fn list(&self, chat_id: &str, root: &Path) -> Result<CheckpointList, EngineError> {
        if !ref_safe_chat_id(chat_id) {
            return Ok(CheckpointList::default());
        }
        let turns = read_turns(root, chat_id).await?;
        let mut checkpoints = Vec::new();
        let mut iter = turns.iter().peekable();
        while let Some((turn, refs)) = iter.next() {
            let Some(pre) = &refs.pre else { continue };
            let end = refs.post.as_ref().map(|post| post.oid.clone()).or_else(|| {
                iter.peek()
                    .and_then(|(_, next)| next.pre.as_ref().map(|p| p.oid.clone()))
            });
            let files_changed = match end {
                Some(end) => changed_paths(root, &pre.oid, &end).await?,
                None => 0,
            };
            checkpoints.push(Checkpoint {
                turn_id: pre.message_id.clone(),
                git_ref: turn_ref(chat_id, *turn, Phase::Pre),
                created_at_ms: pre.at_secs.saturating_mul(1000),
                files_changed,
            });
        }
        Ok(CheckpointList { checkpoints })
    }

    /// The `pre` commit of the turn `turn_id` started, or `checkpoint_missing`.
    ///
    /// # Errors
    ///
    /// `checkpoint_missing`, or a git failure.
    pub async fn pre_commit(
        &self,
        chat_id: &str,
        root: &Path,
        turn_id: &str,
    ) -> Result<String, EngineError> {
        let missing = || {
            EngineError::Other(format!(
                "{CHECKPOINT_MISSING}: no checkpoint for turn {turn_id:?} of chat {chat_id:?}"
            ))
        };
        if !ref_safe_chat_id(chat_id) || turn_id.is_empty() {
            return Err(missing());
        }
        let turns = read_turns(root, chat_id).await?;
        let pre = turns
            .values()
            .filter_map(|turn| turn.pre.as_ref())
            .find(|pre| pre.message_id == turn_id)
            .ok_or_else(missing)?;
        resolve_commit(root, &pre.oid).await.ok_or_else(missing)
    }

    /// Restore the checkout `root` to the `pre` commit of the turn `turn_id`
    /// started, after checking the checkout's diff checksum against
    /// `expected_checksum`. Returns the commit.
    ///
    /// # Errors
    ///
    /// `checkpoint_missing`, `stale_diff`, a partial diff snapshot, or a git
    /// failure.
    pub async fn restore_worktree(
        &self,
        chat_id: &str,
        root: &Path,
        turn_id: &str,
        expected_checksum: &str,
    ) -> Result<String, EngineError> {
        let chat_lock = self.chat_lock(chat_id);
        let _guard = chat_lock.lock().await;
        let commit = self.pre_commit(chat_id, root, turn_id).await?;
        let snapshot = crate::diff_sync::capture_diff(&self.inner.repos, root).await?;
        if snapshot.truncated {
            return Err(EngineError::Other(
                "cannot safely restore over a partial diff snapshot".into(),
            ));
        }
        if expected_checksum.is_empty() || snapshot.checksum != expected_checksum {
            return Err(EngineError::Other(format!(
                "{STALE_DIFF}: the checkout changed since the restore was confirmed"
            )));
        }
        restore_tree(root, &commit).await?;
        Ok(commit)
    }

    /// Delete every checkpoint ref of `chat_id` in the checkout of `cwd`
    /// (the thread was deleted).
    ///
    /// # Errors
    ///
    /// A git failure.
    pub async fn delete_chat(&self, chat_id: &str, cwd: Option<&Path>) -> Result<(), EngineError> {
        let current = lock(&self.inner.current).remove(chat_id);
        if !ref_safe_chat_id(chat_id) {
            return Ok(());
        }
        let root = match cwd {
            Some(cwd) => self.checkout_root(cwd).await,
            None => None,
        }
        .or(current.map(|current| current.root));
        let Some(root) = root else { return Ok(()) };
        let chat_lock = self.chat_lock(chat_id);
        let _guard = chat_lock.lock().await;
        let refs = git_text(
            &root,
            &["for-each-ref", "--format=%(refname)", &chat_prefix(chat_id)],
            &[],
        )
        .await?;
        for git_ref in refs.lines().filter(|line| !line.is_empty()) {
            git_text(&root, &["update-ref", "-d", git_ref], &[]).await?;
        }
        drop(_guard);
        lock(&self.inner.chat_locks).remove(chat_id);
        Ok(())
    }
}

impl crate::sessions::TurnCheckpoints for Checkpoints {
    fn turn_start<'a>(
        &'a self,
        chat_id: &'a str,
        cwd: &'a str,
        message_id: &'a str,
    ) -> futures::future::BoxFuture<'a, ()> {
        Box::pin(async move {
            let capture = self.capture_turn_start(chat_id, Path::new(cwd), message_id);
            match tokio::time::timeout(CAPTURE_TIMEOUT, capture).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    tracing::warn!(chat = %chat_id, %error, "checkpoint: turn start not captured");
                }
                Err(_) => {
                    tracing::warn!(chat = %chat_id, "checkpoint: turn start timed out");
                }
            }
        })
    }

    fn turn_end(&self, chat_id: &str) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let checkpoints = self.clone();
        let chat_id = chat_id.to_owned();
        runtime.spawn(async move {
            let capture = checkpoints.capture_turn_end(&chat_id);
            match tokio::time::timeout(CAPTURE_TIMEOUT, capture).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    tracing::warn!(chat = %chat_id, %error, "checkpoint: turn end not captured");
                }
                Err(_) => tracing::warn!(chat = %chat_id, "checkpoint: turn end timed out"),
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Git
// ---------------------------------------------------------------------------

/// Run git in `root` with `envs`; its stdout, or the failure.
async fn git_bytes(
    root: &Path,
    args: &[&str],
    envs: &[(&str, OsString)],
) -> Result<std::process::Output, EngineError> {
    let mut cmd = tokio::process::Command::new("git");
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.as_std_mut().creation_flags(0x08000000);
    }
    cmd.arg("-C").arg(root).args(args);
    for (key, value) in envs {
        cmd.env(key, value);
    }
    cmd.stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|error| EngineError::Other(format!("git spawn failed: {error}")))
}

/// Run git; its stdout as text, or an error naming the subcommand.
async fn git_text(
    root: &Path,
    args: &[&str],
    envs: &[(&str, OsString)],
) -> Result<String, EngineError> {
    let output = git_bytes(root, args, envs).await?;
    if !output.status.success() {
        let verb = args
            .iter()
            .find(|arg| !arg.starts_with('-') && !arg.contains('='))
            .copied()
            .unwrap_or("git");
        return Err(EngineError::Other(format!(
            "git {verb}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// The commit `rev` names, if it names one.
async fn resolve_commit(root: &Path, rev: &str) -> Option<String> {
    let spec = format!("{rev}^{{commit}}");
    let output = git_bytes(root, &["rev-parse", "--verify", "--quiet", &spec], &[])
        .await
        .ok()?;
    let oid = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (output.status.success() && !oid.is_empty()).then_some(oid)
}

/// Every turn of `chat_id` with refs in the checkout's repository.
async fn read_turns(root: &Path, chat_id: &str) -> Result<BTreeMap<u32, Turn>, EngineError> {
    let prefix = format!("{}turn/", chat_prefix(chat_id));
    let listing = git_text(
        root,
        &[
            "for-each-ref",
            "--format=%(refname)%00%(objectname)%00%(committerdate:unix)%00%(contents:subject)",
            &prefix,
        ],
        &[],
    )
    .await?;
    let mut turns: BTreeMap<u32, Turn> = BTreeMap::new();
    for line in listing.lines() {
        let mut fields = line.split('\0');
        let (Some(name), Some(oid), Some(at), Some(subject)) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let Some((turn, phase)) = name
            .strip_prefix(&prefix)
            .and_then(|rest| rest.split_once('/'))
        else {
            continue;
        };
        let Ok(turn) = turn.parse::<u32>() else {
            continue;
        };
        let point = Point {
            oid: oid.to_owned(),
            at_secs: at.parse().unwrap_or_default(),
            message_id: subject
                .rsplit_once(' ')
                .map(|(_, id)| id.to_owned())
                .unwrap_or_default(),
        };
        let entry = turns.entry(turn).or_default();
        match phase {
            "pre" => entry.pre = Some(point),
            "post" => entry.post = Some(point),
            _ => {}
        }
    }
    Ok(turns)
}

/// How many paths differ between two checkpoint commits.
async fn changed_paths(root: &Path, from: &str, to: &str) -> Result<u32, EngineError> {
    let output = git_bytes(
        root,
        &["diff", "--name-only", "-z", "--no-renames", from, to, "--"],
        &[],
    )
    .await?;
    if !output.status.success() {
        return Err(EngineError::Other(format!(
            "git diff: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let count = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .count();
    Ok(u32::try_from(count).unwrap_or(u32::MAX))
}

/// A temporary index file, removed (with its lock) when dropped.
struct TempIndex(PathBuf);

impl Drop for TempIndex {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let mut lock = self.0.clone().into_os_string();
        lock.push(".lock");
        let _ = std::fs::remove_file(lock);
    }
}

/// Write the checkout's current state (tracked and untracked, not ignored)
/// as a commit whose parent is HEAD, and create `git_ref` at it. The user's
/// index, HEAD and refs are untouched.
async fn write_checkpoint(
    root: &Path,
    git_ref: &str,
    subject: &str,
) -> Result<String, EngineError> {
    let common = git_text(
        root,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        &[],
    )
    .await?;
    let index = TempIndex(
        PathBuf::from(common.trim())
            .join(format!("loams-checkpoint-index-{}", uuid::Uuid::new_v4())),
    );
    let envs: Vec<(&str, OsString)> = vec![
        ("GIT_INDEX_FILE", index.0.clone().into_os_string()),
        ("GIT_AUTHOR_NAME", IDENTITY_NAME.into()),
        ("GIT_AUTHOR_EMAIL", IDENTITY_EMAIL.into()),
        ("GIT_COMMITTER_NAME", IDENTITY_NAME.into()),
        ("GIT_COMMITTER_EMAIL", IDENTITY_EMAIL.into()),
    ];
    let head = resolve_commit(root, "HEAD").await;
    if head.is_some() {
        git_text(
            root,
            &["-c", "core.fsmonitor=false", "read-tree", "HEAD"],
            &envs,
        )
        .await?;
    }
    let mut add = vec!["-c", "core.fsmonitor=false"];
    add.extend_from_slice(DURABLE);
    add.extend_from_slice(&["add", "-A", "--ignore-errors", "--", "."]);
    git_text(root, &add, &envs).await?;
    let mut write_tree: Vec<&str> = DURABLE.to_vec();
    write_tree.push("write-tree");
    let tree = git_text(root, &write_tree, &envs).await?;
    let tree = tree.trim();
    if tree.is_empty() {
        return Err(EngineError::Other("git write-tree: no tree".into()));
    }
    let mut commit_tree: Vec<&str> = DURABLE.to_vec();
    commit_tree.extend_from_slice(&["commit-tree", tree]);
    if let Some(head) = head.as_deref() {
        commit_tree.extend_from_slice(&["-p", head]);
    }
    commit_tree.extend_from_slice(&["-m", subject]);
    let commit = git_text(root, &commit_tree, &envs).await?;
    let commit = commit.trim().to_owned();
    if commit.is_empty() {
        return Err(EngineError::Other("git commit-tree: no commit".into()));
    }
    // Create only: an existing ref of that name is never moved.
    let mut update: Vec<&str> = DURABLE.to_vec();
    update.extend_from_slice(&["update-ref", "-m", subject, git_ref, &commit, ""]);
    git_text(root, &update, &[]).await?;
    Ok(commit)
}

/// Put the checkout `root` back to `commit`'s tree: tracked files restored,
/// files absent from it removed (untracked, not ignored), the index back to
/// HEAD. HEAD and branches do not move; ignored files stay.
async fn restore_tree(root: &Path, commit: &str) -> Result<(), EngineError> {
    let with_tree = format!("--with-tree={commit}");
    let tracked = git_bytes(
        root,
        &["ls-files", "--cached", &with_tree, "-z", "--", "."],
        &[],
    )
    .await?;
    if !tracked.status.success() {
        return Err(EngineError::Other(format!(
            "git ls-files: {}",
            String::from_utf8_lossy(&tracked.stderr).trim()
        )));
    }
    // An empty index and an empty checkpoint give `restore` nothing to match.
    if !tracked.stdout.is_empty() {
        git_text(
            root,
            &[
                "restore",
                "--source",
                commit,
                "--worktree",
                "--staged",
                "--",
                ".",
            ],
            &[],
        )
        .await?;
    }
    // The index now holds the checkpoint, so whatever is untracked (and not
    // ignored) is absent from it. Nested repositories are left alone (no
    // second `-f`), as are ignored files (no `-x`).
    git_text(root, &["clean", "-fd", "--", "."], &[]).await?;
    if resolve_commit(root, "HEAD").await.is_some() {
        git_text(root, &["reset", "--quiet", "--", "."], &[]).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_ids_that_can_name_a_ref() {
        assert!(ref_safe_chat_id("chat-1"));
        assert!(ref_safe_chat_id("6f1c0e9a-8d0b-4a8e-9c1e-2f3a4b5c6d7e"));
        assert!(!ref_safe_chat_id(""));
        assert!(!ref_safe_chat_id("../x"));
        assert!(!ref_safe_chat_id("a/b"));
        assert!(!ref_safe_chat_id(".hidden"));
        assert!(!ref_safe_chat_id("a..b"));
        assert!(!ref_safe_chat_id("x.lock"));
        assert!(!ref_safe_chat_id("a b"));
        assert!(!ref_safe_chat_id("a~1"));
    }

    #[test]
    fn message_ids_fit_the_subject() {
        assert!(subject_safe("msg-1"));
        assert!(!subject_safe(""));
        assert!(!subject_safe("two words"));
        assert!(!subject_safe("line\nbreak"));
    }

    #[test]
    fn turn_refs_follow_the_ruling() {
        assert_eq!(
            turn_ref("chat-1", 3, Phase::Pre),
            "refs/loams/checkpoints/chat-1/turn/3/pre"
        );
        assert_eq!(
            turn_ref("chat-1", 3, Phase::Post),
            "refs/loams/checkpoints/chat-1/turn/3/post"
        );
    }
}
