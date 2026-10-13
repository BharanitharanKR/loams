//! Git writes for Loams Studio's git toolbar (design §53 §6.5 G2, G3, G5;
//! plan SD1 Task 9). `ListWorktrees` (G1) is [`crate::worktree_list`],
//! `CreateChangeRequest` (G4) [`crate::change_request_create`] and
//! `DiscardPaths` (G21) [`crate::diff_sync::discard_paths`].
//!
//! The rules (§53 §8.1, the plan's Review Focus 3):
//! - A commit is checked against the diff checksum the person reviewed
//!   (`stale_diff`), and uses the user's own git identity: nothing here sets
//!   one.
//! - A push names a configured remote, never a URL, and a forced push is only
//!   ever `--force-with-lease=<ref>:<expected>`, the expectation being the
//!   remote-tracking ref this checkout last saw. There is no code path that
//!   builds a plain forced push ([`push_args`] is the only argument list).
//! - A merge checks the source head the person chose, and on conflicts it
//!   aborts, verifies the tree is clean and answers `merge_conflict` with the
//!   paths.
//! - Error texts never carry credentials: URLs are stripped of their user
//!   information ([`redact`]).
//!
//! The engine serializes these writes and checks the paths it is given
//! against the checkouts it knows (`rpc.rs`); these functions take a resolved
//! checkout or repository root.

use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::AsyncWriteExt;

use loams_agentd_proto::git_write::{
    CommitWorktreeParams, CommitWorktreeReply, MERGE_CONFLICT, MergeBranchParams, MergeBranchReply,
    MergeStrategy, NOTHING_TO_COMMIT, PUSH_REJECTED, PushBranchParams, PushBranchReply, STALE_DIFF,
};

use crate::repos::Repos;

/// How long one git write may take: a commit hook or a push over a slow
/// network included.
const GIT_WRITE_TIMEOUT: Duration = Duration::from_secs(180);

/// Bytes of git output kept for an error message.
const ERROR_TAIL: usize = 2 * 1024;

/// A git write that did not happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitWriteError {
    /// The request itself is wrong (`bad_params`).
    BadParams(String),
    /// The write failed; the text starts with a reason when it has one
    /// (`stale_diff: …`).
    Failed(String),
}

impl std::fmt::Display for GitWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadParams(text) | Self::Failed(text) => f.write_str(text),
        }
    }
}

impl std::error::Error for GitWriteError {}

pub(crate) fn bad(text: impl Into<String>) -> GitWriteError {
    GitWriteError::BadParams(text.into())
}

pub(crate) fn failed(text: impl Into<String>) -> GitWriteError {
    GitWriteError::Failed(text.into())
}

fn reason(reason: &str, detail: impl std::fmt::Display) -> GitWriteError {
    GitWriteError::Failed(format!("{reason}: {detail}"))
}

// ---------------------------------------------------------------------------
// Running git
// ---------------------------------------------------------------------------

pub(crate) struct GitOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

impl GitOutput {
    /// The output's last lines, without credentials, for an error text.
    pub fn detail(&self) -> String {
        let text = if self.stderr.trim().is_empty() {
            &self.stdout
        } else {
            &self.stderr
        };
        let text = text.trim();
        let start = text.len().saturating_sub(ERROR_TAIL);
        let start = (start..=text.len())
            .find(|index| text.is_char_boundary(*index))
            .unwrap_or(text.len());
        redact(&text[start..])
    }
}

/// `git -C <cwd> <args>`, never prompting, with `stdin` written when given.
pub(crate) async fn run_git<S: AsRef<std::ffi::OsStr>>(
    cwd: &Path,
    args: &[S],
    stdin: Option<&[u8]>,
) -> Result<GitOutput, GitWriteError> {
    let mut command = tokio::process::Command::new("git");
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.as_std_mut().creation_flags(0x08000000);
    }
    command
        .arg("-C")
        .arg(cwd)
        .args(args)
        // Never wait on a credential prompt nobody can answer, and read git's
        // messages in one language.
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("LC_ALL", "C")
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|error| failed(format!("git spawn failed: {error}")))?;
    if let Some(bytes) = stdin
        && let Some(mut pipe) = child.stdin.take()
    {
        pipe.write_all(bytes)
            .await
            .map_err(|error| failed(format!("git stdin failed: {error}")))?;
        drop(pipe);
    }
    let output = tokio::time::timeout(GIT_WRITE_TIMEOUT, child.wait_with_output())
        .await
        .map_err(|_| failed("git timed out"))?
        .map_err(|error| failed(format!("git wait failed: {error}")))?;
    Ok(GitOutput {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

/// [`run_git`] that must succeed; answers trimmed stdout.
pub(crate) async fn git_ok<S: AsRef<std::ffi::OsStr>>(
    cwd: &Path,
    args: &[S],
) -> Result<String, GitWriteError> {
    let output = run_git(cwd, args, None).await?;
    if output.success {
        Ok(output.stdout.trim().to_owned())
    } else {
        Err(failed(format!("git: {}", output.detail())))
    }
}

/// [`run_git`] whose failure is only an answer (`--verify --quiet`).
pub(crate) async fn git_try(cwd: &Path, args: &[&str]) -> Option<String> {
    match run_git(cwd, args, None).await {
        Ok(output) if output.success => Some(output.stdout.trim().to_owned()),
        _ => None,
    }
}

/// `text` with the user information of every URL removed
/// (`https://user:token@host/…` → `https://host/…`).
pub fn redact(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(index) = rest.find("://") {
        let (head, tail) = rest.split_at(index + 3);
        out.push_str(head);
        let authority_end = tail
            .find(|c: char| c == '/' || c == '?' || c == '#' || c.is_whitespace())
            .unwrap_or(tail.len());
        let authority = &tail[..authority_end];
        match authority.rfind('@') {
            Some(at) => out.push_str(&authority[at + 1..]),
            None => out.push_str(authority),
        }
        rest = &tail[authority_end..];
    }
    out.push_str(rest);
    out
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// A path relative to a checkout: not empty, not absolute, no `..`, no NUL.
pub fn checkout_relative(path: &str) -> Result<&str, GitWriteError> {
    let invalid = || bad(format!("paths must be relative to the checkout: {path:?}"));
    if path.is_empty() || path.contains('\0') || path.starts_with('/') || path.starts_with('\\') {
        return Err(invalid());
    }
    for component in Path::new(path).components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(invalid());
            }
        }
    }
    if path.split(['/', '\\']).any(|part| part == "..") {
        return Err(invalid());
    }
    Ok(path)
}

/// A local branch name git accepts, which can't be read as an option.
pub async fn valid_branch(cwd: &Path, name: &str) -> Result<(), GitWriteError> {
    let invalid = || bad(format!("not a branch name: {name:?}"));
    if name.is_empty()
        || name.starts_with('-')
        || name.contains(char::is_whitespace)
        || name.len() > 255
    {
        return Err(invalid());
    }
    git_try(cwd, &["check-ref-format", "--branch", name])
        .await
        .map(drop)
        .ok_or_else(invalid)
}

/// The commit a local branch points at.
async fn branch_head(cwd: &Path, name: &str) -> Result<String, GitWriteError> {
    git_try(
        cwd,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("refs/heads/{name}^{{commit}}"),
        ],
    )
    .await
    .ok_or_else(|| bad(format!("no local branch {name:?}")))
}

/// Tracked changes in a worktree (`untracked`: untracked files count too).
async fn has_changes(cwd: &Path, untracked: bool) -> Result<bool, GitWriteError> {
    let mode = if untracked {
        "--untracked-files=all"
    } else {
        "--untracked-files=no"
    };
    Ok(!git_ok(cwd, &["status", "--porcelain", mode])
        .await?
        .is_empty())
}

// ---------------------------------------------------------------------------
// G2: CommitWorktree
// ---------------------------------------------------------------------------

/// Stage `paths` (or everything with `all`) and commit in the checkout at
/// `root`, refusing when its diff checksum is not `expected_checksum`.
///
/// # Errors
///
/// `stale_diff`, `nothing_to_commit`, bad params, or git's own failure.
pub async fn commit(
    repos: &Repos,
    root: &Path,
    params: &CommitWorktreeParams,
) -> Result<CommitWorktreeReply, GitWriteError> {
    let message = params.message.trim();
    if message.is_empty() {
        return Err(bad("a commit needs a message"));
    }
    if params.all && !params.paths.is_empty() {
        return Err(bad("name paths or set all, not both"));
    }
    let paths = params
        .paths
        .iter()
        .map(|path| checkout_relative(path))
        .collect::<Result<Vec<_>, _>>()?;

    let snapshot = crate::diff_sync::capture_diff(repos, root)
        .await
        .map_err(|error| failed(error.to_string()))?;
    if snapshot.checksum != params.expected_checksum {
        return Err(reason(
            STALE_DIFF,
            "the working tree changed since the commit was confirmed",
        ));
    }

    if params.all {
        git_ok(root, &["add", "-A"]).await?;
    } else if !paths.is_empty() {
        let mut args: Vec<&str> = vec!["--literal-pathspecs", "add", "-A", "--"];
        args.extend(paths.iter().copied());
        git_ok(root, &args).await?;
    }

    // `diff --cached --quiet` exits 1 when something is staged.
    let staged = !run_git(root, &["diff", "--cached", "--quiet"], None)
        .await?
        .success;
    if !staged && !params.amend {
        return Err(reason(NOTHING_TO_COMMIT, "nothing is staged"));
    }

    let mut args = vec!["commit", "--quiet", "--cleanup=strip", "-F", "-"];
    if params.signoff {
        args.push("--signoff");
    }
    if params.amend {
        args.push("--amend");
    }
    let output = run_git(root, &args, Some(message.as_bytes())).await?;
    if !output.success {
        return Err(failed(format!("git commit failed: {}", output.detail())));
    }
    Ok(CommitWorktreeReply {
        sha: git_ok(root, &["rev-parse", "HEAD"]).await?,
        summary: git_ok(root, &["log", "-1", "--format=%s"]).await?,
    })
}

// ---------------------------------------------------------------------------
// G3: PushBranch
// ---------------------------------------------------------------------------

/// The one argument list a push is built from. `lease` is the commit the
/// remote branch must still be at (`""`: it must not exist yet); without one
/// the push is a plain fast-forward push. A refspec never starts with `+`.
pub fn push_args(
    remote: &str,
    branch: &str,
    set_upstream: bool,
    lease: Option<&str>,
) -> Vec<String> {
    let reference = format!("refs/heads/{branch}");
    let mut args = vec!["push".to_owned(), "--porcelain".to_owned()];
    if set_upstream {
        args.push("--set-upstream".to_owned());
    }
    if let Some(expected) = lease {
        args.push(format!("--force-with-lease={reference}:{expected}"));
    }
    args.push("--".to_owned());
    args.push(remote.to_owned());
    args.push(format!("{reference}:{reference}"));
    args
}

/// Push `branch` of the checkout at `root` to a configured remote.
///
/// # Errors
///
/// `push_rejected` on a non-fast-forward or a failed lease; bad params for
/// an unknown remote or branch; git's own failure, without credentials.
pub async fn push(
    root: &Path,
    params: &PushBranchParams,
) -> Result<PushBranchReply, GitWriteError> {
    let remote = params.remote.as_deref().unwrap_or("origin");
    let remotes = git_ok(root, &["remote"]).await?;
    if remote.starts_with('-') || !remotes.lines().any(|known| known.trim() == remote) {
        return Err(bad(format!("{remote:?} is not a configured remote")));
    }
    valid_branch(root, &params.branch).await?;
    branch_head(root, &params.branch).await?;

    let lease = if params.force_with_lease {
        // What this checkout last saw of the remote branch; nothing means
        // the branch must not exist there yet.
        Some(
            git_try(
                root,
                &[
                    "rev-parse",
                    "--verify",
                    "--quiet",
                    &format!("refs/remotes/{remote}/{}", params.branch),
                ],
            )
            .await
            .unwrap_or_default(),
        )
    } else {
        None
    };
    let args = push_args(
        remote,
        &params.branch,
        params.set_upstream,
        lease.as_deref(),
    );
    let output = run_git(root, &args, None).await?;
    if !output.success {
        // `--porcelain` marks a refused ref with `!`.
        let refused = output.stdout.lines().any(|line| line.starts_with('!'))
            || [
                "[rejected]",
                "stale info",
                "non-fast-forward",
                "fetch first",
            ]
            .iter()
            .any(|marker| output.stderr.contains(marker));
        return Err(if refused {
            reason(PUSH_REJECTED, output.detail())
        } else {
            failed(format!("git push failed: {}", output.detail()))
        });
    }
    let url = git_try(root, &["remote", "get-url", "--", remote])
        .await
        .map(|url| redact(&url));
    Ok(PushBranchReply {
        remote: remote.to_owned(),
        ref_name: format!("refs/heads/{}", params.branch),
        url,
    })
}

// ---------------------------------------------------------------------------
// G5: MergeBranch
// ---------------------------------------------------------------------------

/// One `git worktree list --porcelain` stanza.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorktreeStanza {
    pub path: PathBuf,
    pub head: String,
    /// Without `refs/heads/`; `None` when detached.
    pub branch: Option<String>,
    pub bare: bool,
    /// The first stanza: the main worktree.
    pub main: bool,
}

pub(crate) fn parse_worktrees(porcelain: &str) -> Vec<WorktreeStanza> {
    let mut out: Vec<WorktreeStanza> = Vec::new();
    for line in porcelain.lines() {
        if let Some(path) = line.strip_prefix("worktree ") {
            out.push(WorktreeStanza {
                path: PathBuf::from(path),
                head: String::new(),
                branch: None,
                bare: false,
                main: out.is_empty(),
            });
        } else if let Some(stanza) = out.last_mut() {
            if let Some(head) = line.strip_prefix("HEAD ") {
                stanza.head = head.to_owned();
            } else if let Some(branch) = line.strip_prefix("branch ") {
                stanza.branch = Some(
                    branch
                        .strip_prefix("refs/heads/")
                        .unwrap_or(branch)
                        .to_owned(),
                );
            } else if line == "bare" {
                stanza.bare = true;
            }
        }
    }
    out
}

pub(crate) async fn worktrees(repo: &Path) -> Result<Vec<WorktreeStanza>, GitWriteError> {
    Ok(parse_worktrees(
        &git_ok(repo, &["worktree", "list", "--porcelain"]).await?,
    ))
}

/// A worktree this merge adds and removes.
struct Scratch {
    repo: PathBuf,
    path: PathBuf,
}

impl Scratch {
    /// A new worktree under the repository's git directory, at `start`
    /// (`detach`: without checking a branch out).
    async fn add(repo: &Path, start: &str, detach: bool) -> Result<Self, GitWriteError> {
        let common = git_ok(
            repo,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )
        .await?;
        let base = PathBuf::from(common).join("loams-merge");
        std::fs::create_dir_all(&base).map_err(|error| failed(error.to_string()))?;
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.subsec_nanos())
            .unwrap_or_default();
        let path = base.join(format!("{}-{n}-{nanos}", std::process::id()));
        let mut args: Vec<OsString> = vec!["worktree".into(), "add".into(), "--quiet".into()];
        if detach {
            args.push("--detach".into());
        }
        args.push(path.clone().into_os_string());
        args.push(start.into());
        git_ok(repo, &args).await?;
        Ok(Self {
            repo: repo.to_path_buf(),
            path,
        })
    }

    /// Removes the worktree; it is always clean by then, so a plain removal
    /// works, and its files are deleted directly if git refuses.
    async fn remove(self) {
        let removed = run_git(
            &self.repo,
            &[
                OsString::from("worktree"),
                OsString::from("remove"),
                self.path.clone().into_os_string(),
            ],
            None,
        )
        .await
        .is_ok_and(|output| output.success);
        if !removed {
            let _ = std::fs::remove_dir_all(&self.path);
            let _ = run_git(&self.repo, &["worktree", "prune"], None).await;
        }
    }
}

/// The unmerged paths of an interrupted merge or rebase in `cwd`.
async fn unmerged(cwd: &Path) -> Vec<String> {
    git_try(cwd, &["diff", "--name-only", "--diff-filter=U"])
        .await
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .filter(|line| !line.is_empty())
        .collect()
}

/// Undo an interrupted merge (`abort` names how), check the tree is clean,
/// and answer `merge_conflict` with `conflicts`.
async fn stop_on_conflict(cwd: &Path, abort: &[&str], conflicts: Vec<String>) -> GitWriteError {
    let _ = run_git(cwd, abort, None).await;
    match has_changes(cwd, false).await {
        Ok(false) => {}
        _ => {
            return failed(format!(
                "the merge stopped on conflicts and the tree could not be cleaned: {}",
                cwd.display()
            ));
        }
    }
    let paths = serde_json::to_string(&conflicts).unwrap_or_else(|_| "[]".to_owned());
    GitWriteError::Failed(format!("{MERGE_CONFLICT}: {paths}"))
}

/// Merge `source` into `target` in `target`'s worktree (or a temporary one
/// when it isn't checked out), with the chosen strategy.
///
/// # Errors
///
/// `stale_diff` when the source moved; `merge_conflict: [paths]` with the
/// target's tree left clean; `nothing_to_commit` for an empty squash; bad
/// params; git's own failure.
pub async fn merge(
    _repos: &Repos,
    repo: &Path,
    params: &MergeBranchParams,
) -> Result<MergeBranchReply, GitWriteError> {
    let MergeBranchParams {
        source,
        target,
        strategy,
        ..
    } = params;
    if source == target {
        return Err(bad("source and target are the same branch"));
    }
    valid_branch(repo, source).await?;
    valid_branch(repo, target).await?;
    let source_head = branch_head(repo, source).await?;
    branch_head(repo, target).await?;
    if source_head != params.expected_source_head {
        return Err(reason(
            STALE_DIFF,
            format!("{source} moved since the merge was confirmed"),
        ));
    }

    let list = worktrees(repo).await?;
    let checked_out = |branch: &str| {
        list.iter()
            .find(|stanza| !stanza.bare && stanza.branch.as_deref() == Some(branch))
            .cloned()
    };
    let source_tree = checked_out(source);
    if params.remove_worktree {
        let Some(tree) = &source_tree else {
            return Err(bad(format!("{source} is not checked out in a worktree")));
        };
        if tree.main {
            return Err(bad("the main worktree is never removed"));
        }
        if has_changes(&tree.path, true).await? {
            return Err(failed(format!(
                "{source}'s worktree has uncommitted changes"
            )));
        }
    }
    if params.delete_source && source_tree.is_some() && !params.remove_worktree {
        return Err(bad(format!(
            "{source} is checked out in a worktree; remove the worktree to delete the branch"
        )));
    }

    // Where the target is merged into.
    let (target_tree, scratch) = match checked_out(target) {
        Some(tree) => {
            if has_changes(&tree.path, false).await? {
                return Err(failed(format!(
                    "{target}'s worktree has uncommitted changes"
                )));
            }
            (tree.path, None)
        }
        None => {
            let scratch = Scratch::add(repo, target, false).await?;
            (scratch.path.clone(), Some(scratch))
        }
    };

    let merged = merge_in(&target_tree, repo, params, &source_head, *strategy).await;
    if let Some(scratch) = scratch {
        scratch.remove().await;
    }
    let sha = merged?;

    if params.remove_worktree
        && let Some(tree) = &source_tree
    {
        let args = [
            OsString::from("worktree"),
            OsString::from("remove"),
            tree.path.clone().into_os_string(),
        ];
        if git_ok(repo, &args).await.is_err() {
            return Err(failed(format!(
                "merged as {sha}, but {source}'s worktree could not be removed"
            )));
        }
    }
    if params.delete_source {
        git_ok(repo, &["branch", "-D", "--", source])
            .await
            .map_err(|_| {
                failed(format!(
                    "merged as {sha}, but {source} could not be deleted"
                ))
            })?;
    }
    Ok(MergeBranchReply { sha })
}

/// The merge itself, in the target's worktree `tree`; answers the target's
/// new head.
async fn merge_in(
    tree: &Path,
    repo: &Path,
    params: &MergeBranchParams,
    source_head: &str,
    strategy: MergeStrategy,
) -> Result<String, GitWriteError> {
    let source = &params.source;
    let message = params
        .message
        .as_deref()
        .map(str::trim)
        .filter(|m| !m.is_empty());
    match strategy {
        MergeStrategy::Merge => {
            let message = message
                .map(str::to_owned)
                .unwrap_or_else(|| format!("Merge branch '{source}'"));
            let output = run_git(
                tree,
                &["merge", "--no-ff", "--no-edit", "-m", &message, source_head],
                None,
            )
            .await?;
            if !output.success {
                return Err(merge_failure(tree, &output, &["merge", "--abort"]).await);
            }
        }
        MergeStrategy::Squash => {
            let output = run_git(tree, &["merge", "--squash", source_head], None).await?;
            if !output.success {
                return Err(merge_failure(tree, &output, &["reset", "--merge"]).await);
            }
            if !has_changes(tree, false).await? {
                return Err(reason(NOTHING_TO_COMMIT, format!("{source} adds nothing")));
            }
            let message = message
                .map(str::to_owned)
                .unwrap_or_else(|| format!("Squash branch '{source}'"));
            let output = run_git(
                tree,
                &["commit", "--quiet", "--cleanup=strip", "-F", "-"],
                Some(message.as_bytes()),
            )
            .await?;
            if !output.success {
                let _ = run_git(tree, &["reset", "--merge"], None).await;
                return Err(failed(format!("git commit failed: {}", output.detail())));
            }
        }
        MergeStrategy::Rebase => {
            // Replay the source onto the target in a detached worktree, so
            // neither branch's own checkout is touched, then fast-forward.
            let target_head = git_ok(tree, &["rev-parse", "HEAD"]).await?;
            let replay = Scratch::add(repo, source_head, true).await?;
            let output = run_git(&replay.path, &["rebase", "--quiet", &target_head], None).await;
            let rebased = match output {
                Ok(output) if output.success => git_ok(&replay.path, &["rev-parse", "HEAD"]).await,
                Ok(output) => {
                    let conflicts = unmerged(&replay.path).await;
                    let error = if conflicts.is_empty() {
                        let _ = run_git(&replay.path, &["rebase", "--abort"], None).await;
                        failed(format!("git rebase failed: {}", output.detail()))
                    } else {
                        stop_on_conflict(&replay.path, &["rebase", "--abort"], conflicts).await
                    };
                    Err(error)
                }
                Err(error) => Err(error),
            };
            replay.remove().await;
            let rebased = rebased?;
            let output = run_git(tree, &["merge", "--ff-only", "--quiet", &rebased], None).await?;
            if !output.success {
                return Err(failed(format!(
                    "the rebased commits could not be fast-forwarded: {}",
                    output.detail()
                )));
            }
        }
        MergeStrategy::Ff => {
            let output =
                run_git(tree, &["merge", "--ff-only", "--quiet", source_head], None).await?;
            if !output.success {
                return Err(failed(format!(
                    "{} cannot be fast-forwarded to {source}: {}",
                    params.target,
                    output.detail()
                )));
            }
        }
    }
    git_ok(tree, &["rev-parse", "HEAD"]).await
}

/// A failed `merge`: a conflict is aborted and reported with its paths;
/// anything else (git refused to start) is reported as it is.
async fn merge_failure(tree: &Path, output: &GitOutput, abort: &[&str]) -> GitWriteError {
    let conflicts = unmerged(tree).await;
    if conflicts.is_empty() {
        let _ = run_git(tree, abort, None).await;
        return failed(format!("git merge failed: {}", output.detail()));
    }
    stop_on_conflict(tree, abort, conflicts).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_strips_user_information() {
        assert_eq!(
            redact("fatal: https://me:s3cret@forgejo.example/a/b.git refused"),
            "fatal: https://forgejo.example/a/b.git refused"
        );
        assert_eq!(redact("ssh://git@host:22/a"), "ssh://host:22/a");
        assert_eq!(redact("no url here"), "no url here");
        assert_eq!(redact("https://host/a@b"), "https://host/a@b");
    }

    #[test]
    fn relative_paths_only() {
        assert!(checkout_relative("src/main.rs").is_ok());
        assert!(checkout_relative("./a").is_ok());
        for path in ["", "/etc/passwd", "../x", "a/../../x", "a\0b", "\\x"] {
            assert!(checkout_relative(path).is_err(), "{path:?}");
        }
    }

    #[test]
    fn worktree_porcelain_parses() {
        let list = parse_worktrees(
            "worktree /r\nHEAD aaa\nbranch refs/heads/main\n\n\
             worktree /w\nHEAD bbb\ndetached\n\nworktree /x\nHEAD ccc\nbranch refs/heads/a/b\n",
        );
        assert_eq!(list.len(), 3);
        assert!(list[0].main && !list[1].main);
        assert_eq!(list[0].branch.as_deref(), Some("main"));
        assert_eq!(list[1].branch, None);
        assert_eq!(list[2].branch.as_deref(), Some("a/b"));
        assert_eq!(list[2].head, "ccc");
    }
}
