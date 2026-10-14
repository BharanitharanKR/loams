//! Review comments on a checkout's diff (design §53 §5.6, §6.5 G8–G11; plan
//! SD1 Task 11).
//!
//! - [`ReviewComments`] keeps the comments in the daemon's SQLite store
//!   (`review_comments`), keyed by checkout, and tells watchers which
//!   checkout changed.
//! - [`watch`] streams a checkout's comments: a reset, then upserts and
//!   removals. `outdated` is computed, never stored ([`is_outdated`]): a
//!   comment is outdated when its diff checksum is not the checkout's
//!   current one and its anchored lines are no longer lines of the current
//!   diff on its side.
//! - [`prompt`] is `SendReviewToThread`'s fixed template; each body is quoted
//!   as data ([`quote`]), so a comment can't end the list or add a line of
//!   its own.
//! - [`post_review`] posts comments as a review on the branch's open pull
//!   request: Forgejo through its REST API with the factory credential
//!   (ruling T9-3's path: only to its own host, `https`, no redirects),
//!   GitHub through `gh api`.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use loams_agentd_proto::{CheckoutDiff, DiffSide, ReviewComment, ReviewCommentsFrame, ReviewEvent};
use loams_agentd_store::{DocsStore, ReviewCommentRow};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::{broadcast, watch as tokio_watch};

use crate::change_request_create::{ChangeRequestCreator, forgejo_api_url, not_configured};
use crate::git_write::{GitWriteError, bad, checkout_relative, failed, git_try, redact};
use crate::repos::Repos;
use crate::source_control::GitRemote;

/// At most this many comments per checkout.
pub const MAX_COMMENTS_PER_CHECKOUT: usize = 1_000;
/// A comment's body is at most this long.
pub const MAX_BODY_BYTES: usize = 64 * 1024;
/// `SendReviewToThread` and `PostReviewToChangeRequest` take at most this
/// many comments.
pub const MAX_COMMENT_IDS: usize = 200;
/// The first line of `SendReviewToThread`'s prompt.
pub const PROMPT_HEADER: &str = "Please address these review comments:";
/// The body of a review posted to a pull request; the comments carry the
/// substance.
pub const POSTED_REVIEW_BODY: &str = "Review comments from Loams Studio.";
/// How often a watch re-reads a checkout the diff sync does not track.
const UNTRACKED_REFRESH: Duration = Duration::from_secs(15);
const HTTP_PAGE: usize = 50;
const HTTP_PAGES: usize = 10;
const GH_TIMEOUT: Duration = Duration::from_secs(60);

/// A review-comment call that did not happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewError {
    /// The request itself is wrong (`bad_params`).
    BadParams(String),
    /// No such comment in that checkout (`not_found`).
    NotFound(String),
    /// The store failed.
    Failed(String),
}

impl std::fmt::Display for ReviewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadParams(text) | Self::Failed(text) => f.write_str(text),
            Self::NotFound(text) => write!(f, "not_found: {text}"),
        }
    }
}

impl std::error::Error for ReviewError {}

fn store_failed(error: loams_agentd_store::StoreError) -> ReviewError {
    ReviewError::Failed(format!("the review store failed: {error}"))
}

fn side_name(side: DiffSide) -> &'static str {
    match side {
        DiffSide::Old => "old",
        DiffSide::New => "new",
    }
}

fn to_comment(row: ReviewCommentRow) -> ReviewComment {
    ReviewComment {
        id: row.id,
        checkout_id: row.checkout_id,
        path: row.path,
        side: if row.side == "old" {
            DiffSide::Old
        } else {
            DiffSide::New
        },
        line: row.line,
        end_line: row.end_line,
        body: row.body,
        diff_checksum: row.diff_checksum,
        created_at_ms: row.created_at_ms,
        resolved: row.resolved,
        outdated: false,
    }
}

/// The daemon's review comments.
#[derive(Clone)]
pub struct ReviewComments {
    store: Arc<DocsStore>,
    /// The checkout whose comments changed.
    changed: broadcast::Sender<String>,
}

impl std::fmt::Debug for ReviewComments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReviewComments").finish_non_exhaustive()
    }
}

impl ReviewComments {
    pub fn new(store: Arc<DocsStore>) -> Self {
        let (changed, _) = broadcast::channel(64);
        Self { store, changed }
    }

    /// Notices of the checkouts whose comments changed.
    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.changed.subscribe()
    }

    fn notify(&self, checkout_id: &str) {
        // No receiver is fine: nobody is watching.
        let _ = self.changed.send(checkout_id.to_owned());
    }

    /// A checkout's comments, oldest first (`outdated` not computed).
    ///
    /// # Errors
    ///
    /// The store failed.
    pub async fn list(&self, checkout_id: &str) -> Result<Vec<ReviewComment>, ReviewError> {
        let store = Arc::clone(&self.store);
        let checkout_id = checkout_id.to_owned();
        tokio::task::spawn_blocking(move || store.review_comments(&checkout_id))
            .await
            .map_err(|error| ReviewError::Failed(error.to_string()))?
            .map(|rows| rows.into_iter().map(to_comment).collect())
            .map_err(store_failed)
    }

    /// These comments of a checkout, in the order asked, each once.
    ///
    /// # Errors
    ///
    /// `bad_params` for no ids or too many; `not_found` for an id the
    /// checkout doesn't have.
    pub async fn select(
        &self,
        checkout_id: &str,
        ids: &[String],
    ) -> Result<Vec<ReviewComment>, ReviewError> {
        if ids.is_empty() {
            return Err(ReviewError::BadParams("commentIds is empty".into()));
        }
        if ids.len() > MAX_COMMENT_IDS {
            return Err(ReviewError::BadParams(format!(
                "at most {MAX_COMMENT_IDS} comments at a time"
            )));
        }
        let mut by_id: HashMap<String, ReviewComment> = self
            .list(checkout_id)
            .await?
            .into_iter()
            .map(|comment| (comment.id.clone(), comment))
            .collect();
        let mut out = Vec::with_capacity(ids.len());
        let mut seen = BTreeSet::new();
        for id in ids {
            if !seen.insert(id.as_str()) {
                continue;
            }
            let comment = by_id
                .remove(id)
                .ok_or_else(|| ReviewError::NotFound(format!("review comment {id:?}")))?;
            out.push(comment);
        }
        Ok(out)
    }

    /// Create (empty `id`) or edit one comment; answers its id.
    ///
    /// # Errors
    ///
    /// `bad_params` for an invalid anchor or body; `not_found` to edit a
    /// comment the checkout doesn't have.
    pub async fn upsert(&self, comment: ReviewComment) -> Result<String, ReviewError> {
        validate(&comment)?;
        let store = Arc::clone(&self.store);
        let checkout_id = comment.checkout_id.clone();
        let id = tokio::task::spawn_blocking(move || -> Result<String, ReviewError> {
            let (id, created_at_ms) = if comment.id.is_empty() {
                if store
                    .review_comment_count(&comment.checkout_id)
                    .map_err(store_failed)?
                    >= MAX_COMMENTS_PER_CHECKOUT
                {
                    return Err(ReviewError::BadParams(format!(
                        "a checkout has at most {MAX_COMMENTS_PER_CHECKOUT} review comments"
                    )));
                }
                (crate::new_id(), crate::now_ms())
            } else {
                let existing = store
                    .review_comment(&comment.checkout_id, &comment.id)
                    .map_err(store_failed)?
                    .ok_or_else(|| {
                        ReviewError::NotFound(format!("review comment {:?}", comment.id))
                    })?;
                (existing.id, existing.created_at_ms)
            };
            let row = ReviewCommentRow {
                id: id.clone(),
                checkout_id: comment.checkout_id,
                path: comment.path,
                side: side_name(comment.side).to_owned(),
                line: comment.line,
                end_line: comment.end_line,
                body: comment.body,
                diff_checksum: comment.diff_checksum,
                created_at_ms,
                resolved: comment.resolved,
            };
            if !store.upsert_review_comment(&row).map_err(store_failed)? {
                return Err(ReviewError::NotFound(format!("review comment {id:?}")));
            }
            Ok(id)
        })
        .await
        .map_err(|error| ReviewError::Failed(error.to_string()))??;
        self.notify(&checkout_id);
        Ok(id)
    }

    /// Delete one comment.
    ///
    /// # Errors
    ///
    /// `not_found` when the checkout has no such comment.
    pub async fn delete(&self, checkout_id: &str, id: &str) -> Result<(), ReviewError> {
        let store = Arc::clone(&self.store);
        let (checkout, comment) = (checkout_id.to_owned(), id.to_owned());
        let deleted =
            tokio::task::spawn_blocking(move || store.delete_review_comment(&checkout, &comment))
                .await
                .map_err(|error| ReviewError::Failed(error.to_string()))?
                .map_err(store_failed)?;
        if !deleted {
            return Err(ReviewError::NotFound(format!("review comment {id:?}")));
        }
        self.notify(checkout_id);
        Ok(())
    }

    /// Mark one comment resolved, or not.
    ///
    /// # Errors
    ///
    /// `not_found` when the checkout has no such comment.
    pub async fn resolve(
        &self,
        checkout_id: &str,
        id: &str,
        resolved: bool,
    ) -> Result<(), ReviewError> {
        let store = Arc::clone(&self.store);
        let (checkout, comment) = (checkout_id.to_owned(), id.to_owned());
        let changed = tokio::task::spawn_blocking(move || {
            store.resolve_review_comment(&checkout, &comment, resolved)
        })
        .await
        .map_err(|error| ReviewError::Failed(error.to_string()))?
        .map_err(store_failed)?;
        if !changed {
            return Err(ReviewError::NotFound(format!("review comment {id:?}")));
        }
        self.notify(checkout_id);
        Ok(())
    }
}

/// A comment's anchor and body, as an upsert receives them.
fn validate(comment: &ReviewComment) -> Result<(), ReviewError> {
    let bad = |text: String| ReviewError::BadParams(text);
    if comment.checkout_id.is_empty() {
        return Err(bad("checkoutId is required".into()));
    }
    checkout_relative(&comment.path).map_err(|error| bad(error.to_string()))?;
    if comment.path.len() > 4096 || comment.path.contains(['\n', '\r']) {
        return Err(bad(format!("not a checkout path: {:?}", comment.path)));
    }
    if comment.line == 0 {
        return Err(bad("line is 1-based".into()));
    }
    if comment.end_line.is_some_and(|end| end < comment.line) {
        return Err(bad("endLine is before line".into()));
    }
    if comment.body.trim().is_empty() {
        return Err(bad("a review comment needs a body".into()));
    }
    if comment.body.len() > MAX_BODY_BYTES {
        return Err(bad("the body is longer than 64 KiB".into()));
    }
    if comment.diff_checksum.is_empty() || comment.diff_checksum.len() > 128 {
        return Err(bad("diffChecksum is required".into()));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Outdated comments
// ---------------------------------------------------------------------------

/// What [`is_outdated`] compares a comment against: a checkout's current
/// working-tree diff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentDiff {
    pub checksum: String,
    pub patch: String,
    pub truncated: bool,
}

impl From<&CheckoutDiff> for CurrentDiff {
    fn from(diff: &CheckoutDiff) -> Self {
        Self {
            checksum: diff.checksum.clone(),
            patch: diff.patch.clone(),
            truncated: diff.truncated,
        }
    }
}

/// The line numbers `path` shows on `side` in a unified `patch` (context
/// lines on both sides; removed lines on the old side, added lines on the
/// new), or `None` when the patch has no section for `path`.
pub fn diff_lines(patch: &str, path: &str, side: DiffSide) -> Option<BTreeSet<u32>> {
    let old_name = format!("a/{path}");
    let new_name = format!("b/{path}");
    let mut found: Option<BTreeSet<u32>> = None;
    let mut in_file = false;
    let mut old_line = 0u32;
    let mut new_line = 0u32;
    let mut in_hunk = false;
    for line in patch.lines() {
        if line.starts_with("diff --git ") {
            in_file = false;
            in_hunk = false;
            continue;
        }
        if let Some(name) = line.strip_prefix("--- ") {
            in_hunk = false;
            if name == old_name {
                in_file = true;
                found.get_or_insert_with(BTreeSet::new);
            }
            continue;
        }
        if let Some(name) = line.strip_prefix("+++ ") {
            in_hunk = false;
            if name == new_name {
                in_file = true;
                found.get_or_insert_with(BTreeSet::new);
            }
            continue;
        }
        if !in_file {
            continue;
        }
        if let Some(header) = line.strip_prefix("@@ ") {
            // `@@ -a,b +c,d @@`
            let mut ranges = header.split_whitespace();
            let start = |range: Option<&str>, sign: char| -> Option<u32> {
                range?.strip_prefix(sign)?.split(',').next()?.parse().ok()
            };
            old_line = start(ranges.next(), '-').unwrap_or(0);
            new_line = start(ranges.next(), '+').unwrap_or(0);
            in_hunk = true;
            continue;
        }
        if !in_hunk {
            continue;
        }
        let Some(lines) = found.as_mut() else {
            continue;
        };
        match line.chars().next() {
            Some(' ') => {
                lines.insert(if side == DiffSide::Old {
                    old_line
                } else {
                    new_line
                });
                old_line += 1;
                new_line += 1;
            }
            Some('-') => {
                if side == DiffSide::Old {
                    lines.insert(old_line);
                }
                old_line += 1;
            }
            Some('+') => {
                if side == DiffSide::New {
                    lines.insert(new_line);
                }
                new_line += 1;
            }
            // `\ No newline at end of file`, and anything else.
            _ => {}
        }
    }
    found
}

/// Whether `comment`'s anchor no longer matches `current`: another checksum,
/// and a line of its range that the current diff no longer shows on its
/// side. A truncated diff that lacks the file says nothing either way, so
/// the comment is not outdated.
pub fn is_outdated(comment: &ReviewComment, current: &CurrentDiff) -> bool {
    if comment.diff_checksum == current.checksum {
        return false;
    }
    let Some(lines) = diff_lines(&current.patch, &comment.path, comment.side) else {
        return !current.truncated;
    };
    let end = comment.end_line.unwrap_or(comment.line).max(comment.line);
    !(comment.line..=end).all(|line| lines.contains(&line))
}

/// Why [`refuse_outdated`] refused to post.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotPostable {
    /// The checkout's diff could not be read, so stale anchors can't be ruled
    /// out.
    DiffUnreadable(String),
    /// These comments' anchors no longer match the diff.
    Outdated(Vec<String>),
}

/// Marks `comments` against `current`, a fresh capture of the checkout's
/// working-tree diff, and refuses when any is outdated. Fails closed: a diff
/// that could not be read refuses them all (SD1 Task 30; before, a failed
/// capture skipped the check and posted whatever anchors there were).
///
/// # Errors
///
/// [`NotPostable`].
pub fn refuse_outdated(
    comments: &mut [ReviewComment],
    current: Result<CurrentDiff, String>,
) -> Result<(), NotPostable> {
    let current = current.map_err(NotPostable::DiffUnreadable)?;
    for comment in comments.iter_mut() {
        comment.outdated = is_outdated(comment, &current);
    }
    let outdated: Vec<String> = comments
        .iter()
        .filter(|comment| comment.outdated)
        .map(|comment| comment.id.clone())
        .collect();
    if outdated.is_empty() {
        Ok(())
    } else {
        Err(NotPostable::Outdated(outdated))
    }
}

// ---------------------------------------------------------------------------
// WatchReviewComments
// ---------------------------------------------------------------------------

/// Where a watch reads its checkout's current diff: the diff sync's published
/// one when it tracks the checkout, else a capture of `root`.
#[derive(Clone)]
pub struct DiffSource {
    pub diffs: tokio_watch::Receiver<Vec<CheckoutDiff>>,
    pub repos: Repos,
    pub root: PathBuf,
}

impl std::fmt::Debug for DiffSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiffSource")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl DiffSource {
    async fn current(&self, checkout_id: &str) -> Option<CurrentDiff> {
        let published = self
            .diffs
            .borrow()
            .iter()
            .find(|diff| diff.checkout_id == checkout_id)
            .map(CurrentDiff::from);
        if published.is_some() {
            return published;
        }
        crate::diff_sync::capture_diff(&self.repos, &self.root)
            .await
            .ok()
            .map(|snapshot| CurrentDiff {
                checksum: snapshot.checksum,
                patch: snapshot.patch,
                truncated: snapshot.truncated,
            })
    }

    fn tracked(&self, checkout_id: &str) -> bool {
        self.diffs
            .borrow()
            .iter()
            .any(|diff| diff.checkout_id == checkout_id)
    }
}

struct WatchState {
    comments: ReviewComments,
    changed: broadcast::Receiver<String>,
    source: DiffSource,
    checkout_id: String,
    /// What the client holds, by id; `None` before the reset.
    sent: Option<HashMap<String, ReviewComment>>,
}

impl WatchState {
    async fn snapshot(&self) -> Option<Vec<ReviewComment>> {
        let mut comments = self.comments.list(&self.checkout_id).await.ok()?;
        if let Some(current) = self.source.current(&self.checkout_id).await {
            for comment in &mut comments {
                comment.outdated = is_outdated(comment, &current);
            }
        }
        Some(comments)
    }

    /// Wait until the comments or the diff may have changed.
    async fn wake(&mut self) -> bool {
        let tracked = self.source.tracked(&self.checkout_id);
        loop {
            tokio::select! {
                notice = self.changed.recv() => match notice {
                    Ok(checkout) if checkout == self.checkout_id => return true,
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => return true,
                    Err(broadcast::error::RecvError::Closed) => return false,
                },
                changed = self.source.diffs.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                    return true;
                }
                () = tokio::time::sleep(UNTRACKED_REFRESH), if !tracked => return true,
            }
        }
    }

    async fn next(&mut self) -> Option<ReviewCommentsFrame> {
        if self.sent.is_none() {
            let comments = self.snapshot().await?;
            self.sent = Some(
                comments
                    .iter()
                    .map(|comment| (comment.id.clone(), comment.clone()))
                    .collect(),
            );
            return Some(ReviewCommentsFrame::Reset { reset: comments });
        }
        loop {
            if !self.wake().await {
                return None;
            }
            let Some(comments) = self.snapshot().await else {
                continue;
            };
            let sent = self.sent.get_or_insert_with(HashMap::new);
            let mut upsert = Vec::new();
            let mut now = HashMap::with_capacity(comments.len());
            for comment in comments {
                if sent.get(&comment.id) != Some(&comment) {
                    upsert.push(comment.clone());
                }
                now.insert(comment.id.clone(), comment);
            }
            let mut remove: Vec<String> = sent
                .keys()
                .filter(|id| !now.contains_key(*id))
                .cloned()
                .collect();
            remove.sort();
            *sent = now;
            if !upsert.is_empty() || !remove.is_empty() {
                return Some(ReviewCommentsFrame::Delta { upsert, remove });
            }
        }
    }
}

/// `WatchReviewComments`: the checkout's comments, then each change, with
/// `outdated` following its diff.
pub fn watch(
    comments: ReviewComments,
    checkout_id: String,
    source: DiffSource,
) -> impl Stream<Item = ReviewCommentsFrame> + Send + 'static {
    let state = WatchState {
        changed: comments.subscribe(),
        comments,
        source,
        checkout_id,
        sent: None,
    };
    futures::stream::unfold(state, |mut state| async move {
        let frame = state.next().await?;
        Some((frame, state))
    })
}

// ---------------------------------------------------------------------------
// SendReviewToThread
// ---------------------------------------------------------------------------

/// `text` as a double-quoted string's inside: `\` and `"` escaped, and
/// line breaks and other control characters written as escapes, so a quoted
/// body is always one line that ends where its quote ends.
pub fn quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() || matches!(c, '\u{2028}' | '\u{2029}') => {
                out.push_str(&format!("\\u{{{:04x}}}", u32::from(c)));
            }
            c => out.push(c),
        }
    }
    out
}

/// `SendReviewToThread`'s prompt (SD1 Task 11's fixed template):
///
/// ```text
/// Please address these review comments:
/// - <path>:<line> — "<body>"
/// ```
///
/// A range is `<line>-<endLine>`; a comment on the old side says so.
pub fn prompt(comments: &[ReviewComment]) -> String {
    let mut out = String::from(PROMPT_HEADER);
    for comment in comments {
        let mut anchor = format!("{}:{}", quote(&comment.path), comment.line);
        if let Some(end) = comment.end_line.filter(|end| *end > comment.line) {
            anchor.push_str(&format!("-{end}"));
        }
        if comment.side == DiffSide::Old {
            anchor.push_str(" (old)");
        }
        out.push_str(&format!("\n- {anchor} — \"{}\"", quote(&comment.body)));
    }
    out
}

// ---------------------------------------------------------------------------
// PostReviewToChangeRequest
// ---------------------------------------------------------------------------

/// The pull request a review goes to.
struct PullRequest {
    number: u64,
    url: String,
}

/// The checkout's branch, as its remote knows it, and that remote.
async fn branch_and_remote(
    creator: &ChangeRequestCreator,
    root: &Path,
) -> Result<(String, GitRemote), GitWriteError> {
    let branch = git_try(root, &["symbolic-ref", "--quiet", "--short", "HEAD"])
        .await
        .filter(|branch| !branch.is_empty())
        .ok_or_else(|| bad("the checkout is not on a branch"))?;
    let remote = creator.remote(root, &branch).await?;
    let merge = git_try(
        root,
        &["config", "--get", &format!("branch.{branch}.merge")],
    )
    .await;
    let upstream = merge
        .as_deref()
        .and_then(|merge| merge.strip_prefix("refs/heads/"))
        .filter(|name| !name.is_empty())
        .map_or_else(|| branch.clone(), str::to_owned);
    Ok((upstream, remote))
}

/// Post `comments` (all on the checkout at `root`) as one review on its
/// branch's open pull request; answers the review's URL. GitHub
/// (`github.com`) goes through `gh api`; any other host through Forgejo's
/// REST API with the factory credential for that host.
///
/// # Errors
///
/// `provider_not_configured` without a credential (or `gh`), `not_found`
/// without an open pull request, or the provider's refusal.
pub async fn post_review(
    creator: &ChangeRequestCreator,
    root: &Path,
    comments: &[ReviewComment],
    event: ReviewEvent,
) -> Result<String, GitWriteError> {
    if comments.is_empty() {
        return Err(bad("commentIds is empty"));
    }
    let (branch, remote) = branch_and_remote(creator, root).await?;
    if remote.host.eq_ignore_ascii_case("github.com") {
        github_review(creator, root, &remote, &branch, comments, event).await
    } else {
        forgejo_review(creator, &remote, &branch, comments, event).await
    }
}

fn no_pull_request(branch: &str) -> GitWriteError {
    failed(format!("not_found: no open pull request for {branch:?}"))
}

fn http_failure(
    provider: &str,
    status: reqwest::StatusCode,
    reply: &serde_json::Value,
) -> GitWriteError {
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        return not_configured(format!(
            "{provider} refused the credential (HTTP {})",
            status.as_u16()
        ));
    }
    let message = reply
        .get("message")
        .and_then(serde_json::Value::as_str)
        .map(|message| redact(message.chars().take(512).collect::<String>().as_str()))
        .unwrap_or_default();
    failed(format!("{provider}: HTTP {} {message}", status.as_u16()))
}

async fn forgejo_review(
    creator: &ChangeRequestCreator,
    remote: &GitRemote,
    branch: &str,
    comments: &[ReviewComment],
    event: ReviewEvent,
) -> Result<String, GitWriteError> {
    let (base, token) = creator.forgejo_credential_for(remote).await?;
    let authorization = format!("token {token}");
    let send_failed = |error: reqwest::Error| {
        failed(format!(
            "forgejo: the request failed: {}",
            redact(&error.without_url().to_string())
        ))
    };

    // The branch's open pull request in this repository.
    let mut pull = None;
    for page in 1..=HTTP_PAGES {
        let mut url = forgejo_api_url(&base, remote, &["pulls"])?;
        url.query_pairs_mut()
            .append_pair("state", "open")
            .append_pair("page", &page.to_string())
            .append_pair("limit", &HTTP_PAGE.to_string());
        let response = creator
            .http()
            .get(url)
            .header("authorization", &authorization)
            .header("accept", "application/json")
            .send()
            .await
            .map_err(send_failed)?;
        let status = response.status();
        let reply: serde_json::Value = response.json().await.unwrap_or_default();
        if !status.is_success() {
            return Err(http_failure("forgejo", status, &reply));
        }
        let pulls = reply.as_array().cloned().unwrap_or_default();
        pull = pulls.iter().find_map(|candidate| {
            let head = candidate.get("head")?;
            if head.get("ref").and_then(serde_json::Value::as_str) != Some(branch) {
                return None;
            }
            // A fork's branch of the same name is another branch.
            if let Some(name) = head
                .get("repo")
                .and_then(|repo| repo.get("full_name"))
                .and_then(serde_json::Value::as_str)
                && !name.eq_ignore_ascii_case(&format!("{}/{}", remote.owner, remote.repository))
            {
                return None;
            }
            Some(PullRequest {
                number: candidate.get("number")?.as_u64()?,
                url: candidate
                    .get("html_url")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            })
        });
        if pull.is_some() || pulls.len() < HTTP_PAGE {
            break;
        }
    }
    let pull = pull.ok_or_else(|| no_pull_request(branch))?;

    let number = pull.number.to_string();
    let url = forgejo_api_url(&base, remote, &["pulls", &number, "reviews"])?;
    let body = serde_json::json!({
        "body": POSTED_REVIEW_BODY,
        "event": match event {
            ReviewEvent::Comment => "COMMENT",
            ReviewEvent::RequestChanges => "REQUEST_CHANGES",
        },
        "comments": comments.iter().map(|comment| match comment.side {
            DiffSide::New => serde_json::json!({
                "path": comment.path,
                "body": comment.body,
                "new_position": comment.line,
            }),
            DiffSide::Old => serde_json::json!({
                "path": comment.path,
                "body": comment.body,
                "old_position": comment.line,
            }),
        }).collect::<Vec<_>>(),
    });
    let response = creator
        .http()
        .post(url)
        .header("authorization", &authorization)
        .header("accept", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(send_failed)?;
    let status = response.status();
    let reply: serde_json::Value = response.json().await.unwrap_or_default();
    if !status.is_success() {
        return Err(http_failure("forgejo", status, &reply));
    }
    Ok(reply
        .get("html_url")
        .and_then(serde_json::Value::as_str)
        .filter(|url| !url.is_empty())
        .map_or(pull.url, str::to_owned))
}

/// `gh api` with `args`, `stdin` written when given; answers stdout as JSON.
async fn gh_api(
    creator: &ChangeRequestCreator,
    root: &Path,
    args: &[String],
    stdin: Option<&[u8]>,
) -> Result<serde_json::Value, GitWriteError> {
    let mut command = creator.gh_command(root);
    command.arg("api").args(args).stdin(if stdin.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(not_configured("gh is not installed"));
        }
        Err(error) => return Err(failed(format!("gh could not start: {error}"))),
    };
    if let Some(bytes) = stdin
        && let Some(mut pipe) = child.stdin.take()
    {
        pipe.write_all(bytes)
            .await
            .map_err(|error| failed(format!("gh stdin failed: {error}")))?;
        drop(pipe);
    }
    let output = tokio::time::timeout(GH_TIMEOUT, child.wait_with_output())
        .await
        .map_err(|_| failed("gh timed out"))?
        .map_err(|error| failed(format!("gh failed: {error}")))?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        if stderr.contains("gh auth login") || stderr.contains("not logged in") {
            return Err(not_configured("gh is not signed in"));
        }
        let detail: String = stderr.trim().chars().take(1024).collect();
        return Err(failed(format!("gh: {}", redact(&detail))));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|_| failed("gh printed something other than JSON"))
}

async fn github_review(
    creator: &ChangeRequestCreator,
    root: &Path,
    remote: &GitRemote,
    branch: &str,
    comments: &[ReviewComment],
    event: ReviewEvent,
) -> Result<String, GitWriteError> {
    let repo = format!("repos/{}/{}", remote.owner, remote.repository);
    let hostname = format!("--hostname={}", remote.host);
    let mut head =
        reqwest::Url::parse("https://localhost/").map_err(|error| failed(error.to_string()))?;
    head.query_pairs_mut()
        .append_pair("head", &format!("{}:{branch}", remote.owner))
        .append_pair("state", "open");
    let query = head.query().unwrap_or_default();
    let pulls = gh_api(
        creator,
        root,
        &[hostname.clone(), format!("{repo}/pulls?{query}")],
        None,
    )
    .await?;
    let pull = pulls
        .as_array()
        .and_then(|pulls| pulls.first())
        .and_then(|pull| {
            Some(PullRequest {
                number: pull.get("number")?.as_u64()?,
                url: pull
                    .get("html_url")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            })
        })
        .ok_or_else(|| no_pull_request(branch))?;

    let body = serde_json::json!({
        "body": POSTED_REVIEW_BODY,
        "event": match event {
            ReviewEvent::Comment => "COMMENT",
            ReviewEvent::RequestChanges => "REQUEST_CHANGES",
        },
        "comments": comments.iter().map(|comment| {
            let side = match comment.side {
                DiffSide::Old => "LEFT",
                DiffSide::New => "RIGHT",
            };
            match comment.end_line.filter(|end| *end > comment.line) {
                Some(end) => serde_json::json!({
                    "path": comment.path,
                    "body": comment.body,
                    "start_line": comment.line,
                    "start_side": side,
                    "line": end,
                    "side": side,
                }),
                None => serde_json::json!({
                    "path": comment.path,
                    "body": comment.body,
                    "line": comment.line,
                    "side": side,
                }),
            }
        }).collect::<Vec<_>>(),
    });
    let payload = serde_json::to_vec(&body).map_err(|error| failed(error.to_string()))?;
    let review = gh_api(
        creator,
        root,
        &[
            hostname,
            "--method=POST".to_owned(),
            format!("{repo}/pulls/{}/reviews", pull.number),
            "--input=-".to_owned(),
        ],
        Some(&payload),
    )
    .await?;
    Ok(review
        .get("html_url")
        .and_then(serde_json::Value::as_str)
        .filter(|url| !url.is_empty())
        .map_or(pull.url, str::to_owned))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn comment(path: &str, side: DiffSide, line: u32, end: Option<u32>) -> ReviewComment {
        ReviewComment {
            id: "c1".into(),
            checkout_id: "k".into(),
            path: path.into(),
            side,
            line,
            end_line: end,
            body: "Why?".into(),
            diff_checksum: "old-sum".into(),
            created_at_ms: 1,
            resolved: false,
            outdated: false,
        }
    }

    const PATCH: &str = "diff --git a/notes.txt b/notes.txt\n\
index 1..2 100644\n\
--- a/notes.txt\n\
+++ b/notes.txt\n\
@@ -1,2 +1,3 @@\n\
\x20first\n\
-second\n\
+changed\n\
+added\n\
diff --git a/other.txt b/other.txt\n\
--- a/other.txt\n\
+++ b/other.txt\n\
@@ -5 +5 @@\n\
-x\n\
+y\n";

    #[test]
    fn diff_lines_follow_each_side() {
        let new: Vec<u32> = diff_lines(PATCH, "notes.txt", DiffSide::New)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(new, [1, 2, 3]);
        let old: Vec<u32> = diff_lines(PATCH, "notes.txt", DiffSide::Old)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(old, [1, 2]);
        let other: Vec<u32> = diff_lines(PATCH, "other.txt", DiffSide::New)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(other, [5]);
        assert!(diff_lines(PATCH, "missing.txt", DiffSide::New).is_none());
    }

    #[test]
    fn outdated_needs_another_checksum_and_a_missing_line() {
        let current = CurrentDiff {
            checksum: "new-sum".into(),
            patch: PATCH.into(),
            truncated: false,
        };
        assert!(!is_outdated(
            &comment("notes.txt", DiffSide::New, 3, None),
            &current
        ));
        assert!(is_outdated(
            &comment("notes.txt", DiffSide::New, 4, None),
            &current
        ));
        assert!(is_outdated(
            &comment("notes.txt", DiffSide::New, 2, Some(4)),
            &current
        ));
        assert!(is_outdated(
            &comment("gone.txt", DiffSide::New, 1, None),
            &current
        ));
        let same = CurrentDiff {
            checksum: "old-sum".into(),
            ..current.clone()
        };
        assert!(!is_outdated(
            &comment("gone.txt", DiffSide::New, 1, None),
            &same
        ));
        let truncated = CurrentDiff {
            truncated: true,
            ..current
        };
        assert!(!is_outdated(
            &comment("gone.txt", DiffSide::New, 1, None),
            &truncated
        ));
    }

    #[test]
    fn quote_keeps_a_body_on_one_line() {
        assert_eq!(
            quote("a \"b\"\n- c\\d\u{7}"),
            "a \\\"b\\\"\\n- c\\\\d\\u{0007}"
        );
    }
}
