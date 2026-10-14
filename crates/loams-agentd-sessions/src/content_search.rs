//! `SearchWorkspaceContent` (design §53 §6.5 G23; plan SD1 Task 11): a
//! streamed search of one workspace's file contents, for the palette and the
//! search dialog.
//!
//! - The root is the workspace target's (a chat's checkout or a space's), as
//!   the file tree resolves it, so the search sees exactly what the file tree
//!   may show.
//! - The `ignore` walker honours `.gitignore`, `.ignore` and git's excludes,
//!   never follows a symbolic link and never enters `.git`.
//! - A query is a literal or a regular expression (the `regex` crate, linear
//!   time, with a compiled-size limit), case-sensitive or not; globs narrow
//!   the paths.
//! - Caps: [`MAX_RESULTS`] matches, the first [`MAX_FILE_BYTES`] of a file,
//!   binary files skipped, and [`SEARCH_DEADLINE`] in all; the stream simply
//!   ends at a cap. A dropped stream stops the walk.

use std::io::Read as _;
use std::path::{Component, Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::Stream;
use loams_agentd_proto::{ContentSearchMatch, SearchWorkspaceContentParams};
use tokio::sync::mpsc;

/// At most this many matches.
pub const MAX_RESULTS: usize = 2_000;
/// Only a file's first MiB is searched.
pub const MAX_FILE_BYTES: u64 = 1024 * 1024;
/// The search stops after this long.
pub const SEARCH_DEADLINE: Duration = Duration::from_secs(10);
/// A query is at most this many characters.
pub const MAX_QUERY_CHARS: usize = 1_024;
/// At most this many globs, each at most 256 characters.
pub const MAX_GLOBS: usize = 32;
/// A preview is at most this many characters.
pub const MAX_PREVIEW_CHARS: usize = 400;
/// The compiled regular expression's size limit.
const REGEX_SIZE_LIMIT: usize = 1 << 20;

/// A content search that could not start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentSearchError(pub String);

impl std::fmt::Display for ContentSearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ContentSearchError {}

/// A validated search: the matcher, the path filter and the cap.
#[derive(Debug, Clone)]
pub struct ContentQuery {
    matcher: regex::Regex,
    globs: Vec<String>,
    limit: usize,
}

impl ContentQuery {
    /// Check and compile `params`' query and globs.
    ///
    /// # Errors
    ///
    /// An empty or too long query, an invalid regular expression, or too many
    /// or too long globs.
    pub fn new(params: &SearchWorkspaceContentParams) -> Result<Self, ContentSearchError> {
        let bad = |text: String| ContentSearchError(text);
        if params.query.is_empty() {
            return Err(bad("query must not be empty".into()));
        }
        if params.query.chars().count() > MAX_QUERY_CHARS {
            return Err(bad(format!(
                "query must not exceed {MAX_QUERY_CHARS} characters"
            )));
        }
        if params.globs.len() > MAX_GLOBS
            || params
                .globs
                .iter()
                .any(|glob| glob.is_empty() || glob.chars().count() > 256)
        {
            return Err(bad(format!(
                "at most {MAX_GLOBS} globs of 1 to 256 characters"
            )));
        }
        let pattern = if params.regex {
            params.query.clone()
        } else {
            regex::escape(&params.query)
        };
        let matcher = regex::RegexBuilder::new(&pattern)
            .case_insensitive(!params.case_sensitive)
            .size_limit(REGEX_SIZE_LIMIT)
            .dfa_size_limit(REGEX_SIZE_LIMIT)
            .build()
            .map_err(|error| {
                bad(format!(
                    "not a regular expression this search accepts: {error}"
                ))
            })?;
        let limit = match params.max_results {
            0 => MAX_RESULTS,
            n => usize::try_from(n).unwrap_or(MAX_RESULTS).min(MAX_RESULTS),
        };
        Ok(Self {
            matcher,
            globs: params.globs.clone(),
            limit,
        })
    }

    /// The walker, and the globs as a filter applied after it: an `ignore`
    /// override would re-include an ignored file, so the globs only ever
    /// narrow what the walker yields.
    fn walker(
        &self,
        root: &Path,
    ) -> Result<(ignore::Walk, Option<ignore::overrides::Override>), ContentSearchError> {
        let mut builder = ignore::WalkBuilder::new(root);
        builder
            .follow_links(false)
            .hidden(false)
            // `.gitignore` applies even where the root is not (yet) a git
            // repository's top level.
            .require_git(false)
            .filter_entry(|entry| entry.file_name() != ".git");
        let globs = if self.globs.is_empty() {
            None
        } else {
            let mut overrides = ignore::overrides::OverrideBuilder::new(root);
            for glob in &self.globs {
                overrides
                    .add(glob)
                    .map_err(|error| ContentSearchError(format!("not a glob: {error}")))?;
            }
            Some(
                overrides
                    .build()
                    .map_err(|error| ContentSearchError(format!("not a glob: {error}")))?,
            )
        };
        Ok((builder.build(), globs))
    }
}

/// `path` relative to `root`, `/`-separated, or `None` inside `.git` or for a
/// path that is not UTF-8.
fn wire_path(root: &Path, path: &Path) -> Option<String> {
    let relative = path.strip_prefix(root).ok()?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str()?;
                if part == ".git" {
                    return None;
                }
                parts.push(part);
            }
            _ => return None,
        }
    }
    (!parts.is_empty()).then(|| parts.join("/"))
}

/// The first [`MAX_FILE_BYTES`] of a regular file, or `None` for a binary
/// one (a NUL byte) or one that can't be read.
fn read_text(path: &Path) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let mut bytes = Vec::new();
    file.take(MAX_FILE_BYTES).read_to_end(&mut bytes).ok()?;
    if bytes.contains(&0) {
        return None;
    }
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

fn preview(line: &str) -> String {
    line.chars().take(MAX_PREVIEW_CHARS).collect()
}

/// Search `root` and send each match to `out`, until a cap, the deadline,
/// `cancel` or a closed channel. Blocking: run it off the runtime.
pub fn search_blocking(
    root: &Path,
    query: &ContentQuery,
    deadline: Instant,
    cancel: &AtomicBool,
    out: &mpsc::Sender<ContentSearchMatch>,
) -> Result<usize, ContentSearchError> {
    let mut sent = 0usize;
    let (walker, globs) = query.walker(root)?;
    for entry in walker {
        if cancel.load(Ordering::Relaxed) || Instant::now() >= deadline {
            break;
        }
        let Ok(entry) = entry else {
            continue;
        };
        if !entry.file_type().is_some_and(|kind| kind.is_file()) {
            continue;
        }
        if globs
            .as_ref()
            .is_some_and(|globs| globs.matched(entry.path(), false).is_ignore())
        {
            continue;
        }
        let Some(path) = wire_path(root, entry.path()) else {
            continue;
        };
        let Some(text) = read_text(entry.path()) else {
            continue;
        };
        for (index, line) in text.lines().enumerate() {
            for found in query.matcher.find_iter(line) {
                if found.as_str().is_empty() {
                    continue;
                }
                let column = line[..found.start()].chars().count() + 1;
                let item = ContentSearchMatch {
                    path: path.clone(),
                    line: u32::try_from(index + 1).unwrap_or(u32::MAX),
                    column: u32::try_from(column).unwrap_or(u32::MAX),
                    preview: preview(line),
                };
                if out.blocking_send(item).is_err() {
                    return Ok(sent);
                }
                sent += 1;
                if sent >= query.limit {
                    return Ok(sent);
                }
            }
            if Instant::now() >= deadline || cancel.load(Ordering::Relaxed) {
                return Ok(sent);
            }
        }
    }
    Ok(sent)
}

/// Sets the flag when dropped: the stream's end stops the walk.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// The matches of `query` under `root`, as a stream. The walk runs on the
/// blocking pool and stops when the stream is dropped.
pub fn search(
    root: std::path::PathBuf,
    query: ContentQuery,
) -> impl Stream<Item = ContentSearchMatch> + Send + 'static {
    let (tx, rx) = mpsc::channel(256);
    let cancel = Arc::new(AtomicBool::new(false));
    let stop = StopOnDrop(Arc::clone(&cancel));
    let deadline = Instant::now() + SEARCH_DEADLINE;
    tokio::task::spawn_blocking(move || {
        if let Err(error) = search_blocking(&root, &query, deadline, &cancel, &tx) {
            tracing::debug!(%error, "content search stopped");
        }
    });
    futures::stream::unfold((rx, stop), |(mut rx, stop)| async move {
        let item = rx.recv().await?;
        Some((item, (rx, stop)))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(query: &str) -> SearchWorkspaceContentParams {
        SearchWorkspaceContentParams {
            space_id: None,
            chat_id: None,
            query: query.into(),
            regex: false,
            case_sensitive: false,
            globs: Vec::new(),
            max_results: 0,
        }
    }

    #[test]
    fn queries_are_checked() {
        assert!(ContentQuery::new(&params("")).is_err());
        let mut regex = params("(unclosed");
        assert!(ContentQuery::new(&regex).is_ok(), "a literal is escaped");
        regex.regex = true;
        assert!(ContentQuery::new(&regex).is_err());
        let mut many = params("x");
        many.globs = vec!["*.rs".into(); MAX_GLOBS + 1];
        assert!(ContentQuery::new(&many).is_err());
        let mut capped = params("x");
        capped.max_results = 1_000_000;
        assert_eq!(ContentQuery::new(&capped).unwrap().limit, MAX_RESULTS);
    }

    #[test]
    fn wire_paths_skip_git() {
        let root = Path::new("/r");
        assert_eq!(
            wire_path(root, Path::new("/r/a/b.rs")).as_deref(),
            Some("a/b.rs")
        );
        assert_eq!(wire_path(root, Path::new("/r/.git/config")), None);
        assert_eq!(wire_path(root, Path::new("/r")), None);
    }
}
