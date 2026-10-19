//! `Grep` (§36 §19.8, D833; plan GT1 Task 21): `git grep` over a tree's
//! blobs on the server.
//!
//! Patterns are matched line by line with ripgrep's library crates
//! (`grep-regex`, `grep-searcher`; MIT OR Unlicense). git's patterns are
//! POSIX: basic regular expressions by default, extended ones with `-E`,
//! fixed strings with `-F`; they are translated into the regex crate's
//! syntax ([`translate_basic`], [`translate_extended`]), and what has no
//! translation (back-references) is refused, so the shim falls through to
//! git (ruling T21.3).
//!
//! Files are searched in parallel and answered in path order (git's tree
//! order, which is the order `git grep` prints), one [`GrepFile`] per file
//! with a match. A file with a NUL in its first 8,000 bytes is binary, as
//! git decides: it is answered with no lines, which git prints as `Binary
//! file <path> matches`. The search stops at [`GREP_DEADLINE`] or after
//! [`GREP_MAX_MATCHES`] matching lines (files, with `-l`), and the answer
//! says which.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use grep_matcher::Matcher as _;
use grep_regex::{RegexMatcher, RegexMatcherBuilder};
use grep_searcher::sinks::Bytes as BytesSink;
use grep_searcher::{BinaryDetection, SearcherBuilder};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::{HistoryError, Pathspec, TreeReader, join, resolve_tree};
use crate::ids::ObjectId;
use crate::limits::{GREP_DEADLINE, GREP_MAX_MATCHES, MAX_GREP_PATTERNS};
use crate::odb::{ObjectDb, OdbError, RangeOdb};
use crate::reflog::RefSnapshot;

/// Bytes git looks at for a NUL to call a file binary.
const BINARY_PROBE: usize = 8000;
/// Blobs read and searched at once.
const PARALLEL: usize = 16;

/// git grep's matching flags that `Grep` serves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GrepFlags {
    /// `-i`: case-insensitive.
    pub ignore_case: bool,
    /// `-w`: whole words only.
    pub word: bool,
    /// `-F`: patterns are fixed strings.
    pub fixed: bool,
    /// `-E`: patterns are extended regular expressions (else basic).
    pub extended: bool,
    /// `-l`: answer the files with a match, not their lines.
    pub files_only: bool,
}

/// What a `Grep` call asks for.
#[derive(Clone, Debug)]
pub struct GrepQuery {
    /// A commit, tag or tree, as `Log` names revisions; empty is `HEAD`.
    pub rev: String,
    /// Patterns (`-e`), any of which matches; at least one.
    pub patterns: Vec<String>,
    /// The paths searched (literal; empty: all).
    pub pathspec: Pathspec,
    /// The flags.
    pub flags: GrepFlags,
    /// The most matching lines (files, with `-l`) answered; at most
    /// [`GREP_MAX_MATCHES`].
    pub max_matches: usize,
    /// How long the search may take; at most [`GREP_DEADLINE`].
    pub deadline: Duration,
}

impl GrepQuery {
    /// A query for `patterns` at `rev` with the default limits.
    pub fn new(rev: impl Into<String>, patterns: Vec<String>) -> Self {
        Self {
            rev: rev.into(),
            patterns,
            pathspec: Pathspec::all(),
            flags: GrepFlags::default(),
            max_matches: GREP_MAX_MATCHES,
            deadline: GREP_DEADLINE,
        }
    }
}

/// One matching line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrepLine {
    /// Its number, 1-based.
    pub number: u64,
    /// The line, without its newline.
    pub line: Bytes,
}

/// One file with a match.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrepFile {
    /// Its path.
    pub path: Vec<u8>,
    /// Its blob.
    pub blob: ObjectId,
    /// Whether it is binary (then `lines` is empty).
    pub binary: bool,
    /// The matching lines, in order (empty with `-l`).
    pub lines: Vec<GrepLine>,
}

/// How a `Grep` call ended.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GrepStats {
    /// Files read and searched.
    pub searched: u64,
    /// Matching lines answered (files, with `-l`).
    pub matches: u64,
    /// The search stopped at the match limit.
    pub truncated: bool,
    /// The search stopped at the deadline.
    pub deadline_exceeded: bool,
}

fn refuse(pattern: &str, reason: impl Into<String>) -> HistoryError {
    HistoryError::Pattern {
        pattern: pattern.to_owned(),
        reason: reason.into(),
    }
}

/// Copies a bracket expression starting at `chars[i] == '['` into `out`,
/// escaped for the regex crate (which nests classes and has set
/// operators), and returns the index after its `]`.
fn bracket(
    pattern: &str,
    chars: &[char],
    mut i: usize,
    out: &mut String,
) -> Result<usize, HistoryError> {
    out.push('[');
    i += 1;
    if chars.get(i) == Some(&'^') {
        out.push('^');
        i += 1;
    }
    let mut first = true;
    loop {
        let Some(&c) = chars.get(i) else {
            return Err(refuse(pattern, "unterminated bracket expression"));
        };
        if c == ']' && !first {
            out.push(']');
            return Ok(i + 1);
        }
        first = false;
        if c == '[' && matches!(chars.get(i + 1), Some(':' | '.' | '=')) {
            let kind = chars[i + 1];
            let close = (i + 2..chars.len().saturating_sub(1))
                .find(|&j| chars[j] == kind && chars[j + 1] == ']')
                .ok_or_else(|| refuse(pattern, "unterminated character class"))?;
            if kind != ':' {
                return Err(refuse(pattern, "collating elements are not served"));
            }
            let name: String = chars[i + 2..close].iter().collect();
            out.push_str(&format!("[:{name}:]"));
            i = close + 2;
            continue;
        }
        match c {
            // Literal in POSIX brackets; special to the regex crate.
            '\\' | '[' | '&' | '~' | ']' => {
                out.push('\\');
                out.push(c);
            }
            '-' => {
                // A range keeps its `-`; a `-` before `]` or after `[` is literal.
                let literal =
                    chars.get(i + 1) == Some(&']') || out.ends_with('[') || out.ends_with("[^");
                if literal {
                    out.push_str("\\-");
                } else {
                    out.push('-');
                }
            }
            other => out.push(other),
        }
        i += 1;
    }
}

/// POSIX `.` as git's regex library matches it: any character but a
/// newline or NUL.
const ANY: &str = "[^\\x00\\n]";

/// Escapes `c` for the regex crate when it is special there.
fn literal(c: char, out: &mut String) {
    if regex_meta(c) {
        out.push('\\');
    }
    out.push(c);
}

fn regex_meta(c: char) -> bool {
    matches!(
        c,
        '\\' | '.'
            | '+'
            | '*'
            | '?'
            | '('
            | ')'
            | '|'
            | '['
            | ']'
            | '{'
            | '}'
            | '^'
            | '$'
            | '#'
            | '&'
            | '-'
            | '~'
    )
}

/// The GNU escapes both POSIX flavours share: `\<`, `\>`, `\b`, `\B`,
/// `\w`, `\W`, `\s`, `\S`, and an escaped character standing for itself.
fn common_escape(pattern: &str, c: char, out: &mut String) -> Result<(), HistoryError> {
    match c {
        '<' | '>' | 'b' => out.push_str("\\b"),
        'B' => out.push_str("\\B"),
        'w' | 'W' | 's' | 'S' => {
            out.push('\\');
            out.push(c);
        }
        '1'..='9' => return Err(refuse(pattern, "back-references are not served")),
        other if other.is_ascii_alphanumeric() => {
            return Err(refuse(
                pattern,
                format!("the escape \\{other} is not served"),
            ));
        }
        other => literal(other, out),
    }
    Ok(())
}

/// Translates a POSIX basic regular expression (git's default, with GNU's
/// `\+`, `\?` and `\|`) into the regex crate's syntax.
pub fn translate_basic(pattern: &str) -> Result<String, HistoryError> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len() + 8);
    // Where a `*` is literal and `^` is an anchor: at the start of the
    // expression, of a group or of an alternative.
    let mut at_start = true;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let was_start = at_start;
        at_start = false;
        match c {
            '\\' => {
                let Some(&next) = chars.get(i + 1) else {
                    return Err(refuse(pattern, "trailing backslash"));
                };
                i += 2;
                match next {
                    '(' => {
                        out.push('(');
                        at_start = true;
                    }
                    ')' => out.push(')'),
                    '|' => {
                        out.push('|');
                        at_start = true;
                    }
                    '{' => out.push('{'),
                    '}' => out.push('}'),
                    '+' | '?' => out.push(next),
                    other => common_escape(pattern, other, &mut out)?,
                }
                continue;
            }
            '^' if was_start => {
                out.push('^');
                at_start = true;
            }
            '$' => {
                let ends = i + 1 == chars.len()
                    || (chars[i + 1] == '\\' && matches!(chars.get(i + 2), Some(')' | '|')));
                if ends {
                    out.push('$');
                } else {
                    out.push_str("\\$");
                }
            }
            '*' if was_start => out.push_str("\\*"),
            '*' => out.push(c),
            '.' => out.push_str(ANY),
            '[' => {
                i = bracket(pattern, &chars, i, &mut out)?;
                continue;
            }
            other => literal(other, &mut out),
        }
        i += 1;
    }
    Ok(out)
}

/// Translates a POSIX extended regular expression (`-E`, with GNU's
/// escapes) into the regex crate's syntax.
pub fn translate_extended(pattern: &str) -> Result<String, HistoryError> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len() + 8);
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\\' => {
                let Some(&next) = chars.get(i + 1) else {
                    return Err(refuse(pattern, "trailing backslash"));
                };
                common_escape(pattern, next, &mut out)?;
                i += 2;
                continue;
            }
            '[' => {
                i = bracket(pattern, &chars, i, &mut out)?;
                continue;
            }
            '.' => out.push_str(ANY),
            '+' | '*' | '?' | '(' | ')' | '|' | '{' | '}' | '^' | '$' => out.push(c),
            other => literal(other, &mut out),
        }
        i += 1;
    }
    Ok(out)
}

/// The matcher for `patterns` under `flags`.
pub fn matcher(patterns: &[String], flags: GrepFlags) -> Result<RegexMatcher, HistoryError> {
    let Some(first) = patterns.first() else {
        return Err(refuse("", "no pattern"));
    };
    if patterns.len() > MAX_GREP_PATTERNS {
        return Err(HistoryError::TooMany {
            what: "patterns",
            max: MAX_GREP_PATTERNS,
            got: patterns.len(),
        });
    }
    let mut builder = RegexMatcherBuilder::new();
    builder
        .case_insensitive(flags.ignore_case)
        .word(flags.word)
        .line_terminator(Some(b'\n'));
    let translated = patterns
        .iter()
        .map(|pattern| {
            if flags.fixed {
                let mut out = String::with_capacity(pattern.len());
                pattern.chars().for_each(|c| literal(c, &mut out));
                Ok(out)
            } else if flags.extended {
                translate_extended(pattern)
            } else {
                translate_basic(pattern)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let built = builder
        .build_many(&translated)
        .map_err(|err| refuse(first, err.to_string()))?;
    // git answers a pattern that matches an empty line with a phantom line
    // after a file's final newline for some patterns (`^$`) and not others
    // (`x*`); such patterns are not served (ruling T21.3).
    if built.is_match(b"").unwrap_or(true) {
        return Err(refuse(
            first,
            "patterns that match an empty line are not served",
        ));
    }
    Ok(built)
}

/// Whether `data` is binary as git decides it for grep.
fn is_binary(data: &[u8]) -> bool {
    data[..data.len().min(BINARY_PROBE)].contains(&0)
}

/// The matches of one blob; `None` when there is none. `max` caps the
/// lines kept.
fn search(
    matcher: &RegexMatcher,
    data: &[u8],
    files_only: bool,
    max: usize,
) -> Result<Option<(bool, Vec<GrepLine>)>, std::io::Error> {
    let binary = is_binary(data);
    let mut searcher = SearcherBuilder::new()
        .line_number(true)
        .binary_detection(BinaryDetection::none())
        .bom_sniffing(false)
        .build();
    let mut lines = Vec::new();
    let mut found = false;
    let keep = !binary && !files_only;
    searcher.search_slice(
        matcher,
        data,
        BytesSink(|number, line| {
            found = true;
            if !keep {
                return Ok(false);
            }
            let line = line.strip_suffix(b"\n").unwrap_or(line);
            lines.push(GrepLine {
                number,
                line: Bytes::copy_from_slice(line),
            });
            Ok(lines.len() < max)
        }),
    )?;
    Ok(found.then_some((binary, lines)))
}

/// The regular files below `tree` that `spec` matches, in tree order
/// (`git grep` skips symlinks and submodules).
fn files<'r, 'a: 'r>(
    reader: &'r TreeReader<'a>,
    prefix: Vec<u8>,
    tree: ObjectId,
    spec: &'r Pathspec,
    out: &'r mut Vec<(Vec<u8>, ObjectId)>,
) -> BoxFuture<'r, Result<(), OdbError>> {
    Box::pin(async move {
        let entries = reader.tree(&tree).await?;
        for entry in entries.iter() {
            let path = join(&prefix, &entry.name);
            if entry.is_tree() {
                if spec.may_contain(&path) {
                    files(reader, path, entry.oid, spec, out).await?;
                }
            } else if matches!(entry.mode, 0o100644 | 0o100755) && spec.matches(&path) {
                out.push((path, entry.oid));
            }
        }
        Ok(())
    })
}

/// Runs `query` over `snapshot` and sends each file with a match to `out`,
/// in path order, stopping early when `out` is closed.
pub async fn grep(
    odb: &RangeOdb,
    snapshot: &RefSnapshot,
    query: &GrepQuery,
    out: &mpsc::Sender<GrepFile>,
) -> Result<GrepStats, HistoryError> {
    let started = Instant::now();
    let deadline = started + query.deadline.min(GREP_DEADLINE);
    let max = query.max_matches.min(GREP_MAX_MATCHES);
    let matcher = Arc::new(matcher(&query.patterns, query.flags)?);
    let rev = if query.rev.is_empty() {
        "HEAD"
    } else {
        query.rev.as_str()
    };
    let tree = resolve_tree(odb, snapshot, rev).await?;
    let reader = TreeReader::new(odb);
    let mut listed = Vec::new();
    files(&reader, Vec::new(), tree, &query.pathspec, &mut listed).await?;
    let files_only = query.flags.files_only;
    let mut results = futures::stream::iter(listed)
        .map(|(path, blob)| {
            let odb = odb.clone();
            let matcher = Arc::clone(&matcher);
            async move {
                let (_, data) = odb.read(&blob).await?;
                let found =
                    tokio::task::spawn_blocking(move || search(&matcher, &data, files_only, max))
                        .await
                        .map_err(|err| HistoryError::Pattern {
                            pattern: String::new(),
                            reason: format!("the search failed: {err}"),
                        })?
                        .map_err(|err| HistoryError::Pattern {
                            pattern: String::new(),
                            reason: format!("the search failed: {err}"),
                        })?;
                Ok::<_, HistoryError>((path, blob, found))
            }
        })
        .buffered(PARALLEL);
    let mut stats = GrepStats::default();
    let mut answered = 0usize;
    loop {
        let next = match tokio::time::timeout_at(deadline, results.next()).await {
            Ok(next) => next,
            Err(_) => {
                stats.deadline_exceeded = true;
                break;
            }
        };
        let Some(result) = next else {
            break;
        };
        let (path, blob, found) = result?;
        stats.searched += 1;
        let Some((binary, mut lines)) = found else {
            continue;
        };
        if answered >= max {
            stats.truncated = true;
            break;
        }
        let counted = if files_only || binary { 1 } else { lines.len() };
        if answered + counted > max {
            lines.truncate(max - answered);
            stats.truncated = true;
        }
        answered += counted.min(max - answered);
        let file = GrepFile {
            path,
            blob,
            binary,
            lines,
        };
        if out.send(file).await.is_err() || stats.truncated {
            break;
        }
    }
    stats.matches = answered as u64;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_expressions_translate() {
        assert_eq!(translate_basic("a+b").unwrap(), "a\\+b");
        assert_eq!(translate_basic("a\\+b").unwrap(), "a+b");
        assert_eq!(translate_basic("\\(ab\\)*").unwrap(), "(ab)*");
        assert_eq!(translate_basic("*a").unwrap(), "\\*a");
        assert_eq!(translate_basic("^a$").unwrap(), "^a$");
        assert_eq!(translate_basic("a^b$c").unwrap(), "a\\^b\\$c");
        assert_eq!(translate_basic("a\\|b").unwrap(), "a|b");
        assert_eq!(translate_basic("x\\{2,3\\}").unwrap(), "x{2,3}");
        assert_eq!(translate_basic("[]a-c[]").unwrap(), "[\\]a-c\\[]");
        assert_eq!(translate_basic("[[:alpha:]-]").unwrap(), "[[:alpha:]\\-]");
        assert!(translate_basic("\\(a\\)\\1").is_err());
    }

    #[test]
    fn extended_expressions_translate() {
        assert_eq!(translate_extended("(a|b)+c?").unwrap(), "(a|b)+c?");
        assert_eq!(translate_extended("a\\.b").unwrap(), "a\\.b");
        assert_eq!(translate_extended("\\<w\\>").unwrap(), "\\bw\\b");
        assert_eq!(translate_extended("a&&b").unwrap(), "a\\&\\&b");
        assert!(translate_extended("(a)\\1").is_err());
    }
}
