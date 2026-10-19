//! `merge3`: a clean-room three-way merge of file contents (Ruling 36;
//! design §36 §19.7.5; plan GT1 Task 26).
//!
//! Written from S. Khanna, K. Kunal and B. C. Pierce, "A Formal
//! Investigation of Diff3" (FSTTCS 2007), and from the output `git
//! merge-file --diff3` documents (`git-merge-file(1)`). Both sides are
//! diffed against the base with [`crate::diff::myers`]. A base line is
//! *stable* when neither diff changes it; maximal runs of stable lines that
//! are consecutive in all three files split the files into chunks. Between
//! two stable runs lies an unstable chunk: if only one side differs from
//! the base there it takes that side, if both made the same change it
//! takes it once, and otherwise it is a conflict, written with diff3-style
//! markers (`<<<<<<< ours`, `||||||| base`, `=======`, `>>>>>>> theirs`).
//! Changes that touch with no stable line between them are one chunk, so
//! adjacent edits conflict, as in diff3 and git.

use crate::diff::myers::{Hunk, diff_lines, split_lines};

/// The width of a conflict marker, as git's default `conflict-marker-size`.
pub const MARKER_SIZE: usize = 7;

/// The labels written after the conflict markers.
#[derive(Clone, Copy, Debug)]
pub struct MergeLabels<'a> {
    /// After `<<<<<<<`.
    pub ours: &'a str,
    /// After `|||||||`.
    pub base: &'a str,
    /// After `>>>>>>>`.
    pub theirs: &'a str,
}

/// The result of [`merge3`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Merge3 {
    /// The merged contents, with conflict markers where the sides
    /// conflict.
    pub content: Vec<u8>,
    /// The number of conflicts (what `git merge-file` exits with, below
    /// 128).
    pub conflicts: usize,
}

impl Merge3 {
    /// Whether it merged without a conflict.
    pub fn is_clean(&self) -> bool {
        self.conflicts == 0
    }
}

/// Merges the changes from `base` to `ours` and from `base` to `theirs`.
pub fn merge3(base: &[u8], ours: &[u8], theirs: &[u8], labels: MergeLabels<'_>) -> Merge3 {
    let base = split_lines(base);
    let ours = split_lines(ours);
    let theirs = split_lines(theirs);
    let to_ours = matches(base.len(), ours.len(), &diff_lines(&base, &ours));
    let to_theirs = matches(base.len(), theirs.len(), &diff_lines(&base, &theirs));
    let mut out = Merge3 {
        content: Vec::new(),
        conflicts: 0,
    };
    // Positions just past the last stable run written, in each file.
    let (mut b, mut o, mut t) = (0, 0, 0);
    let mut i = 0;
    while i <= base.len() {
        // The next stable line, or the end of all three files.
        let next = (i..base.len()).find(|&i| to_ours[i].is_some() && to_theirs[i].is_some());
        let (sb, so, st) = match next {
            Some(i) => (
                i,
                to_ours[i].expect("stable"),
                to_theirs[i].expect("stable"),
            ),
            None => (base.len(), ours.len(), theirs.len()),
        };
        // An unstable chunk lies before it unless it continues the run.
        if (sb, so, st) != (b, o, t) {
            chunk(&base[b..sb], &ours[o..so], &theirs[t..st], labels, &mut out);
        }
        let Some(start) = next else {
            break;
        };
        // The stable run: consecutive in all three files.
        let mut end = start;
        while end < base.len()
            && to_ours[end] == Some(so + (end - start))
            && to_theirs[end] == Some(st + (end - start))
        {
            end += 1;
        }
        for line in &base[start..end] {
            out.content.extend_from_slice(line);
        }
        (b, o, t) = (end, so + (end - start), st + (end - start));
        i = end;
    }
    out
}

/// For each base line, the line of the other file it matches, if any.
fn matches(base_len: usize, other_len: usize, hunks: &[Hunk]) -> Vec<Option<usize>> {
    let mut out = vec![None; base_len];
    let (mut i, mut j) = (0, 0);
    for hunk in hunks {
        while i < hunk.a.start {
            out[i] = Some(j);
            i += 1;
            j += 1;
        }
        i = hunk.a.end;
        j = hunk.b.end;
    }
    while i < base_len {
        out[i] = Some(j);
        i += 1;
        j += 1;
    }
    debug_assert_eq!(j.max(other_len), other_len);
    out
}

/// Writes one unstable chunk.
fn chunk(
    base: &[&[u8]],
    ours: &[&[u8]],
    theirs: &[&[u8]],
    labels: MergeLabels<'_>,
    out: &mut Merge3,
) {
    let take = |lines: &[&[u8]], content: &mut Vec<u8>| {
        for line in lines {
            content.extend_from_slice(line);
        }
    };
    if ours == base || ours == theirs {
        take(theirs, &mut out.content);
        return;
    }
    if theirs == base {
        take(ours, &mut out.content);
        return;
    }
    out.conflicts += 1;
    let content = &mut out.content;
    marker(b'<', labels.ours, content);
    section(ours, content);
    marker(b'|', labels.base, content);
    section(base, content);
    marker(b'=', "", content);
    section(theirs, content);
    marker(b'>', labels.theirs, content);
}

/// A conflict section's lines; a last line without a newline gets one, so
/// the next marker starts a line.
fn section(lines: &[&[u8]], content: &mut Vec<u8>) {
    for line in lines {
        content.extend_from_slice(line);
    }
    if lines.last().is_some_and(|line| line.last() != Some(&b'\n')) {
        content.push(b'\n');
    }
}

fn marker(byte: u8, label: &str, content: &mut Vec<u8>) {
    content.extend(std::iter::repeat_n(byte, MARKER_SIZE));
    if !label.is_empty() {
        content.push(b' ');
        content.extend_from_slice(label.as_bytes());
    }
    content.push(b'\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    const LABELS: MergeLabels<'static> = MergeLabels {
        ours: "ours",
        base: "base",
        theirs: "theirs",
    };

    fn m(base: &str, ours: &str, theirs: &str) -> (String, usize) {
        let out = merge3(base.as_bytes(), ours.as_bytes(), theirs.as_bytes(), LABELS);
        (
            String::from_utf8(out.content).expect("utf-8"),
            out.conflicts,
        )
    }

    #[test]
    fn disjoint_changes_merge() {
        assert_eq!(
            m("a\nb\nc\nd\ne\n", "A\nb\nc\nd\ne\n", "a\nb\nc\nd\nE\n"),
            ("A\nb\nc\nd\nE\n".into(), 0)
        );
    }

    #[test]
    fn same_change_is_clean() {
        assert_eq!(
            m("a\nb\nc\n", "a\nX\nc\n", "a\nX\nc\n"),
            ("a\nX\nc\n".into(), 0)
        );
    }

    #[test]
    fn overlapping_changes_conflict() {
        assert_eq!(
            m("a\nb\nc\n", "a\nO\nc\n", "a\nT\nc\n"),
            (
                "a\n<<<<<<< ours\nO\n||||||| base\nb\n=======\nT\n>>>>>>> theirs\nc\n".into(),
                1
            )
        );
    }
}
