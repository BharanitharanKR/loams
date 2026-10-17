//! One file's patch as `git diff --no-renames --no-indent-heuristic`
//! prints it (plan GT1 Task 26, ruling T26.2).
//!
//! Written from `git-diff(1)` ("Generating patch text with -p") and
//! `gitattributes(5)` ("Defining a custom hunk-header": the default hunk
//! header is the nearest line above the hunk that begins with a letter, an
//! underscore or a dollar sign): the `diff --git` line, the extended
//! header (`new file mode`, `deleted file mode`, `old mode`/`new mode`,
//! `index <old>..<new>[ <mode>]` with 7-digit abbreviations), the `---`
//! and `+++` lines, then hunks with 3 lines of context, hunks whose
//! contexts touch merged, `\ No newline at end of file` after a last line
//! without one. A file with a NUL in its first 8,000 bytes prints `Binary
//! files … differ`. A path with a control, quote, backslash or non-ASCII
//! byte is quoted C-style, as `core.quotePath` does by default. A change
//! of type (a file to a symlink, say) prints as a deletion then an
//! addition. A symlink diffs as its target and a submodule as the line
//! `Subproject commit <oid>`.

use crate::ids::ObjectId;

use super::myers::{Hunk, diff_lines, split_lines};

/// Bytes git looks at for a NUL to call a file binary.
const BINARY_PROBE: usize = 8000;
/// The hex digits of an abbreviated object id in the `index` line.
const ABBREV: usize = 7;
/// The most bytes of a hunk header's function line.
const FUNC_LINE_MAX: usize = 80;
/// git's mode of a submodule.
const GITLINK: u32 = 0o160000;

/// One side of a file diff.
#[derive(Clone, Copy, Debug)]
pub struct DiffSide<'a> {
    /// git's mode bits.
    pub mode: u32,
    /// The object.
    pub oid: ObjectId,
    /// Its contents (a symlink's target; ignored for a submodule).
    pub data: &'a [u8],
}

/// Appends the patch of `path` from `old` to `new` (`None`: absent on
/// that side) with `context` lines of context to `out`. Nothing is
/// written when both sides are equal.
pub fn write_file_patch(
    path: &[u8],
    old: Option<DiffSide<'_>>,
    new: Option<DiffSide<'_>>,
    context: usize,
    out: &mut Vec<u8>,
) {
    if let (Some(a), Some(b)) = (old, new) {
        if a.mode == b.mode && a.oid == b.oid {
            return;
        }
        if file_type(a.mode) != file_type(b.mode) {
            write_file_patch(path, Some(a), None, context, out);
            write_file_patch(path, None, Some(b), context, out);
            return;
        }
    }
    if old.is_none() && new.is_none() {
        return;
    }
    let a_name = quoted(b"a/", path);
    let b_name = quoted(b"b/", path);
    out.extend_from_slice(b"diff --git ");
    out.extend_from_slice(&a_name);
    out.push(b' ');
    out.extend_from_slice(&b_name);
    out.push(b'\n');
    let null = ObjectId::null(gix_hash::Kind::Sha1);
    let (old_oid, new_oid) = (old.map_or(null, |s| s.oid), new.map_or(null, |s| s.oid));
    match (old, new) {
        (None, Some(b)) => {
            out.extend_from_slice(format!("new file mode {:06o}\n", b.mode).as_bytes());
            index_line(old_oid, new_oid, None, out);
        }
        (Some(a), None) => {
            out.extend_from_slice(format!("deleted file mode {:06o}\n", a.mode).as_bytes());
            index_line(old_oid, new_oid, None, out);
        }
        (Some(a), Some(b)) => {
            if a.mode != b.mode {
                out.extend_from_slice(format!("old mode {:06o}\n", a.mode).as_bytes());
                out.extend_from_slice(format!("new mode {:06o}\n", b.mode).as_bytes());
            }
            if a.oid == b.oid {
                return;
            }
            let mode = (a.mode == b.mode).then_some(a.mode);
            index_line(old_oid, new_oid, mode, out);
        }
        (None, None) => unreachable!("one side is present"),
    }
    let content = |side: Option<DiffSide<'_>>| -> Vec<u8> {
        match side {
            None => Vec::new(),
            Some(s) if s.mode == GITLINK => format!("Subproject commit {}\n", s.oid).into_bytes(),
            Some(s) => s.data.to_vec(),
        }
    };
    let (before, after) = (content(old), content(new));
    let minus: Vec<u8> = if old.is_some() {
        a_name
    } else {
        b"/dev/null".to_vec()
    };
    let plus: Vec<u8> = if new.is_some() {
        b_name
    } else {
        b"/dev/null".to_vec()
    };
    if is_binary(&before) || is_binary(&after) {
        out.extend_from_slice(b"Binary files ");
        out.extend_from_slice(&minus);
        out.extend_from_slice(b" and ");
        out.extend_from_slice(&plus);
        out.extend_from_slice(b" differ\n");
        return;
    }
    let (a, b) = (split_lines(&before), split_lines(&after));
    let hunks = diff_lines(&a, &b);
    if hunks.is_empty() {
        return;
    }
    out.extend_from_slice(b"--- ");
    out.extend_from_slice(&minus);
    out.extend_from_slice(b"\n+++ ");
    out.extend_from_slice(&plus);
    out.push(b'\n');
    write_hunks(&a, &b, &hunks, context, out);
}

/// Appends the hunks of `changes` between lines `a` and `b`, with
/// `context` lines of context, to `out`.
pub fn write_hunks(a: &[&[u8]], b: &[&[u8]], changes: &[Hunk], context: usize, out: &mut Vec<u8>) {
    let mut i = 0;
    while i < changes.len() {
        // Changes whose contexts touch print as one hunk.
        let mut j = i;
        while j + 1 < changes.len() && changes[j + 1].a.start - changes[j].a.end <= 2 * context {
            j += 1;
        }
        let first = &changes[i];
        let last = &changes[j];
        let a_start = first.a.start.saturating_sub(context);
        let b_start = first.b.start - (first.a.start - a_start);
        let a_end = (last.a.end + context).min(a.len());
        let b_end = last.b.end + (a_end - last.a.end);
        out.extend_from_slice(b"@@ -");
        range(a_start, a_end - a_start, out);
        out.extend_from_slice(b" +");
        range(b_start, b_end - b_start, out);
        out.extend_from_slice(b" @@");
        if let Some(func) = func_line(a, a_start) {
            out.push(b' ');
            out.extend_from_slice(func);
        }
        out.push(b'\n');
        let mut at = a_start;
        for change in &changes[i..=j] {
            for line in &a[at..change.a.start] {
                emit(b' ', line, out);
            }
            for line in &a[change.a.clone()] {
                emit(b'-', line, out);
            }
            for line in &b[change.b.clone()] {
                emit(b'+', line, out);
            }
            at = change.a.end;
        }
        for line in &a[at..a_end] {
            emit(b' ', line, out);
        }
        i = j + 1;
    }
}

/// `start,len` of a hunk header (1-based; `len` left out when 1; an empty
/// range names the line before it).
fn range(start: usize, len: usize, out: &mut Vec<u8>) {
    let shown = if len == 0 { start } else { start + 1 };
    out.extend_from_slice(shown.to_string().as_bytes());
    if len != 1 {
        out.extend_from_slice(format!(",{len}").as_bytes());
    }
}

/// The nearest line above line `at` of the old file that begins with a
/// letter, `_` or `$`, without its newline and trailing blanks.
fn func_line<'a>(a: &[&'a [u8]], at: usize) -> Option<&'a [u8]> {
    a[..at].iter().rev().find_map(|line| {
        let first = *line.first()?;
        if !(first.is_ascii_alphabetic() || first == b'_' || first == b'$') {
            return None;
        }
        let line = &line[..line.len().min(FUNC_LINE_MAX)];
        let end = line
            .iter()
            .rposition(|b| !b.is_ascii_whitespace())
            .map_or(0, |p| p + 1);
        Some(&line[..end])
    })
}

fn emit(sign: u8, line: &[u8], out: &mut Vec<u8>) {
    out.push(sign);
    out.extend_from_slice(line);
    if line.last() != Some(&b'\n') {
        out.extend_from_slice(b"\n\\ No newline at end of file\n");
    }
}

fn index_line(old: ObjectId, new: ObjectId, mode: Option<u32>, out: &mut Vec<u8>) {
    out.extend_from_slice(b"index ");
    out.extend_from_slice(old.to_hex_with_len(ABBREV).to_string().as_bytes());
    out.extend_from_slice(b"..");
    out.extend_from_slice(new.to_hex_with_len(ABBREV).to_string().as_bytes());
    if let Some(mode) = mode {
        out.extend_from_slice(format!(" {mode:06o}").as_bytes());
    }
    out.push(b'\n');
}

/// The file type bits of a mode (a regular file, executable or not, is
/// one type).
fn file_type(mode: u32) -> u32 {
    mode & 0o170000
}

fn is_binary(data: &[u8]) -> bool {
    data[..data.len().min(BINARY_PROBE)].contains(&0)
}

/// `prefix` + `path`, in double quotes with C escapes when the path has a
/// byte `core.quotePath` quotes.
fn quoted(prefix: &[u8], path: &[u8]) -> Vec<u8> {
    let needs = path
        .iter()
        .any(|b| *b < 0x20 || *b >= 0x7f || *b == b'"' || *b == b'\\');
    let mut out = Vec::with_capacity(path.len() + prefix.len() + 2);
    if !needs {
        out.extend_from_slice(prefix);
        out.extend_from_slice(path);
        return out;
    }
    out.push(b'"');
    out.extend_from_slice(prefix);
    for b in path {
        match *b {
            0x07 => out.extend_from_slice(b"\\a"),
            0x08 => out.extend_from_slice(b"\\b"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b'\n' => out.extend_from_slice(b"\\n"),
            0x0b => out.extend_from_slice(b"\\v"),
            0x0c => out.extend_from_slice(b"\\f"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b if !(0x20..0x7f).contains(&b) => {
                out.extend_from_slice(format!("\\{b:03o}").as_bytes())
            }
            b => out.push(b),
        }
    }
    out.push(b'"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting() {
        assert_eq!(quoted(b"a/", b"x y"), b"a/x y");
        assert_eq!(quoted(b"a/", "é".as_bytes()), b"\"a/\\303\\251\"");
        assert_eq!(quoted(b"b/", b"q\"t"), b"\"b/q\\\"t\"");
    }

    #[test]
    fn ranges() {
        let mut out = Vec::new();
        range(0, 0, &mut out);
        out.push(b' ');
        range(0, 1, &mut out);
        out.push(b' ');
        range(4, 3, &mut out);
        assert_eq!(out, b"0,0 1 5,3");
    }
}
