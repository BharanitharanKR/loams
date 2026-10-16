//! `DiffStat` (§36 §19.8, D833; plan GT1 Task 20): per-file added and
//! removed line counts between two trees, as `git diff --numstat
//! --no-renames` prints them.
//!
//! Line counts are those of a shortest edit script (Myers' O(ND)
//! algorithm, implemented from the 1986 paper), which is what git's diff
//! finds for every input it diffs minimally. Lines that only one side has
//! and the common prefix and suffix are set aside first, as they cannot be
//! matched. A file with a NUL byte in its first 8,000 bytes is binary and
//! has no counts (`-`). A symlink is diffed as its target, and a submodule
//! as the line `Subproject commit <oid>`. Renames are not detected
//! (ruling T20.5).

use std::collections::HashMap;

use bytes::Bytes;

use super::log::{ChangeKind, ChangedPath};
use super::{HistoryError, Pathspec, TreeReader, diff_trees};
use crate::ids::ObjectId;
use crate::odb::{ObjectDb, RangeOdb};

/// Bytes git looks at for a NUL to call a file binary.
const BINARY_PROBE: usize = 8000;
/// The most work (`(n + m) · d` steps) one file's diff may take; past it
/// the counts are an upper bound (every unmatched line counted).
const MAX_DIFF_WORK: u64 = 200_000_000;

/// One file of a `DiffStat` answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileStat {
    /// The path, its change and its objects.
    pub change: ChangedPath,
    /// Lines added, `None` for a binary file.
    pub added: Option<u64>,
    /// Lines removed, `None` for a binary file.
    pub removed: Option<u64>,
}

/// The per-file line counts between trees `old` and `new` (`None` is the
/// empty tree), restricted to `spec`, in git's order.
pub async fn diff_stat(
    odb: &RangeOdb,
    old: Option<ObjectId>,
    new: Option<ObjectId>,
    spec: &Pathspec,
) -> Result<Vec<FileStat>, HistoryError> {
    let reader = TreeReader::new(odb);
    let changes = diff_trees(&reader, old, new, spec, None).await?;
    let mut out = Vec::with_capacity(changes.len());
    for change in changes {
        let before = content(odb, change.old).await?;
        let after = content(odb, change.new).await?;
        let (added, removed) = if is_binary(&before) || is_binary(&after) {
            (None, None)
        } else {
            let (added, removed) = count_lines(&before, &after);
            (Some(added), Some(removed))
        };
        out.push(FileStat {
            change: change.into(),
            added,
            removed,
        });
    }
    Ok(out)
}

impl FileStat {
    /// Whether the file was added, deleted or changed.
    pub fn kind(&self) -> ChangeKind {
        self.change.kind
    }
}

async fn content(odb: &RangeOdb, side: Option<(u32, ObjectId)>) -> Result<Bytes, HistoryError> {
    match side {
        None => Ok(Bytes::new()),
        Some((0o160000, oid)) => Ok(Bytes::from(format!("Subproject commit {oid}\n"))),
        Some((_, oid)) => Ok(odb.read(&oid).await?.1),
    }
}

fn is_binary(data: &[u8]) -> bool {
    data[..data.len().min(BINARY_PROBE)].contains(&0)
}

/// The lines of `data`, each with its newline (the last may have none).
fn lines(data: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, byte) in data.iter().enumerate() {
        if *byte == b'\n' {
            out.push(&data[start..=i]);
            start = i + 1;
        }
    }
    if start < data.len() {
        out.push(&data[start..]);
    }
    out
}

/// `(added, removed)` lines from `before` to `after`.
pub fn count_lines(before: &[u8], after: &[u8]) -> (u64, u64) {
    let (a, b) = (lines(before), lines(after));
    // Intern lines so the diff compares integers.
    let mut ids: HashMap<&[u8], u32> = HashMap::new();
    let a: Vec<u32> = a.into_iter().map(|line| intern(&mut ids, line)).collect();
    let b: Vec<u32> = b.into_iter().map(|line| intern(&mut ids, line)).collect();
    let common = lcs_len(&a, &b);
    ((b.len() - common) as u64, (a.len() - common) as u64)
}

fn intern<'d>(ids: &mut HashMap<&'d [u8], u32>, line: &'d [u8]) -> u32 {
    let next = ids.len() as u32;
    *ids.entry(line).or_insert(next)
}

/// The length of a longest common subsequence of `a` and `b`.
fn lcs_len(a: &[u32], b: &[u32]) -> usize {
    let prefix = a.iter().zip(b).take_while(|(x, y)| x == y).count();
    let (a, b) = (&a[prefix..], &b[prefix..]);
    let suffix = a
        .iter()
        .rev()
        .zip(b.iter().rev())
        .take_while(|(x, y)| x == y)
        .count();
    let (a, b) = (&a[..a.len() - suffix], &b[..b.len() - suffix]);
    // Lines only one side has cannot be matched.
    let in_b: std::collections::HashSet<u32> = b.iter().copied().collect();
    let in_a: std::collections::HashSet<u32> = a.iter().copied().collect();
    let a: Vec<u32> = a.iter().copied().filter(|x| in_b.contains(x)).collect();
    let b: Vec<u32> = b.iter().copied().filter(|x| in_a.contains(x)).collect();
    prefix + suffix + myers_lcs(&a, &b)
}

/// Myers' greedy forward search for the shortest edit script of `a` to
/// `b` (insertions and deletions); answers `(n + m − d) / 2`.
fn myers_lcs(a: &[u32], b: &[u32]) -> usize {
    let (n, m) = (a.len() as i64, b.len() as i64);
    if n == 0 || m == 0 {
        return 0;
    }
    let max = (n + m) as usize;
    let offset = max as i64;
    let mut v = vec![0i64; 2 * max + 2];
    for d in 0..=(n + m) {
        if ((n + m) as u64).saturating_mul(d as u64) > MAX_DIFF_WORK {
            // Too costly: count every remaining line as changed.
            return 0;
        }
        let mut k = -d;
        while k <= d {
            let idx = (k + offset) as usize;
            let mut x = if k == -d || (k != d && v[idx - 1] < v[idx + 1]) {
                v[idx + 1]
            } else {
                v[idx - 1] + 1
            };
            let mut y = x - k;
            while x < n && y < m && a[x as usize] == b[y as usize] {
                x += 1;
                y += 1;
            }
            v[idx] = x;
            if x >= n && y >= m {
                return ((n + m - d) / 2) as usize;
            }
            k += 2;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_match_a_shortest_edit_script() {
        assert_eq!(count_lines(b"", b""), (0, 0));
        assert_eq!(count_lines(b"", b"a\nb\n"), (2, 0));
        assert_eq!(count_lines(b"a\nb\n", b""), (0, 2));
        assert_eq!(count_lines(b"a\nb\nc\n", b"a\nx\nc\n"), (1, 1));
        // A missing final newline makes the last line differ.
        assert_eq!(count_lines(b"a\nb", b"a\nb\n"), (1, 1));
        assert_eq!(count_lines(b"a\nb\nc\nd\n", b"b\nc\nd\ne\n"), (1, 1));
        assert_eq!(count_lines(b"x\ny\nx\ny\n", b"y\nx\ny\nx\n"), (1, 1));
    }
}
