//! A line diff: Myers' O(ND) shortest edit script, then git's placement of
//! change groups (Ruling 36; plan GT1 Task 26).
//!
//! Written from E. W. Myers, "An O(ND) Difference Algorithm and Its
//! Variations", Algorithmica 1 (1986), §4b (the linear-space bidirectional
//! search for the middle snake), and from the behaviour `git diff`
//! documents and shows: a run of changed lines that can slide over equal
//! lines is placed as low as it can go, unless a position along its slide
//! sits opposite a change in the other file, in which case the lowest such
//! position is used, so a deletion and an insertion read as one
//! replacement. The indent heuristic (`diff.indentHeuristic`, git's
//! default since 2.14) is not applied: the output is what
//! `git diff --no-indent-heuristic` prints (ruling T26.2).
//!
//! Lines compare as bytes, newline included. Before the search, the
//! common prefix and suffix are set aside, and lines that occur only in
//! one file are marked changed (they cannot be matched).

use std::collections::HashMap;
use std::ops::Range;

/// One changed region: lines `a` of the old file were replaced by lines
/// `b` of the new one (either may be empty, not both).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hunk {
    /// The old file's lines.
    pub a: Range<usize>,
    /// The new file's lines.
    pub b: Range<usize>,
}

/// The lines of `data`, each with its newline (the last may have none).
pub fn split_lines(data: &[u8]) -> Vec<&[u8]> {
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

/// The changed regions from lines `a` to lines `b`, in order.
pub fn diff_lines(a: &[&[u8]], b: &[&[u8]]) -> Vec<Hunk> {
    let mut ids: HashMap<&[u8], u32> = HashMap::new();
    let mut a_ids = Vec::with_capacity(a.len());
    let mut b_ids = Vec::with_capacity(b.len());
    for (lines, out) in [(a, &mut a_ids), (b, &mut b_ids)] {
        for line in lines {
            let next = ids.len() as u32;
            out.push(*ids.entry(line).or_insert(next));
        }
    }
    diff_ids(&a_ids, &b_ids)
}

/// [`diff_lines`] over interned lines.
pub fn diff_ids(a: &[u32], b: &[u32]) -> Vec<Hunk> {
    let mut changed_a = vec![false; a.len()];
    let mut changed_b = vec![false; b.len()];
    mark_changes(a, b, &mut changed_a, &mut changed_b);
    compact(a, &mut changed_a, &changed_b);
    compact(b, &mut changed_b, &changed_a);
    hunks(&changed_a, &changed_b)
}

/// Marks the lines a shortest edit script deletes from `a` and inserts
/// from `b`.
fn mark_changes(a: &[u32], b: &[u32], changed_a: &mut [bool], changed_b: &mut [bool]) {
    let prefix = a.iter().zip(b).take_while(|(x, y)| x == y).count();
    let suffix = a[prefix..]
        .iter()
        .rev()
        .zip(b[prefix..].iter().rev())
        .take_while(|(x, y)| x == y)
        .count();
    let (a_mid, b_mid) = (prefix..a.len() - suffix, prefix..b.len() - suffix);
    // Lines only one side has are changes; the rest go to the search.
    let mut count_a: HashMap<u32, u32> = HashMap::new();
    let mut count_b: HashMap<u32, u32> = HashMap::new();
    for id in &a[a_mid.clone()] {
        *count_a.entry(*id).or_default() += 1;
    }
    for id in &b[b_mid.clone()] {
        *count_b.entry(*id).or_default() += 1;
    }
    let mut keep_a = Vec::new();
    let mut keep_b = Vec::new();
    for i in a_mid {
        if count_b.contains_key(&a[i]) {
            keep_a.push(i);
        } else {
            changed_a[i] = true;
        }
    }
    for j in b_mid {
        if count_a.contains_key(&b[j]) {
            keep_b.push(j);
        } else {
            changed_b[j] = true;
        }
    }
    let sa: Vec<u32> = keep_a.iter().map(|i| a[*i]).collect();
    let sb: Vec<u32> = keep_b.iter().map(|j| b[*j]).collect();
    let mut del = vec![false; sa.len()];
    let mut ins = vec![false; sb.len()];
    let mut search = Search::new(sa.len() + sb.len());
    search.compare(&sa, &sb, 0..sa.len(), 0..sb.len(), &mut del, &mut ins);
    for (k, i) in keep_a.iter().enumerate() {
        changed_a[*i] |= del[k];
    }
    for (k, j) in keep_b.iter().enumerate() {
        changed_b[*j] |= ins[k];
    }
}

/// The working vectors of the bidirectional search.
struct Search {
    forward: Vec<isize>,
    backward: Vec<isize>,
    offset: isize,
    /// Work left (about `(n + m)` per edit step of a search); once spent,
    /// what is left to compare is marked changed whole.
    budget: u64,
}

/// The most work one diff spends finding a shortest edit script; past it
/// the rest of the files is shown as replaced (a correct, longer script),
/// so a pathological pair of files costs bounded time.
const MAX_DIFF_WORK: u64 = 200_000_000;

impl Search {
    fn new(total: usize) -> Self {
        let size = 2 * total + 3;
        Self {
            forward: vec![0; size],
            backward: vec![0; size],
            offset: total as isize + 1,
            budget: MAX_DIFF_WORK,
        }
    }

    /// Marks the changes between `a[xs]` and `b[ys]`, splitting at a middle
    /// snake until one side is empty.
    fn compare(
        &mut self,
        a: &[u32],
        b: &[u32],
        mut xs: Range<usize>,
        mut ys: Range<usize>,
        del: &mut [bool],
        ins: &mut [bool],
    ) {
        // Equal lines at either end match.
        while xs.start < xs.end && ys.start < ys.end && a[xs.start] == b[ys.start] {
            xs.start += 1;
            ys.start += 1;
        }
        while xs.start < xs.end && ys.start < ys.end && a[xs.end - 1] == b[ys.end - 1] {
            xs.end -= 1;
            ys.end -= 1;
        }
        if xs.is_empty() {
            ins[ys].fill(true);
            return;
        }
        if ys.is_empty() {
            del[xs].fill(true);
            return;
        }
        let Some((x, y)) = self.middle(a, b, xs.clone(), ys.clone()) else {
            del[xs].fill(true);
            ins[ys].fill(true);
            return;
        };
        self.compare(a, b, xs.start..x, ys.start..y, del, ins);
        self.compare(a, b, x..xs.end, y..ys.end, del, ins);
    }

    /// A point on a middle snake of a shortest edit script of `a[xs]` to
    /// `b[ys]` (both non-empty, differing at both ends): the script splits
    /// there into two of about half its length. `None` once the budget is
    /// spent.
    fn middle(
        &mut self,
        a: &[u32],
        b: &[u32],
        xs: Range<usize>,
        ys: Range<usize>,
    ) -> Option<(usize, usize)> {
        let (x0, y0) = (xs.start as isize, ys.start as isize);
        let (n, m) = ((xs.end - xs.start) as isize, (ys.end - ys.start) as isize);
        let delta = n - m;
        let odd = delta & 1 == 1;
        let off = self.offset;
        // forward[k]: the furthest x on diagonal k = x - y from (0, 0);
        // backward[k]: the smallest x on diagonal k from (n, m).
        self.forward[(off + 1) as usize] = 0;
        self.backward[(off + delta - 1) as usize] = n;
        let max = (n + m + 1) / 2;
        for d in 0..=max {
            let step = (n + m) as u64;
            if self.budget < step {
                return None;
            }
            self.budget -= step;
            // Diagonals from the highest down (ruling T26.2: git's tie-breaks).
            let mut k = d;
            while k >= -d {
                let i = (off + k) as usize;
                let mut x = if k == -d || (k != d && self.forward[i - 1] < self.forward[i + 1]) {
                    self.forward[i + 1]
                } else {
                    self.forward[i - 1] + 1
                };
                let mut y = x - k;
                while x < n && y < m && a[(x0 + x) as usize] == b[(y0 + y) as usize] {
                    x += 1;
                    y += 1;
                }
                self.forward[i] = x;
                if odd && k >= delta - (d - 1) && k <= delta + (d - 1) {
                    let j = (off + k) as usize;
                    if self.backward[j] <= x {
                        return Some(((x0 + x) as usize, (y0 + y) as usize));
                    }
                }
                k -= 2;
            }
            let mut k = d + delta;
            while k >= -d + delta {
                let i = (off + k) as usize;
                let mut x = if k == d + delta
                    || (k != -d + delta && self.backward[i - 1] < self.backward[i + 1])
                {
                    self.backward[i - 1]
                } else {
                    self.backward[i + 1] - 1
                };
                let mut y = x - k;
                while x > 0 && y > 0 && a[(x0 + x - 1) as usize] == b[(y0 + y - 1) as usize] {
                    x -= 1;
                    y -= 1;
                }
                self.backward[i] = x;
                if !odd && k >= -d && k <= d {
                    let j = (off + k) as usize;
                    if self.forward[j] >= x {
                        return Some(((x0 + x) as usize, (y0 + y) as usize));
                    }
                }
                k -= 2;
            }
        }
        unreachable!("a middle snake exists within (n + m + 1) / 2 steps")
    }
}

/// Slides each group of changed lines of one file (`lines`, `changed`)
/// as described in the module docs; `other` is the other file's changes,
/// which stay put.
fn compact(lines: &[u32], changed: &mut [bool], other: &[bool]) {
    // gaps[u]: whether the other file has changed lines right after its
    // u-th unchanged line (u = 0: before the first).
    let mut gaps = Vec::with_capacity(other.len() + 1);
    let mut run = false;
    for is_changed in other {
        if *is_changed {
            run = true;
        } else {
            gaps.push(run);
            run = false;
        }
    }
    gaps.push(run);
    let len = lines.len();
    let mut start = 0;
    // Unchanged lines before `start`.
    let mut u = 0;
    loop {
        while start < len && !changed[start] {
            start += 1;
            u += 1;
        }
        if start == len {
            break;
        }
        let mut end = start;
        while end < len && changed[end] {
            end += 1;
        }
        let mut aligned;
        loop {
            let size = end - start;
            // Up as far as it goes, joining the group above.
            while start > 0 && lines[start - 1] == lines[end - 1] {
                start -= 1;
                end -= 1;
                changed[start] = true;
                changed[end] = false;
                u -= 1;
                while start > 0 && changed[start - 1] {
                    start -= 1;
                }
            }
            aligned = gaps.get(u).copied().unwrap_or(false).then_some(end);
            // Then down as far as it goes, joining the groups below.
            while end < len && lines[start] == lines[end] {
                changed[start] = false;
                changed[end] = true;
                start += 1;
                end += 1;
                u += 1;
                while end < len && changed[end] {
                    end += 1;
                }
                if gaps.get(u).copied().unwrap_or(false) {
                    aligned = Some(end);
                }
            }
            if end - start == size {
                break;
            }
        }
        // Back up to the lowest place opposite a change in the other file.
        if let Some(at) = aligned {
            while end > at {
                start -= 1;
                end -= 1;
                changed[start] = true;
                changed[end] = false;
                u -= 1;
            }
        }
        start = end;
    }
}

/// The changed regions of two marked files.
fn hunks(changed_a: &[bool], changed_b: &[bool]) -> Vec<Hunk> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < changed_a.len() || j < changed_b.len() {
        let (si, sj) = (i, j);
        while i < changed_a.len() && changed_a[i] {
            i += 1;
        }
        while j < changed_b.len() && changed_b[j] {
            j += 1;
        }
        if i > si || j > sj {
            out.push(Hunk { a: si..i, b: sj..j });
        } else {
            // An unchanged line on each side: they match.
            i += 1;
            j += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(a: &str, b: &str) -> Vec<(Range<usize>, Range<usize>)> {
        let (a, b) = (split_lines(a.as_bytes()), split_lines(b.as_bytes()));
        diff_lines(&a, &b).into_iter().map(|h| (h.a, h.b)).collect()
    }

    #[test]
    fn simple_edits() {
        assert_eq!(d("", ""), vec![]);
        assert_eq!(d("a\n", "a\n"), vec![]);
        assert_eq!(d("", "a\n"), vec![(0..0, 0..1)]);
        assert_eq!(d("a\nb\nc\n", "a\nx\nc\n"), vec![(1..2, 1..2)]);
        assert_eq!(d("a\nb", "a\nb\n"), vec![(1..2, 1..2)]);
    }

    #[test]
    fn insertions_slide_down() {
        // "a\n" inserted among a's: placed last.
        assert_eq!(d("a\na\nb\n", "a\na\na\nb\n"), vec![(2..2, 2..3)]);
    }

    /// `b`, rebuilt from `a` and the hunks.
    fn apply(a: &[&[u8]], b: &[&[u8]], hunks: &[Hunk]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut at = 0;
        for hunk in hunks {
            out.extend(a[at..hunk.a.start].iter().map(|l| l.to_vec()));
            out.extend(b[hunk.b.clone()].iter().map(|l| l.to_vec()));
            at = hunk.a.end;
        }
        out.extend(a[at..].iter().map(|l| l.to_vec()));
        out
    }

    #[test]
    fn a_costly_pair_is_still_a_correct_diff() {
        let a: String = (0..40_000)
            .map(|i| if i % 3 == 0 { "x\n" } else { "y\n" })
            .collect();
        let b: String = (0..40_000)
            .map(|i| if i % 5 == 0 { "y\n" } else { "x\n" })
            .collect();
        let (a, b) = (split_lines(a.as_bytes()), split_lines(b.as_bytes()));
        let started = std::time::Instant::now();
        let hunks = diff_lines(&a, &b);
        assert!(started.elapsed() < std::time::Duration::from_secs(30));
        let want: Vec<Vec<u8>> = b.iter().map(|l| l.to_vec()).collect();
        assert_eq!(apply(&a, &b, &hunks), want);
    }

    #[test]
    fn a_spent_budget_marks_the_rest_changed() {
        let (a, b) = ([1, 2, 3, 4], [4, 3, 2, 1]);
        let mut search = Search::new(8);
        search.budget = 1;
        let (mut del, mut ins) = (vec![false; 4], vec![false; 4]);
        search.compare(&a, &b, 0..4, 0..4, &mut del, &mut ins);
        assert!(del.iter().chain(&ins).all(|changed| *changed));
    }

    #[test]
    fn edit_scripts_are_shortest() {
        assert_eq!(d("a\nb\nc\nd\n", "b\nc\nd\ne\n").len(), 2);
        let hunks = d("x\ny\nx\ny\n", "y\nx\ny\nx\n");
        let cost: usize = hunks.iter().map(|(a, b)| a.len() + b.len()).sum();
        assert_eq!(cost, 2);
    }
}
