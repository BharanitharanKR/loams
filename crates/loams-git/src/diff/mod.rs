//! Line diffs of file contents (Ruling 36; plan GT1 Task 26): [`myers`]
//! finds the changed lines as `git diff` does and [`unified`] prints them
//! as `git diff` does. The three-way merge built on them is
//! [`crate::ws::merge3`]. Task 38's porcelain uses these as a library.

pub mod myers;
pub mod unified;

pub use myers::{Hunk, diff_lines, split_lines};
pub use unified::{DiffSide, write_file_patch};
