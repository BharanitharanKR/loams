//! UP1 Task 1: Git's format constants match the registry. The segment and
//! checkpoint readers already accept `MIN_VERSION..=VERSION` (their policy in
//! `format/mod.rs`); the registry carries the same window.

use loams_common::formats;
use loams_git::format::checkpoint::{CHECKPOINT_MIN_VERSION, CHECKPOINT_VERSION};
use loams_git::format::segment::{SEGMENT_MIN_VERSION, SEGMENT_VERSION};

#[test]
fn format_constants_match_registry() {
    let segment = formats::spec_of(formats::GIT_SEGMENT);
    assert_eq!(
        (segment.min_read, segment.current),
        (SEGMENT_MIN_VERSION, SEGMENT_VERSION)
    );
    let checkpoint = formats::spec_of(formats::GIT_CHECKPOINT);
    assert_eq!(
        (checkpoint.min_read, checkpoint.current),
        (CHECKPOINT_MIN_VERSION, CHECKPOINT_VERSION)
    );
}
