//! The limits module documents every constant and lists it once.

use loams_git::limits::LIMITS;

const SOURCE: &str = include_str!("../../src/limits.rs");

/// Every `pub const` (other than the table itself) with its doc comment.
fn constants() -> Vec<(String, String)> {
    let mut found = Vec::new();
    let mut doc = String::new();
    for line in SOURCE.lines().map(str::trim) {
        if let Some(text) = line.strip_prefix("///") {
            doc.push_str(text);
            doc.push(' ');
        } else if let Some(rest) = line.strip_prefix("pub const ") {
            let name = rest
                .split(':')
                .next()
                .unwrap_or_default()
                .trim()
                .to_string();
            if name != "LIMITS" {
                found.push((name, std::mem::take(&mut doc)));
            }
            doc.clear();
        } else {
            doc.clear();
        }
    }
    found
}

#[test]
fn limits_are_documented() {
    let constants = constants();
    assert!(constants.len() >= 40, "found {}", constants.len());
    for (name, doc) in &constants {
        assert!(
            doc.contains("§36 §"),
            "{name}'s doc comment names no §36 section: {doc:?}"
        );
    }
    let listed: Vec<&str> = LIMITS.iter().map(|limit| limit.name).collect();
    let declared: Vec<&str> = constants.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        listed, declared,
        "LIMITS lists every constant once, in order"
    );
    for limit in LIMITS {
        let (_, doc) = constants
            .iter()
            .find(|(name, _)| name == limit.name)
            .unwrap();
        assert!(
            doc.contains(limit.section),
            "{}: {doc:?} vs {}",
            limit.name,
            limit.section
        );
        assert!(!limit.value.is_empty());
    }
}

#[test]
fn limits_have_the_planned_values() {
    use loams_git::limits::*;
    use std::time::Duration;
    assert_eq!(MAX_GROUP_TXNS, 64);
    assert_eq!(MAX_SEGMENT_BYTES, 1 << 20);
    assert_eq!(IDEMPOTENCY_WINDOW, Duration::from_secs(3600));
    assert_eq!(CHECKPOINT_EVERY_BYTES, 8 << 20);
    assert_eq!(EXACTLY_RETENTION, Duration::from_secs(86_400));
    assert_eq!(MAX_PACK_BYTES, 2 << 30);
    assert_eq!(MULTIPART_THRESHOLD, 64 << 20);
    assert_eq!(MAX_WANTS, 65_536);
    assert_eq!(UPLOAD_DEADLINE, Duration::from_secs(1800));
    assert_eq!(MAX_SNAPSHOT_BYTES, 1 << 20);
    assert_eq!(SNAPSHOT_KEEP_ALL, Duration::from_secs(7 * 86_400));
    assert_eq!(MIN_COALESCED_RANGE, 64 << 10);
    assert_eq!(CACHE_DEFAULT_BYTES, 20 << 30);
    assert_eq!(SHIM_PASSTHROUGH_BUDGET, Duration::from_millis(2));
    assert_eq!(MAX_ENV_FILE_BYTES, 256 << 10);
    assert_eq!(SCAN_MAX_FILE_BYTES, 8 << 20);
}
