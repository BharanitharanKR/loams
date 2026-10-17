//! Every limit the GT1 plan names (plan "Limits"; D88's rule: one table in
//! code is the source). Each constant's doc comment names the § that sets
//! it, and [`LIMITS`] lists each one with its value as the docs print it.
//! The tasks that enforce a limit test it at the limit and one past it.

use std::time::Duration;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;
const MINUTE: u64 = 60;
const HOUR: u64 = 60 * MINUTE;
const DAY: u64 = 24 * HOUR;

// The WAL core (§36 §4–§7).

/// Transactions in one group commit (§36 §4.4).
pub const MAX_GROUP_TXNS: usize = 64;
/// The largest segment body, on every store (§36 §4.3, D390).
pub const MAX_SEGMENT_BYTES: u64 = MIB;
/// How long an idempotency key answers with its first receipt (§36 §4.5).
pub const IDEMPOTENCY_WINDOW: Duration = Duration::from_secs(HOUR);
/// Segments after which a checkpoint is written (§36 §7).
pub const CHECKPOINT_EVERY_SEGMENTS: u64 = 256;
/// Segment bytes after which a checkpoint is written (§36 §7).
pub const CHECKPOINT_EVERY_BYTES: u64 = 8 * MIB;
/// Ref updates in one transaction (§36 §5.2).
pub const MAX_REFS_PER_TXN: usize = 4096;
/// Ref updates in one push (§36 §6.3).
pub const MAX_REFS_PER_PUSH: usize = 4096;
/// How long segments stay readable for `ReadAt::Exactly` (§36 §4.6).
pub const EXACTLY_RETENTION: Duration = Duration::from_secs(DAY);
/// How long an unreferenced object survives before GC deletes it (§36 §7, §03 §7).
pub const GC_GRACE: Duration = Duration::from_secs(HOUR);
/// The largest pack one push may upload (§36 §6.3).
pub const MAX_PACK_BYTES: u64 = 2 * GIB;
/// Blobs above this size are uploaded with a multipart PUT (§36 §5.3, Ruling 1).
pub const MULTIPART_THRESHOLD: u64 = 64 * MIB;
/// How many fork parents object lookup falls through (§36 §4.7).
pub const MAX_FORK_DEPTH: usize = 8;
/// Fenced retries of one group before its transactions are answered `Unavailable` (§36 §4.2).
pub const MAX_FENCE_RETRIES: usize = 16;
/// `want` lines in one fetch request (§36 §6.1).
pub const MAX_WANTS: usize = 65_536;
/// `have` lines in one negotiation round (§36 §6.1).
pub const MAX_HAVES_PER_ROUND: usize = 256;
/// Negotiation rounds in one fetch (§36 §6.1).
pub const MAX_NEGOTIATION_ROUNDS: usize = 64;
/// A receive-pack connection idle this long is closed (§36 §6.3).
pub const RECEIVE_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// The longest an upload-pack response may take (§36 §6.1).
pub const UPLOAD_DEADLINE: Duration = Duration::from_secs(30 * MINUTE);
/// An idle sequencer is evicted after this long (§36 §4.4, Ruling 28).
pub const SEQ_IDLE_EVICT: Duration = Duration::from_secs(10 * MINUTE);
/// A forwarded transaction falls back to a local, fenced sequencer after this long (§36 §4.4, Ruling 27).
pub const FORWARD_TIMEOUT: Duration = Duration::from_secs(2);
/// The `head` hint is written at most this often (§36 §4.2).
pub const HINT_EVERY: Duration = Duration::from_secs(1);

// Monorepo mode (§36 §19).

/// The largest workspace snapshot body (§36 §19.4.2).
pub const MAX_SNAPSHOT_BYTES: u64 = MIB;
/// Paths in a snapshot's `changed` hint before it overflows (§36 §19.4.2).
pub const MAX_CHANGED_HINT: usize = 10_000;
/// A writer's latest snapshot younger than this makes other mounts read-only (§36 §19.4.3).
pub const WRITER_IDLE: Duration = Duration::from_secs(10 * MINUTE);
/// A snapshot naming more workspace packs than this is compacted (§36 §19.4.5).
pub const WS_PACKS_COMPACT_AT: usize = 64;
/// Every snapshot is kept this long (§36 §19.4.5).
pub const SNAPSHOT_KEEP_ALL: Duration = Duration::from_secs(7 * DAY);
/// The last snapshot of each hour is kept this long (§36 §19.4.5).
pub const SNAPSHOT_KEEP_HOURLY: Duration = Duration::from_secs(30 * DAY);
/// Salvaged state is kept this long (§36 §19.4.5).
pub const SALVAGE_RETENTION: Duration = Duration::from_secs(30 * DAY);
/// Object ids in one fetch batch (§36 §19.5.1).
pub const FETCH_BATCH_OIDS: usize = 1024;
/// Tree levels fetched ahead of a directory read (§36 §19.5.1).
pub const TREE_PREFETCH_DEPTH: u8 = 2;
/// Range reads closer than this are coalesced into one (§36 §19.5.1).
pub const MIN_COALESCED_RANGE: u64 = 64 * KIB;
/// A save waits this long after the last write (§36 §19.5.3).
pub const SAVE_DEBOUNCE: Duration = Duration::from_secs(2);
/// Saves of one workspace are at least this far apart (§36 §19.5.3).
pub const SAVE_MIN_INTERVAL: Duration = Duration::from_secs(1);
/// One object fetch gives up after this long (§36 §19.6.3).
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(30);
/// vfsd's default host cache budget (§36 §19.6.2).
pub const CACHE_DEFAULT_BYTES: u64 = 20 * GIB;
/// The default quota of workspaces per principal (§36 §19.11; enforced by GT1 Task 75).
pub const MAX_WORKSPACES_PER_PRINCIPAL: usize = 100_000;
/// The shim's budget for passing a command through to real git (§36 §19.7.6).
pub const SHIM_PASSTHROUGH_BUDGET: Duration = Duration::from_millis(2);
/// The shim's start-up budget for a native command (§36 §19.7.6).
pub const SHIM_NATIVE_STARTUP_BUDGET: Duration = Duration::from_millis(20);
/// A rendered env file's values are refreshed after this long (§36 §19.13.7).
pub const ENV_RENDER_TTL: Duration = Duration::from_secs(5 * MINUTE);
/// The largest env file (§36 §19.13.2).
pub const MAX_ENV_FILE_BYTES: u64 = 256 * KIB;
/// Keys in one env file (§36 §19.13.2).
pub const MAX_ENV_KEYS: usize = 2048;
/// Files larger than this are scanned in their first and last 8 MiB, and
/// binary files are skipped (§36 §19.13.9).
pub const SCAN_MAX_FILE_BYTES: u64 = 8 * MIB;
/// Paths (changed files and their directories) a commit may change against
/// its first parent and still get a changed-path Bloom filter; above it the
/// filter answers "maybe" to every path (git's value; §36 §19.8).
pub const BLOOM_MAX_CHANGED_PATHS: usize = 512;
/// Layers in one commit-graph chain; a new layer merges the ones below it
/// to stay within this (§36 §19.8).
pub const MAX_GRAPH_LAYERS: usize = 16;
/// Revisions in one `Log` call (§36 §19.8).
pub const MAX_LOG_REVS: usize = 64;
/// Pathspecs in one `Log` or `DiffStat` call (§36 §19.8).
pub const MAX_HISTORY_PATHS: usize = 64;
/// How long one `Grep` call searches; past it the answer is what was found,
/// flagged `deadline_exceeded` (§36 §19.8).
pub const GREP_DEADLINE: Duration = Duration::from_secs(30);
/// Matching lines (or, with `-l`, files) one `Grep` call answers; past it
/// the answer is flagged `truncated` (§36 §19.8).
pub const GREP_MAX_MATCHES: usize = 10_000;
/// Patterns (`-e`) in one `Grep` call (§36 §19.8).
pub const MAX_GREP_PATTERNS: usize = 64;

/// One limit as the limits table prints it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limit {
    /// The constant's name in this module.
    pub name: &'static str,
    /// Its value as the docs print it.
    pub value: &'static str,
    /// The § that sets it.
    pub section: &'static str,
}

const fn limit(name: &'static str, value: &'static str, section: &'static str) -> Limit {
    Limit {
        name,
        value,
        section,
    }
}

/// Every constant above, in order: the source of the limits table.
pub const LIMITS: &[Limit] = &[
    limit("MAX_GROUP_TXNS", "64", "§36 §4.4"),
    limit("MAX_SEGMENT_BYTES", "1 MiB", "§36 §4.3"),
    limit("IDEMPOTENCY_WINDOW", "1 h", "§36 §4.5"),
    limit("CHECKPOINT_EVERY_SEGMENTS", "256", "§36 §7"),
    limit("CHECKPOINT_EVERY_BYTES", "8 MiB", "§36 §7"),
    limit("MAX_REFS_PER_TXN", "4096", "§36 §5.2"),
    limit("MAX_REFS_PER_PUSH", "4096", "§36 §6.3"),
    limit("EXACTLY_RETENTION", "24 h", "§36 §4.6"),
    limit("GC_GRACE", "1 h", "§36 §7"),
    limit("MAX_PACK_BYTES", "2 GiB", "§36 §6.3"),
    limit("MULTIPART_THRESHOLD", "64 MiB", "§36 §5.3"),
    limit("MAX_FORK_DEPTH", "8", "§36 §4.7"),
    limit("MAX_FENCE_RETRIES", "16", "§36 §4.2"),
    limit("MAX_WANTS", "65,536", "§36 §6.1"),
    limit("MAX_HAVES_PER_ROUND", "256", "§36 §6.1"),
    limit("MAX_NEGOTIATION_ROUNDS", "64", "§36 §6.1"),
    limit("RECEIVE_IDLE_TIMEOUT", "60 s", "§36 §6.3"),
    limit("UPLOAD_DEADLINE", "30 min", "§36 §6.1"),
    limit("SEQ_IDLE_EVICT", "10 min", "§36 §4.4"),
    limit("FORWARD_TIMEOUT", "2 s", "§36 §4.4"),
    limit("HINT_EVERY", "1 s", "§36 §4.2"),
    limit("MAX_SNAPSHOT_BYTES", "1 MiB", "§36 §19.4.2"),
    limit("MAX_CHANGED_HINT", "10,000 paths", "§36 §19.4.2"),
    limit("WRITER_IDLE", "10 min", "§36 §19.4.3"),
    limit("WS_PACKS_COMPACT_AT", "64", "§36 §19.4.5"),
    limit("SNAPSHOT_KEEP_ALL", "7 d", "§36 §19.4.5"),
    limit("SNAPSHOT_KEEP_HOURLY", "30 d", "§36 §19.4.5"),
    limit("SALVAGE_RETENTION", "30 d", "§36 §19.4.5"),
    limit("FETCH_BATCH_OIDS", "1,024", "§36 §19.5.1"),
    limit("TREE_PREFETCH_DEPTH", "2", "§36 §19.5.1"),
    limit("MIN_COALESCED_RANGE", "64 KiB", "§36 §19.5.1"),
    limit("SAVE_DEBOUNCE", "2 s", "§36 §19.5.3"),
    limit("SAVE_MIN_INTERVAL", "1 s", "§36 §19.5.3"),
    limit("FETCH_TIMEOUT", "30 s", "§36 §19.6.3"),
    limit("CACHE_DEFAULT_BYTES", "20 GiB", "§36 §19.6.2"),
    limit("MAX_WORKSPACES_PER_PRINCIPAL", "100,000", "§36 §19.11"),
    limit("SHIM_PASSTHROUGH_BUDGET", "2 ms", "§36 §19.7.6"),
    limit("SHIM_NATIVE_STARTUP_BUDGET", "20 ms", "§36 §19.7.6"),
    limit("ENV_RENDER_TTL", "5 min", "§36 §19.13.7"),
    limit("MAX_ENV_FILE_BYTES", "256 KiB", "§36 §19.13.2"),
    limit("MAX_ENV_KEYS", "2,048", "§36 §19.13.2"),
    limit("SCAN_MAX_FILE_BYTES", "8 MiB", "§36 §19.13.9"),
    limit("BLOOM_MAX_CHANGED_PATHS", "512 paths", "§36 §19.8"),
    limit("MAX_GRAPH_LAYERS", "16", "§36 §19.8"),
    limit("MAX_LOG_REVS", "64", "§36 §19.8"),
    limit("MAX_HISTORY_PATHS", "64", "§36 §19.8"),
    limit("GREP_DEADLINE", "30 s", "§36 §19.8"),
    limit("GREP_MAX_MATCHES", "10,000", "§36 §19.8"),
    limit("MAX_GREP_PATTERNS", "64", "§36 §19.8"),
];
