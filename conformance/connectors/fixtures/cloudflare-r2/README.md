# Cloudflare R2 fixtures

Recorded only from real R2 (OR-2), by the `connectors-nightly` job's
`r2_fixture_replay_and_nightly` (`fabric/crates/loams-connector-objects/tests/r2.rs`)
through CN2 Task 6's `FixtureProxy`, with credentials scrubbed by its header
and JSON-pointer rules. PR CI replays them; an unmatched request fails.

Nothing is recorded yet: Task 6's proxy is not built, and the first nightly
also records what R2 answers to a conflicting `If-None-Match: *` PUT (R0.22,
unverified: 412 is expected). Until a recording lands here the replay half of
the test skips, and `cloudflare-r2` stays `planned` (CN2 R13.5).
