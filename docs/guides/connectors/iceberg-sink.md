# Iceberg raw-topic sink (native)

**Not available yet. The owner decided (2026-10-13) to wait for an iceberg-rust
release on arrow 59 and revisit once it is at least 14 days old (CN2 R0.15,
R17.1).**

The plan's native sink (OR-8, `loams_connector_lakehouse::iceberg_sink`) appends
through iceberg-rust with the Fabric checkpoint in the snapshot summary property
`loams.checkpoint` (`exactly_once`), against a Lakekeeper catalog. iceberg-rust
0.10.1, the newest release on 2026-10-13, requires arrow and parquet 58, while
the `fabric/` workspace is on arrow 59. Its `main` branch moved to arrow 59.2 on
2026-09-03, but no release includes that yet. The alternatives (linking arrow 58
beside 59, or moving the workspace's Arrow) were measured and not chosen; R17.1
has the numbers.

Until then the factory refuses to start an Iceberg sink with an error naming
R0.15 and R17.1. The ★ Iceberg manifest's declared fallback (OR-4) and CN1's
Iceberg source are unchanged.
