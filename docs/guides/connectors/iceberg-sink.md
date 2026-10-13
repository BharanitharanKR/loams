# Iceberg raw-topic sink (native)

**Not available yet: it waits on an owner decision (CN2 R0.15, R17.1).**

The plan's native sink (OR-8, `loams_connector_lakehouse::iceberg_sink`) appends
through iceberg-rust with the Fabric checkpoint in the snapshot summary property
`loams.checkpoint` (`exactly_once`), against a Lakekeeper catalog. iceberg-rust
0.10.1, the newest release on 2026-10-13, requires arrow and parquet 58, while
the `fabric/` workspace is on arrow 59. The owner chooses between:

- **(a)** waiting for an iceberg-rust release on arrow 59 (its `main` branch moved
  to arrow 59.2 on 2026-09-03; no release carries it yet);
- **(b)** linking iceberg 0.10.1 in `loams-connector-lakehouse` only, with arrow
  58 beside 59 and batches crossing by Arrow IPC bytes;
- **(c)** moving the workspace's Arrow.

Until then the factory refuses to start an Iceberg sink with an error naming
R0.15. The ★ Iceberg manifest's declared fallback (OR-4) and CN1's Iceberg
source are unchanged.
