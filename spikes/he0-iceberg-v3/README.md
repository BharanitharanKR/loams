# HE0 Task 11 spike: iceberg-rust and Iceberg v3 (unmerged branch)

The evidence behind `docs/plans/he0-iceberg-v3-spike-report.md`. This branch is never merged.

- `stack.sh`: one podman pod running RustFS 1.0.0, Postgres 17 and Lakekeeper v0.13.6 (no STS, no
  remote signing). On demand it also runs the House (loams-fabric 0.1.0), Spark 4.0.4 with Iceberg 1.12.0,
  and Trino 483.
- `house-baseline.sh`, `ch.sh`: the House writes the interop baseline tables over ClickHouse HTTP.
- `spark/*.sql`: the v3 tables (deletion vectors, row lineage, Variant, a v2 control, an upgrade in
  place), the expected answers and the lineage fingerprints.
- `reader-common/probe.rs`: one probe built three ways: `reader-0.10` (crates.io 0.10.1),
  `reader-0.11rc2` (git, v0.11.0-rc.2 at 8590485) and `reader-main` (git, main at 000e791). Each is its
  own Cargo workspace.
- `duckdb/read.py` (DuckDB 1.5.6), `trino/` (Trino 483): the other readers.
- `results/`: the outputs the report quotes.

Order: `stack.sh up`, `stack.sh house`, `./ch.sh 'CREATE DATABASE base'`, `house-baseline.sh`,
`stack.sh spark spark/01-write-v3.sql`, then `02` and `03`. Next, the readers. Then the write probes
(`create`, `append`, `upgrade`), `05`–`08`, and `stack.sh trino` followed by `trino-sql`. Finally
`stack.sh down`.
