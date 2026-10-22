# G3: RowDelta interop (unmerged)

Checks `RowDeltaAction` on ostrium-labs/iceberg-rust (`rd/integration`, fork PRs #4 to #6:
apache/iceberg-rust#2203 carried, plus merge-on-read) against Spark 4.0.4 with Iceberg 1.12.0,
Trino 483, DuckDB 2.0.0 and iceberg-rust's own reader.

`run.sh` (after `../stack.sh up` and `../stack.sh trino`):

1. `setup.sql`: Spark writes four tables in `spark.rdw`:
   - `dv`: 4 deletion vectors;
   - `dv_part`: partitioned, 8 deletion vectors;
   - `upgraded`: v2 with position deletes, upgraded to v3, 8 deletion vectors;
   - `pos_v2`: v2, with Spark's Parquet position deletes.
2. `before.sql`: Spark computes the expected answers, the rows left once positions `p % 7 = 3`
   of every data file go (`_pos`).
3. `src/main.rs commit` deletes those positions in one RowDelta commit per table.
   - On v3 it writes deletion vectors with `DeletionVectorWriter`. Each one merges the data
     file's Spark-written vector, and the commit removes the old vectors.
   - On v2 it adds Parquet position delete files from `PositionDeleteFileWriter`.
4. `after.sql` (Spark), `src/main.rs read` (iceberg-rust), `trino-after.sql` (Trino) and
   `duckdb_after.py` (DuckDB 2.0.0, extension `4c0c0874c4`) read every table, with and without a
   filter.

## Result (2026-10-22, `results/`)

All five give Spark's precomputed answer in all eight cases:

| table | rows | `sum(id)` | `sum(_row_id)` | `sum(_row_id * id)` |
|---|---|---|---|---|
| dv | 686 | 343,490 | 343,490 | 229,202,132 |
| dv, id >= 500 | 343 | 257,530 | 257,530 | 200,507,656 |
| dv_part | 2,214 | 3,293,154 | 3,306,450 | 6,143,797,921 |
| dv_part, id >= 1000 | 1,467 | 2,919,770 | 2,645,388 | 5,797,377,065 |
| upgraded | 762 | 422,018 | 416,818 | 268,048,684 |
| upgraded, id >= 500 | 418 | 335,846 | 296,246 | 230,669,264 |
| pos_v2 | 770 | 384,914 | — | — |
| pos_v2, id >= 500 | 385 | 288,742 | — | — |

## Spark's deletion vectors are run-length encoded

Spark wrote a 135-byte blob for 75 positions, and its roaring header shows run containers:
- the length is 127 and the magic is `d1d33964`;
- there is one bitmap, with key 0;
- the cookie is `12347` (`SERIAL_COOKIE`, run containers present);
- the 102-byte container is 25 runs of 4 bytes plus a 2-byte run count.

`DeleteVector::serialize` (apache/iceberg-rust#3232) writes array containers instead: 207 bytes
for 100 positions. Both decode the same, but run optimisation belongs in the upstream PR.
