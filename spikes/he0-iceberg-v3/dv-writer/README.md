# G2: DeletionVectorWriter interop (unmerged)

Checks the `DeletionVectorWriter` on ostrium-labs/iceberg-rust (`dv/writer`, fork PR #2)
against Spark 4.0.4 with Iceberg 1.12.0, Trino 483 and iceberg-rust's own reader.

`run.sh` (after `../stack.sh up`):

1. `setup.sql`: Spark writes fresh copies of the spike's DV tables in `spark.dvw`: `dv` (4 data
   files plus an update file, 4 DVs), `dv_part` (partitioned, 8 DVs) and `upgraded` (v2 with
   position deletes, upgraded to v3, 8 DVs).
2. `before.sql`: Spark computes the expected answers, the rows left once the positions
   `p % 7 = 3` of every data file go (`_pos`), with `sum(_row_id)` and `sum(_row_id * id)`.
3. `src/main.rs write`: for each data file, merge the Spark-written DV with the new positions in
   a `DeletionVectorWriter` (one writer and one Puffin file per partition).
4. `commit.scala`: Iceberg Java's RowDelta commits the new DVs and removes the old ones.
5. `after.sql`, `src/main.rs read` and `trino-after.sql` (`run.sh trino` after
   `../stack.sh trino`) read the tables, with and without a filter.

Result (2026-10-22, `results/`): Spark, Trino and iceberg-rust give the expected answer in all
six cases. For example `dv`: 686 rows, `sum(id)` 343,490, `sum(_row_id * id)` 229,202,132.
