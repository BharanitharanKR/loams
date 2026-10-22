-- After iceberg-rust's RowDelta commit: what Trino reads.
SELECT 'trino dv', count(*), sum(id), sum("$row_id"), sum("$row_id" * id) FROM spark.rdw.dv;
SELECT 'trino dv>=500', count(*), sum(id), sum("$row_id"), sum("$row_id" * id) FROM spark.rdw.dv WHERE id >= 500;
SELECT 'trino dv_part', count(*), sum(id), sum("$row_id"), sum("$row_id" * id) FROM spark.rdw.dv_part;
SELECT 'trino dv_part>=1000', count(*), sum(id), sum("$row_id"), sum("$row_id" * id) FROM spark.rdw.dv_part WHERE id >= 1000;
SELECT 'trino upgraded', count(*), sum(id), sum("$row_id"), sum("$row_id" * id) FROM spark.rdw.upgraded;
SELECT 'trino upgraded>=500', count(*), sum(id), sum("$row_id"), sum("$row_id" * id) FROM spark.rdw.upgraded WHERE id >= 500;
SELECT 'trino pos_v2', count(*), sum(id), 0, 0 FROM spark.rdw.pos_v2;
SELECT 'trino pos_v2>=500', count(*), sum(id), 0, 0 FROM spark.rdw.pos_v2 WHERE id >= 500;
