-- After iceberg-rust's RowDelta commit: what Spark reads.
SELECT 'spark dv', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.rdw.dv;
SELECT 'spark dv>=500', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.rdw.dv WHERE id >= 500;
SELECT 'spark dv_part', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.rdw.dv_part;
SELECT 'spark dv_part>=1000', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.rdw.dv_part WHERE id >= 1000;
SELECT 'spark upgraded', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.rdw.upgraded;
SELECT 'spark upgraded>=500', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.rdw.upgraded WHERE id >= 500;
SELECT 'spark pos_v2', count(*), sum(id), 0, 0 FROM spark.rdw.pos_v2;
SELECT 'spark pos_v2>=500', count(*), sum(id), 0, 0 FROM spark.rdw.pos_v2 WHERE id >= 500;
SELECT 'spark deletes ' || t, content, file_format, count(*), sum(record_count) FROM (SELECT 'dv' AS t, * FROM spark.rdw.dv.delete_files UNION ALL SELECT 'pos_v2', * FROM spark.rdw.pos_v2.delete_files) GROUP BY t, content, file_format ORDER BY 1, 3;
