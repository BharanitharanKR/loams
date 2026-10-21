-- After the commit: what Spark reads through the deletion vectors iceberg-rust wrote.
SELECT 'spark dv', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.dvw.dv;
SELECT 'spark dv>=500', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.dvw.dv WHERE id >= 500;
SELECT 'spark dv_part', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.dvw.dv_part;
SELECT 'spark dv_part>=1000', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.dvw.dv_part WHERE id >= 1000;
SELECT 'spark upgraded', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.dvw.upgraded;
SELECT 'spark upgraded>=500', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.dvw.upgraded WHERE id >= 500;
SELECT 'spark dv deletes', content, file_format, count(*), sum(record_count) FROM spark.dvw.dv.delete_files GROUP BY content, file_format;
