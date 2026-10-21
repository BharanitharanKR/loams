-- What Spark reads back: the expected answers for the iceberg-rust reader.
SELECT 'dv', count(*), sum(id), sum(CASE WHEN v = 'u' THEN 1 ELSE 0 END) FROM spark.v3.dv;
SELECT 'dv_part', count(*), sum(id), sum(CASE WHEN v = 'm' THEN 1 ELSE 0 END) FROM spark.v3.dv_part;
SELECT 'lineage', count(*), sum(id), sum(CASE WHEN v = 'c' THEN 1 ELSE 0 END) FROM spark.v3.lineage;
SELECT 'variant', count(*), sum(id) FROM spark.v3.variant;
SELECT 'posdel_v2', count(*), sum(id), sum(CASE WHEN v = 'u' THEN 1 ELSE 0 END) FROM spark.v3.posdel_v2;
SELECT 'upgraded', count(*), sum(id) FROM spark.v3.upgraded;
SELECT 'lineage_cols', id, v, _row_id, _last_updated_sequence_number FROM spark.v3.lineage WHERE id IN (0, 3, 13, 49, 50, 53, 99) ORDER BY id;
SELECT 'variant_rows', id, to_json(payload) FROM spark.v3.variant ORDER BY id;
SELECT 'dv_delete_files', content, file_format, record_count, referenced_data_file IS NOT NULL, content_offset, content_size_in_bytes FROM spark.v3.dv.delete_files;
SELECT 'dv_part_delete_files', file_format, count(*) FROM spark.v3.dv_part.delete_files GROUP BY file_format;
SELECT 'posdel_v2_delete_files', file_format, count(*) FROM spark.v3.posdel_v2.delete_files GROUP BY file_format;
SELECT 'upgraded_delete_files', file_format, count(*) FROM spark.v3.upgraded.delete_files GROUP BY file_format;
SELECT 'dv_snapshots', snapshot_id, operation FROM spark.v3.dv.snapshots ORDER BY committed_at;
SHOW TBLPROPERTIES spark.v3.dv ('format-version');
SHOW TBLPROPERTIES spark.v3.upgraded ('format-version');
