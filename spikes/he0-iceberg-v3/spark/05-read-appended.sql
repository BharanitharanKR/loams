-- Spark reads the rows iceberg-rust appended to a v3 table: is their lineage valid?
SELECT 'rust_append', v, count(*), min(_row_id), max(_row_id), count(DISTINCT _row_id), min(_last_updated_sequence_number), max(_last_updated_sequence_number) FROM spark.v3.rust_append GROUP BY v ORDER BY v;
SELECT 'rust_append_snapshots', snapshot_id, operation, summary['added-records'] FROM spark.v3.rust_append.snapshots ORDER BY committed_at;
SELECT 'rust_append_manifests', path LIKE '%he0%', added_data_files_count FROM spark.v3.rust_append.manifests;
-- The House table iceberg-rust upgraded from v2 to v3.
SELECT 'house_compacted_v3', count(*), sum(id), min(_row_id), max(_row_id), count(_row_id) FROM house.ns_1.base.compacted;
SHOW TBLPROPERTIES house.ns_1.base.compacted ('format-version');
