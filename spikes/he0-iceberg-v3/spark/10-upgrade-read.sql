SELECT 'rust_upgrade', v, count(*), count(_row_id), min(_row_id), max(_row_id), min(_last_updated_sequence_number), max(_last_updated_sequence_number) FROM spark.v3.rust_upgrade GROUP BY v ORDER BY v;
SELECT 'rust_upgrade_manifests', added_data_files_count, existing_data_files_count FROM spark.v3.rust_upgrade.manifests;
