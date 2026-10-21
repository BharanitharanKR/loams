SELECT 'lineage_rowid_ne_id', count(CASE WHEN _row_id <> id THEN 1 END) FROM spark.v3.lineage;
SELECT 'dv_rowid_ne_id', count(CASE WHEN _row_id <> id THEN 1 END), count(*) FROM spark.v3.dv;
