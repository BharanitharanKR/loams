SELECT 'fp lineage>=40', count(*), sum("$row_id"), sum("$row_id" * id), sum("$last_updated_sequence_number") FROM spark.v3.lineage WHERE id >= 40;
SELECT 'fp dv>=500', count(*), sum("$row_id"), sum("$row_id" * id), sum("$last_updated_sequence_number") FROM spark.v3.dv WHERE id >= 500;
SELECT 'fp dv_part>=1000', count(*), sum("$row_id"), sum("$row_id" * id), sum("$last_updated_sequence_number") FROM spark.v3.dv_part WHERE id >= 1000;
SELECT 'fp upgraded>=500', count(*), sum("$row_id"), sum("$row_id" * id), sum("$last_updated_sequence_number") FROM spark.v3.upgraded WHERE id >= 500;
SELECT 'fp dv_part all', count(*), sum("$row_id"), sum("$row_id" * id), sum("$last_updated_sequence_number") FROM spark.v3.dv_part;
SELECT 'fp upgraded all', count(*), sum("$row_id"), sum("$row_id" * id), sum("$last_updated_sequence_number") FROM spark.v3.upgraded;
SELECT 'rust_append', v, count(*), min("$row_id"), max("$row_id") FROM spark.v3.rust_append GROUP BY v ORDER BY v;
SELECT 'house_v3_upgraded_rowid', count(*), count("$row_id") FROM house."ns_1.base".compacted;
