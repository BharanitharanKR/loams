-- Lineage fingerprints to compare readers: count, sum(_row_id), sum(_row_id * id), sum(_lusn).
SELECT 'fp lineage>=40', count(*), sum(_row_id), sum(_row_id * id), sum(_last_updated_sequence_number) FROM spark.v3.lineage WHERE id >= 40;
SELECT 'fp dv>=500', count(*), sum(_row_id), sum(_row_id * id), sum(_last_updated_sequence_number) FROM spark.v3.dv WHERE id >= 500;
SELECT 'fp dv_part>=1000', count(*), sum(_row_id), sum(_row_id * id), sum(_last_updated_sequence_number) FROM spark.v3.dv_part WHERE id >= 1000;
SELECT 'fp upgraded>=500', count(*), sum(_row_id), sum(_row_id * id), sum(_last_updated_sequence_number) FROM spark.v3.upgraded WHERE id >= 500;
SELECT 'fp dv_part all', count(*), sum(_row_id), sum(_row_id * id), sum(_last_updated_sequence_number) FROM spark.v3.dv_part;
SELECT 'fp upgraded all', count(*), sum(_row_id), sum(_row_id * id), sum(_last_updated_sequence_number) FROM spark.v3.upgraded;
