SELECT 'trino dv', count(*), sum(id), sum("$row_id"), sum("$row_id" * id) FROM spark.dvw.dv;
SELECT 'trino dv>=500', count(*), sum(id), sum("$row_id"), sum("$row_id" * id) FROM spark.dvw.dv WHERE id >= 500;
SELECT 'trino dv_part', count(*), sum(id), sum("$row_id"), sum("$row_id" * id) FROM spark.dvw.dv_part;
SELECT 'trino dv_part>=1000', count(*), sum(id), sum("$row_id"), sum("$row_id" * id) FROM spark.dvw.dv_part WHERE id >= 1000;
SELECT 'trino upgraded', count(*), sum(id), sum("$row_id"), sum("$row_id" * id) FROM spark.dvw.upgraded;
SELECT 'trino upgraded>=500', count(*), sum(id), sum("$row_id"), sum("$row_id" * id) FROM spark.dvw.upgraded WHERE id >= 500;
