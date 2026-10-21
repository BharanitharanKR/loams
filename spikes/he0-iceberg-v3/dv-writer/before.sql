-- The expected answers, before the commit: the rows left once positions p % 7 = 3 go.
SELECT 'expect dv', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.dvw.dv WHERE _pos % 7 <> 3;
SELECT 'expect dv>=500', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.dvw.dv WHERE _pos % 7 <> 3 AND id >= 500;
SELECT 'expect dv_part', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.dvw.dv_part WHERE _pos % 7 <> 3;
SELECT 'expect dv_part>=1000', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.dvw.dv_part WHERE _pos % 7 <> 3 AND id >= 1000;
SELECT 'expect upgraded', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.dvw.upgraded WHERE _pos % 7 <> 3;
SELECT 'expect upgraded>=500', count(*), sum(id), sum(_row_id), sum(_row_id * id) FROM spark.dvw.upgraded WHERE _pos % 7 <> 3 AND id >= 500;
