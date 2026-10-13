-- HS1 Task 11: differential_select_corpus_on_lake
SELECT * FROM (SELECT 'a' AS x, count() FROM t WHERE flag = 0 UNION ALL SELECT 'b' AS x, count() FROM t WHERE flag = 1) ORDER BY x
