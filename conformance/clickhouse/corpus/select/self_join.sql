-- HS1 Task 11: differential_select_corpus_on_lake
SELECT a.g, count() FROM t AS a INNER JOIN t AS b ON a.k = b.k + 1 GROUP BY a.g ORDER BY a.g
