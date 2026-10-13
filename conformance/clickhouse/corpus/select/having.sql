-- HS1 Task 11: differential_select_corpus_on_lake
SELECT toStartOfMonth(d) AS m, uniqExact(g) FROM t GROUP BY m HAVING count() > 10 ORDER BY m
