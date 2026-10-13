-- HS1 Task 11: differential_select_corpus_on_lake
SELECT g, argMax(k, v), max(v) FROM t GROUP BY g ORDER BY g
