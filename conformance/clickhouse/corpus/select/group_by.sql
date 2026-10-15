-- HS1 Task 11: differential_select_corpus_on_lake
SELECT g, count(), round(avg(v), 6) FROM t GROUP BY g ORDER BY g
