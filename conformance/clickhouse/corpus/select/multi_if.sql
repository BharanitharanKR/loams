-- HS1 Task 11: differential_select_corpus_on_lake
SELECT multiIf(v < 10, 'low', v < 50, 'mid', 'high') AS b, count() FROM t GROUP BY b ORDER BY b
