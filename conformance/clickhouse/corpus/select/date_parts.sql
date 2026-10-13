-- HS1 Task 11: differential_select_corpus_on_lake
SELECT toYear(d), toMonth(d), count() FROM t GROUP BY 1, 2 ORDER BY 1, 2
