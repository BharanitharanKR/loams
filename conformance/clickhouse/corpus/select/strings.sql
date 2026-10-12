-- HS1 Task 11: differential_select_corpus_on_lake
SELECT upper(g), length(g), concat(g, '-', toString(k)) FROM t ORDER BY k LIMIT 5
