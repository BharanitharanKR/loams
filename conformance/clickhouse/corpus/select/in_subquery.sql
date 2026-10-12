-- HS1 Task 11: differential_select_corpus_on_lake
SELECT count() FROM t WHERE k IN (SELECT k FROM t WHERE flag = 1)
