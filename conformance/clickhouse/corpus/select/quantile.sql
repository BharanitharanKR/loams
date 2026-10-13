-- HS1 Task 11: differential_select_corpus_on_lake
SELECT round(quantileExact(0.5)(v), 4), quantileExact(0.9)(k) FROM t
