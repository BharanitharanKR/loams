-- HS1 Task 11: differential_select_corpus_on_lake
WITH top AS (SELECT g, max(v) AS mv FROM t GROUP BY g) SELECT g, round(mv, 4) FROM top ORDER BY mv DESC
