-- HS1 Task 11: differential_select_corpus_on_lake
SELECT g, arraySlice(arraySort(groupArray(k)), 1, 3) FROM t GROUP BY g ORDER BY g
