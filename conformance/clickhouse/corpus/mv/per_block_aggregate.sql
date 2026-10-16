-- HS1 Task 17: mv_corpus_matches_reference. One aggregate row per key per
-- insert block, as ClickHouse's views run per block.
CREATE TABLE src (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE agg (k UInt64, c UInt64, s UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv TO agg AS SELECT k, count() AS c, sum(v) AS s FROM src GROUP BY k;
INSERT INTO src VALUES (1, 10), (1, 11), (2, 20);
INSERT INTO src VALUES (1, 12), (3, 30);
INSERT INTO src SELECT number % 4, number FROM numbers(20);
SELECT k, c, s FROM agg ORDER BY k, c, s
