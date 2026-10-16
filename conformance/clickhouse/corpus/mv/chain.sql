-- HS1 Task 17: mv_corpus_matches_reference. A view of a view's target runs
-- on the rows the first view wrote, per block.
CREATE TABLE raw (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE doubled (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE totals (k UInt64, total UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW m1 TO doubled AS SELECT k, v * 2 AS v FROM raw;
CREATE MATERIALIZED VIEW m2 TO totals AS SELECT k, sum(v) AS total FROM doubled GROUP BY k;
INSERT INTO raw VALUES (1, 1), (1, 2), (2, 3);
INSERT INTO raw VALUES (2, 4);
SELECT k, total FROM totals ORDER BY k, total
