-- HS1 Task 17: mv_corpus_matches_reference. Columns go in by name, missing
-- ones take their defaults, filtered rows never reach the target.
CREATE TABLE events (id UInt64, kind String, amount Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE big (amount Float64, id UInt64, note String DEFAULT 'none', kind LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW big_mv TO big AS SELECT upper(kind) AS kind, id, amount * 2 AS amount FROM events WHERE amount > 10;
INSERT INTO events VALUES (1, 'buy', 5), (2, 'sell', 50.5), (3, 'buy', 11);
INSERT INTO events VALUES (4, 'hold', 0);
SELECT id, kind, amount, note FROM big ORDER BY id
