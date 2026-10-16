-- NF1 Task 16, G3's extension smoke on a release candidate's compute
-- (scripts/nf1/candidate-smoke.sh), on deploy/loams-postgres-dev's compute
-- spec. The fork's own image smoke (compute/tests/loams-smoke/core-extensions.sql
-- in ostrium-labs/loams-postgres) creates every core extension with its
-- preload spec; this one covers those that need no shared_preload_libraries
-- (so not pgaudit, pg_cron or pg_session_jwt), on the stack Loams runs.
\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS neon;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
CREATE EXTENSION IF NOT EXISTS hypopg;
CREATE EXTENSION IF NOT EXISTS pg_hint_plan;
CREATE EXTENSION IF NOT EXISTS pg_repack;
CREATE EXTENSION IF NOT EXISTS pg_ivm;

-- A query through each of the bigger ones.
CREATE TABLE candidate_items (id int PRIMARY KEY, embedding vector(3), place geometry(Point, 4326), name text);
INSERT INTO candidate_items VALUES
  (1, '[1,2,3]', ST_SetSRID(ST_MakePoint(0, 0), 4326), 'first'),
  (2, '[3,2,1]', ST_SetSRID(ST_MakePoint(1, 1), 4326), 'second');
DO $$
BEGIN
  IF (SELECT id FROM candidate_items ORDER BY embedding <-> '[1,2,4]' LIMIT 1) <> 1 THEN
    RAISE EXCEPTION 'pgvector: wrong nearest neighbour';
  END IF;
  IF (SELECT round(ST_Distance(a.place::geography, b.place::geography)) FROM candidate_items a, candidate_items b
      WHERE a.id = 1 AND b.id = 2) NOT BETWEEN 150000 AND 160000 THEN
    RAISE EXCEPTION 'PostGIS: wrong distance';
  END IF;
  IF similarity('second', 'secnod') <= 0 THEN
    RAISE EXCEPTION 'pg_trgm: no similarity';
  END IF;
  IF length(digest('loams', 'sha256')) <> 32 THEN
    RAISE EXCEPTION 'pgcrypto: wrong digest length';
  END IF;
END $$;

-- wal2json is an output plugin: decode one change with it (wal_level is logical in the spec).
SELECT 'slot' FROM pg_create_logical_replication_slot('candidate_smoke', 'wal2json');
INSERT INTO candidate_items VALUES (3, '[0,0,1]', NULL, 'third');
DO $$
BEGIN
  IF (SELECT count(*) FROM pg_logical_slot_get_changes('candidate_smoke', NULL, NULL)
      WHERE data LIKE '%candidate_items%') <> 1 THEN
    RAISE EXCEPTION 'wal2json did not decode the insert';
  END IF;
END $$;
SELECT pg_drop_replication_slot('candidate_smoke');
