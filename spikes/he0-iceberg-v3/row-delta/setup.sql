-- HE0 G3: fresh copies of the spike's DV tables (spark/01-write-v3.sql 1, 2, 5 and 6) in
-- namespace rdw, so the harness can run again from the same state.
CREATE NAMESPACE IF NOT EXISTS spark.rdw;
CREATE OR REPLACE TABLE spark.rdw.dv (id BIGINT, v STRING) USING iceberg
  TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read',
                 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read');
INSERT INTO spark.rdw.dv SELECT id, concat('v', id) FROM range(0, 1000);
DELETE FROM spark.rdw.dv WHERE id % 10 = 0;
DELETE FROM spark.rdw.dv WHERE id % 10 = 1;
UPDATE spark.rdw.dv SET v = 'u' WHERE id % 10 = 2;
CREATE OR REPLACE TABLE spark.rdw.dv_part (id BIGINT, p INT, v STRING) USING iceberg PARTITIONED BY (p)
  TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read',
                 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read');
INSERT INTO spark.rdw.dv_part SELECT id, CAST(id % 4 AS INT), concat('v', id) FROM range(0, 2000);
INSERT INTO spark.rdw.dv_part SELECT id, CAST(id % 4 AS INT), concat('w', id) FROM range(2000, 3000);
DELETE FROM spark.rdw.dv_part WHERE id % 7 = 0;
MERGE INTO spark.rdw.dv_part t USING (SELECT id, CAST(id % 4 AS INT) AS p FROM range(0, 3000, 5)) s
  ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = 'm';
CREATE OR REPLACE TABLE spark.rdw.upgraded (id BIGINT, v STRING) USING iceberg
  TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read');
INSERT INTO spark.rdw.upgraded SELECT id, concat('v', id) FROM range(0, 1000);
DELETE FROM spark.rdw.upgraded WHERE id % 10 = 0;
ALTER TABLE spark.rdw.upgraded SET TBLPROPERTIES ('format-version'='3');
INSERT INTO spark.rdw.upgraded SELECT id, concat('v', id) FROM range(1000, 1100);
DELETE FROM spark.rdw.upgraded WHERE id % 10 = 1;
CREATE OR REPLACE TABLE spark.rdw.pos_v2 (id BIGINT, v STRING) USING iceberg
  TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read',
                 'write.update.mode'='merge-on-read');
INSERT INTO spark.rdw.pos_v2 SELECT id, concat('v', id) FROM range(0, 1000);
DELETE FROM spark.rdw.pos_v2 WHERE id % 10 = 0;
UPDATE spark.rdw.pos_v2 SET v = 'u' WHERE id % 10 = 2;
