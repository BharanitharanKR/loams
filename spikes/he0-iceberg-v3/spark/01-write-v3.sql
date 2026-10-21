-- HE0 Task 11: v3 tables written by unmodified Spark 4.0.4 + Iceberg 1.12.0.
CREATE NAMESPACE IF NOT EXISTS spark.v3;

-- 1. Deletion vectors: merge-on-read deletes and an update on an unpartitioned table.
CREATE TABLE spark.v3.dv (id BIGINT, v STRING) USING iceberg
  TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read',
                 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read');
INSERT INTO spark.v3.dv SELECT id, concat('v', id) FROM range(0, 1000);
DELETE FROM spark.v3.dv WHERE id % 10 = 0;
DELETE FROM spark.v3.dv WHERE id % 10 = 1;
UPDATE spark.v3.dv SET v = 'u' WHERE id % 10 = 2;

-- 2. Deletion vectors on a partitioned table, several data files per partition.
CREATE TABLE spark.v3.dv_part (id BIGINT, p INT, v STRING) USING iceberg PARTITIONED BY (p)
  TBLPROPERTIES ('format-version'='3', 'write.delete.mode'='merge-on-read',
                 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read');
INSERT INTO spark.v3.dv_part SELECT id, CAST(id % 4 AS INT), concat('v', id) FROM range(0, 2000);
INSERT INTO spark.v3.dv_part SELECT id, CAST(id % 4 AS INT), concat('w', id) FROM range(2000, 3000);
DELETE FROM spark.v3.dv_part WHERE id % 7 = 0;
MERGE INTO spark.v3.dv_part t USING (SELECT id, CAST(id % 4 AS INT) AS p FROM range(0, 3000, 5)) s
  ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = 'm';

-- 3. Row lineage: two appends, then an update; lineage must survive the update.
CREATE TABLE spark.v3.lineage (id BIGINT, v STRING) USING iceberg
  TBLPROPERTIES ('format-version'='3', 'write.update.mode'='merge-on-read',
                 'write.delete.mode'='merge-on-read');
INSERT INTO spark.v3.lineage SELECT id, 'a' FROM range(0, 50);
INSERT INTO spark.v3.lineage SELECT id, 'b' FROM range(50, 100);
UPDATE spark.v3.lineage SET v = 'c' WHERE id % 10 = 3;

-- 4. Variant.
CREATE TABLE spark.v3.variant (id BIGINT, payload VARIANT) USING iceberg
  TBLPROPERTIES ('format-version'='3');
INSERT INTO spark.v3.variant VALUES
  (1, parse_json('{"a": 1, "b": "x"}')),
  (2, parse_json('[1, 2, 3]')),
  (3, parse_json('"just a string"')),
  (4, parse_json('{"nested": {"k": [true, null, 2.5]}}')),
  (5, NULL);

-- 5. Control: the same deletes on a v2 table (position delete files).
CREATE TABLE spark.v3.posdel_v2 (id BIGINT, v STRING) USING iceberg
  TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read',
                 'write.update.mode'='merge-on-read');
INSERT INTO spark.v3.posdel_v2 SELECT id, concat('v', id) FROM range(0, 1000);
DELETE FROM spark.v3.posdel_v2 WHERE id % 10 = 0;
UPDATE spark.v3.posdel_v2 SET v = 'u' WHERE id % 10 = 2;

-- 6. Upgrade in place (D1551's path): a v2 table with a position delete, upgraded
-- to v3, then deleted from again (the first deletion vector merges the v2 deletes).
CREATE TABLE spark.v3.upgraded (id BIGINT, v STRING) USING iceberg
  TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read');
INSERT INTO spark.v3.upgraded SELECT id, concat('v', id) FROM range(0, 1000);
DELETE FROM spark.v3.upgraded WHERE id % 10 = 0;
ALTER TABLE spark.v3.upgraded SET TBLPROPERTIES ('format-version'='3');
INSERT INTO spark.v3.upgraded SELECT id, concat('v', id) FROM range(1000, 1100);
DELETE FROM spark.v3.upgraded WHERE id % 10 = 1;

-- 7. Default values (v3 initial-default / write-default), if this Spark accepts them.
CREATE TABLE spark.v3.defaults (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='3');
INSERT INTO spark.v3.defaults SELECT id FROM range(0, 10);
ALTER TABLE spark.v3.defaults ADD COLUMN d INT DEFAULT 7;
INSERT INTO spark.v3.defaults (id) VALUES (10);
