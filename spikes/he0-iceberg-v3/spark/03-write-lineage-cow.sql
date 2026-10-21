-- Row lineage without delete files: copy-on-write update, so the rewritten
-- file carries _row_id and _last_updated_sequence_number physically.
CREATE TABLE spark.v3.lineage_cow (id BIGINT, v STRING) USING iceberg
  TBLPROPERTIES ('format-version'='3', 'write.update.mode'='copy-on-write',
                 'write.delete.mode'='copy-on-write');
INSERT INTO spark.v3.lineage_cow SELECT id, 'a' FROM range(0, 50);
INSERT INTO spark.v3.lineage_cow SELECT id, 'b' FROM range(50, 100);
UPDATE spark.v3.lineage_cow SET v = 'c' WHERE id % 10 = 3;
-- An empty v3 table for iceberg-rust's append probe.
CREATE TABLE spark.v3.rust_append (id BIGINT, v STRING) USING iceberg
  TBLPROPERTIES ('format-version'='3');
INSERT INTO spark.v3.rust_append SELECT id, 's' FROM range(0, 10);
SELECT 'lineage_cow_cols', id, v, _row_id, _last_updated_sequence_number FROM spark.v3.lineage_cow WHERE id IN (0, 3, 13, 49, 50, 53, 99) ORDER BY id;
