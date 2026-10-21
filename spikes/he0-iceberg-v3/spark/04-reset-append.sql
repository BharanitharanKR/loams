DROP TABLE IF EXISTS spark.v3.rust_append PURGE;
CREATE TABLE spark.v3.rust_append (id BIGINT, v STRING) USING iceberg
  TBLPROPERTIES ('format-version'='3');
INSERT INTO spark.v3.rust_append SELECT id, 's' FROM range(0, 10);
