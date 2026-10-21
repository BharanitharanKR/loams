-- A v2 table for iceberg-rust to upgrade to v3 and then append to.
CREATE TABLE spark.v3.rust_upgrade (id BIGINT, v STRING) USING iceberg TBLPROPERTIES ('format-version'='2');
INSERT INTO spark.v3.rust_upgrade SELECT id, 'old' FROM range(0, 20);
