"""HE0 G3: DuckDB 2.0.0 reads the tables after iceberg-rust's RowDelta commit.

Run: uv run --no-project --with duckdb==2.0.0 python -I duckdb_after.py
"""
import duckdb

con = duckdb.connect()
for ext in ("httpfs", "iceberg"):
    con.execute(f"INSTALL {ext}; LOAD {ext};")
version = con.execute("SELECT version()").fetchone()[0]
ext = con.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'iceberg'").fetchone()[0]
print(f"duckdb {version} iceberg {ext}")
con.execute("""CREATE SECRET s3 (TYPE S3, KEY_ID 'spikeadmin', SECRET 'spikeadmin-secret',
  ENDPOINT '127.0.0.1:19000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')""")
con.execute("""ATTACH 'spark' AS s (TYPE ICEBERG, ENDPOINT 'http://127.0.0.1:18181/catalog',
  AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')""")
for name, minimum in (("dv", 500), ("dv_part", 1000), ("upgraded", 500), ("pos_v2", 500)):
    lineage = "sum(_row_id), sum(_row_id * id)" if name != "pos_v2" else "0, 0"
    for label, where in ((name, ""), (f"{name}>={minimum}", f" WHERE id >= {minimum}")):
        row = con.execute(f"SELECT count(*), sum(id), {lineage} FROM s.rdw.{name}{where}").fetchone()
        print("duckdb " + label + "\t" + "\t".join(str(v) for v in row))
