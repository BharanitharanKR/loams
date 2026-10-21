"""HE0 Task 11: DuckDB reads the House's v2 tables and Spark's v3 tables through Lakekeeper.

Run: uv run --no-project --with duckdb==1.5.6 --with pytz python -I duckdb/read.py
"""
import duckdb

con = duckdb.connect()
print("duckdb", con.execute("SELECT version()").fetchone()[0])
for ext in ("httpfs", "iceberg"):
    con.execute(f"INSTALL {ext}; LOAD {ext};")
print("iceberg extension", con.execute(
    "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'iceberg'").fetchone()[0])
con.execute("""CREATE SECRET s3 (TYPE S3, KEY_ID 'spikeadmin', SECRET 'spikeadmin-secret',
  ENDPOINT '127.0.0.1:19000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')""")
for alias, wh in (("h", "lakehouse"), ("s", "spark")):
    con.execute(f"""ATTACH '{wh}' AS {alias} (TYPE ICEBERG, ENDPOINT 'http://127.0.0.1:18181/catalog',
      AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')""")


def q(label, sql):
    try:
        rows = con.execute(sql).fetchall()
        print(label, rows if len(rows) != 1 else rows[0])
    except Exception as err:  # the point is to record what fails
        print(label, "FAILED:", str(err).splitlines()[0][:300])


q("schemas", "SELECT database_name, schema_name FROM duckdb_schemas() WHERE database_name IN ('h','s') ORDER BY 1, 2")
q("tables", "SELECT database_name, schema_name, table_name FROM duckdb_tables() WHERE database_name IN ('h','s') ORDER BY 1, 2, 3")
# The House's nested namespace ns_1.base.
for name in ('h."ns_1.base".events', 'h.ns_1.base.events'):
    q(f"events via {name}", f"SELECT count(*), sum(user_id), count(*) FILTER (WHERE maybe IS NULL), min(ts), max(ts), sum(u32), sum(small), sum(big) FROM {name}")
q("events types", """SELECT column_name, data_type FROM duckdb_columns()
  WHERE database_name = 'h' AND table_name = 'events' ORDER BY column_index""")
q("events rows", 'SELECT * FROM h."ns_1.base".events ORDER BY user_id, ts LIMIT 2')
q("events pruned", "SELECT count(*) FROM h.\"ns_1.base\".events WHERE ts >= TIMESTAMPTZ '2026-10-01 00:00:00+00'")
q("latest", 'SELECT count(*), sum(id), sum(is_deleted) FROM h."ns_1.base".latest')
q("mutated", 'SELECT count(*) FROM h."ns_1.base".mutated')
q("compacted (upgraded to v3)", 'SELECT count(*), sum(id) FROM h."ns_1.base".compacted')
# Spark's v3 tables.
q("v3.dv", "SELECT count(*), sum(id), count(*) FILTER (WHERE v = 'u') FROM s.v3.dv")
q("v3.dv_part", "SELECT count(*), sum(id), count(*) FILTER (WHERE v = 'm') FROM s.v3.dv_part")
q("v3.lineage", "SELECT count(*), sum(id), count(*) FILTER (WHERE v = 'c') FROM s.v3.lineage")
q("v3.lineage cols", "SELECT id, v, _row_id, _last_updated_sequence_number FROM s.v3.lineage WHERE id IN (0, 3, 13, 49, 50, 53, 99) ORDER BY id")
q("v3.lineage_cow cols", "SELECT id, v, _row_id, _last_updated_sequence_number FROM s.v3.lineage_cow WHERE id IN (0, 3, 13, 49, 50, 53, 99) ORDER BY id")
q("v3.lineage row ids distinct", "SELECT count(*), count(DISTINCT _row_id), min(_row_id), max(_row_id) FROM s.v3.lineage")
q("v3.variant", "SELECT id, payload::VARCHAR FROM s.v3.variant ORDER BY id")
q("v3.posdel_v2", "SELECT count(*), sum(id), count(*) FILTER (WHERE v = 'u') FROM s.v3.posdel_v2")
q("v3.upgraded", "SELECT count(*), sum(id) FROM s.v3.upgraded")
q("v3.rust_append lineage", "SELECT v, count(*), min(_row_id), max(_row_id) FROM s.v3.rust_append GROUP BY v ORDER BY v")
q("v3.lineage _row_id <> id", "SELECT count(*) FILTER (WHERE _row_id <> id), count(*) FILTER (WHERE _row_id <> id AND v <> 'c') FROM s.v3.lineage")
q("v3.dv _row_id <> id", "SELECT count(*) FILTER (WHERE _row_id <> id), count(*) FROM s.v3.dv")
# The filtered scans below are where 1.5.6's _row_id goes wrong (see the report).
q("v3.lineage filtered id=49", "SELECT id, _row_id FROM s.v3.lineage WHERE id = 49")
q("v3.lineage filtered id>=40", "SELECT count(*) FILTER (WHERE _row_id <> id) FROM s.v3.lineage WHERE id >= 40")
q("v3.dv filtered id>=500", "SELECT count(*) FILTER (WHERE _row_id <> id), count(*) FROM s.v3.dv WHERE id >= 500")
q("v3.lineage_cow filtered id>=40", "SELECT count(*) FILTER (WHERE _row_id <> id) FROM s.v3.lineage_cow WHERE id >= 40")
q("v3.posdel_v2 has no lineage", "SELECT count(*) FROM s.v3.posdel_v2")
# The same lineage fingerprints Spark and iceberg-rust main print.
for label, table, where in (("lineage>=40", "lineage", "id >= 40"), ("dv>=500", "dv", "id >= 500"),
                            ("dv_part>=1000", "dv_part", "id >= 1000"), ("upgraded>=500", "upgraded", "id >= 500"),
                            ("dv_part all", "dv_part", "true"), ("upgraded all", "upgraded", "true")):
    q(f"fp {label}", f"SELECT count(*), sum(_row_id), sum(_row_id * id), sum(_last_updated_sequence_number) FROM s.v3.{table} WHERE {where}")
