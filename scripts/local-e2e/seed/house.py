"""Loams House: NYC TLC yellow taxi trips for January 2024 (about 3M rows,
data/nyc-taxi), loaded over the House's ClickHouse HTTP interface (host port
18123, user `default`, password from the house-data volume's house.key, given
as E2E_HOUSE_PASSWORD), then read back through the engine's loams.house.v1
(the console's path).

- database `nyc`, table `yellow_trips` (MergeTree), loaded with
  `INSERT ... FORMAT Parquet` (the file as the request body);
- table `nyc.payment_types` (a small dimension).

Idempotent: the table is truncated before the load.

    E2E_HOUSE_PASSWORD=... venv/bin/python -I scripts/local-e2e/seed/house.py
"""

from __future__ import annotations

import base64
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import HOUSE, Report, connect, connect_stream, data_path, log, request  # noqa: E402

DDL = [
    "CREATE DATABASE IF NOT EXISTS nyc",
    """CREATE TABLE IF NOT EXISTS nyc.yellow_trips (
        VendorID Int32, tpep_pickup_datetime DateTime64(6), tpep_dropoff_datetime DateTime64(6),
        passenger_count Nullable(Int64), trip_distance Float64, RatecodeID Nullable(Int64),
        store_and_fwd_flag Nullable(String), PULocationID Int32, DOLocationID Int32, payment_type Int64,
        fare_amount Float64, extra Float64, mta_tax Float64, tip_amount Float64, tolls_amount Float64,
        improvement_surcharge Float64, total_amount Float64, congestion_surcharge Nullable(Float64),
        Airport_fee Nullable(Float64)
    ) ENGINE = MergeTree ORDER BY (tpep_pickup_datetime, PULocationID)""",
    "CREATE TABLE IF NOT EXISTS nyc.payment_types (id Int64, name String) ENGINE = MergeTree ORDER BY id",
]


def ch(sql, body=None, params=None):
    pw = os.environ.get("E2E_HOUSE_PASSWORD", "")
    auth = base64.b64encode(f"default:{pw}".encode()).decode()
    from urllib.parse import urlencode
    q = {"query": sql, **(params or {})}
    data = body if body is not None else b""
    status, out = request("POST", f"{HOUSE}/?{urlencode(q)}", data,
                          headers={"authorization": f"Basic {auth}"}, timeout=1800, raw=True)
    if status != 200:
        raise AssertionError(f"House {sql[:60]!r}: {status} {out[:400]!r}")
    return out.decode(errors="replace").strip()


def main():
    report = Report("house")
    report.attempt("ping", lambda: ch("SELECT version()"))
    for sql in DDL:
        report.attempt(f"ddl {sql.split('(')[0][:60]}", lambda sql=sql: ch(sql) or "ok")
    report.attempt("payment types", lambda: ch("TRUNCATE TABLE nyc.payment_types") or ch(
        "INSERT INTO nyc.payment_types VALUES (0,'Flex Fare'),(1,'Credit card'),(2,'Cash'),(3,'No charge'),"
        "(4,'Dispute'),(5,'Unknown'),(6,'Voided trip')") or "ok")
    path = data_path("nyc-taxi", "yellow_tripdata_2024-01.parquet")
    log(f"loading {path} ({os.path.getsize(path) >> 20} MiB)")
    report.attempt("load yellow_trips (FORMAT Parquet)", lambda: ch("TRUNCATE TABLE nyc.yellow_trips") or ch(
        "INSERT INTO nyc.yellow_trips FORMAT Parquet", open(path, "rb").read(),
        {"input_format_parquet_allow_missing_columns": "1"}) or "ok")
    report.attempt("count", lambda: ch("SELECT count() FROM nyc.yellow_trips"))
    report.attempt("top pickup zones", lambda: ch(
        "SELECT PULocationID, count() c FROM nyc.yellow_trips GROUP BY PULocationID ORDER BY c DESC LIMIT 5 FORMAT TSV"))
    report.attempt("daily revenue", lambda: ch(
        "SELECT toDate(tpep_pickup_datetime) AS day, count() AS trips, round(sum(total_amount), 2) AS revenue "
        "FROM nyc.yellow_trips GROUP BY day ORDER BY day LIMIT 3 FORMAT TSV"))
    # CREATE VIEW is not on the House's surface (501 NOT_IMPLEMENTED); a SELECT of a
    # view that is not there answers UNKNOWN_DATABASE for a database that exists.
    # The console's path: the engine's loams.house.v1, as dev-loopback.
    def via_engine():
        msgs, end = connect_stream("loams.house.v1.HouseService/ExecuteQuery", {
            "sql": "SELECT p.name, count() AS trips, round(avg(tip_amount), 2) AS avg_tip FROM nyc.yellow_trips t "
                   "JOIN nyc.payment_types p ON p.id = t.payment_type GROUP BY p.name ORDER BY trips DESC"})
        if end:
            raise AssertionError(end)
        return str(msgs)[:300]
    report.attempt("engine loams.house.v1 ExecuteQuery", via_engine)
    report.attempt("engine loams.house.v1 ListDatabases/Tables", lambda: str(connect(
        "loams.house.v1.HouseService/ListTables", {"database": "nyc"}))[:300])
    return report.done()


if __name__ == "__main__":
    sys.exit(main())
