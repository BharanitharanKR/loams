#!/usr/bin/env python3
"""Replay an application's oracle traffic against a House and diff the answers.

HE0 Task 9 (plan HE1, ruling R9.3). After `scripts/house/apps.sh <app> up` and
`drive` on the pinned clickhouse-server (the oracle), this reads that server's
`system.query_log` and, against a running House:

  ddl     runs every finished statement that is not a SELECT or an INSERT
          (CREATE, ALTER, RENAME, DROP, ...), in the order the application
          sent them;
  data    copies each base table's rows from the oracle, one INSERT per
          part, oldest block first and in part order, so ties that ClickHouse
          breaks by insert order break the same way on the House and rows
          ClickHouse has not merged stay unmerged;
  select  runs every distinct finished SELECT on both and compares the
          answers (FORMAT JSON: column names and types, then rows; rows as a
          multiset when the statement has no ORDER BY; the same rows in
          another order under an ORDER BY are listed as `order-ties`; a read
          of a merging table without FINAL that differs is run again with
          `final = 1` on both and listed as `merge-timing` when that agrees,
          a mismatch otherwise).

Each statement keeps the settings the application sent. A setting the House
refuses (115, 164) is recorded and dropped, and the statement runs again, so
one refused setting does not hide the next gap. Writes a JSON report and
prints a summary; exits 1 when any statement errs or any answer differs.

The traffic is synthetic (the harness's drivers): no user's queries are read.
The report holds query text and answers, so it stays under the harness's
`.out/` and is never committed.

    scripts/house/replay.py --oracle http://127.0.0.1:8123 --database openpanel \\
        --house https://127.0.0.1:8443 --house-user u --out report.json

The House password comes from HE0_HOUSE_PASSWORD; the oracle's from
HE0_ORACLE_PASSWORD (default empty). `--strip` drops DDL clauses the House
does not take yet (skip indexes, TTL), for finding the gaps behind them; a
run with it is discovery, never a pass. Stdlib only.
"""

from __future__ import annotations

import argparse
import base64
import collections
import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

SKIP_SETTINGS = {"log_queries", "log_queries_min_type", "database", "log_comment"}
REFUSED = (115, 164)


class Server:
    def __init__(self, url: str, user: str, password: str, database: str, insecure: bool):
        self.url = url.rstrip("/") + "/"
        self.database = database
        self.auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
        self.context = ssl._create_unverified_context() if insecure else None

    def post(self, sql: str, params=None, body: bytes | None = None, timeout=300):
        query = {"database": self.database, **(params or {})}
        data = sql.encode() if body is None else sql.encode() + b"\n" + body
        req = urllib.request.Request(self.url + "?" + urllib.parse.urlencode(query), data=data, method="POST")
        req.add_header("authorization", self.auth)
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=self.context) as resp:
                return 200, resp.read().decode(errors="replace"), {k.lower(): v for k, v in resp.headers.items()}
        except urllib.error.HTTPError as err:
            return err.code, err.read().decode(errors="replace"), {k.lower(): v for k, v in err.headers.items()}

    def must(self, sql: str, params=None) -> str:
        status, body, _ = self.post(sql, params)
        if status != 200:
            raise SystemExit(f"replay: the oracle refused {sql[:120]!r}: {body[:300]}")
        return body


def code_of(body: str, headers: dict) -> int:
    if "x-clickhouse-exception-code" in headers:
        return int(headers["x-clickhouse-exception-code"])
    m = re.search(r"Code: (\d+)", body)
    return int(m.group(1)) if m else -1


def run(server: Server, sql: str, settings: dict, body: bytes | None = None):
    """Runs `sql`, dropping each setting the server refuses; returns the
    final status, body, headers and the refused settings."""
    refused: list[str] = []
    status, text, headers = server.post(sql, settings, body)
    while status != 200 and code_of(text, headers) in REFUSED and settings:
        named = [k for k in settings if f"'{k}'" in text or f"`{k}`" in text]
        bad = named or list(settings)
        refused += bad
        settings = {k: v for k, v in settings.items() if k not in bad}
        status, text, headers = server.post(sql, settings, body)
    return status, text, headers, refused


def strip(sql: str) -> str:
    sql = re.sub(r"^\s*INDEX\s+\S+\s+.*?GRANULARITY\s+\d+\s*,?\s*$", "", sql, flags=re.M | re.I)
    sql = re.sub(r",(\s*)\)(\s*)ENGINE", r"\1)\2ENGINE", sql)
    sql = re.sub(r"^\s*TTL\s+[^\n]+$", "", sql, flags=re.M)
    return sql


def without_format(sql: str) -> str:
    return re.sub(r"\s+FORMAT\s+\w+\s*;?\s*$", "", sql.strip(), flags=re.I)


def answer(text: str, ordered: bool):
    try:
        doc = json.loads(text)
    except ValueError:
        return {"raw": text}
    rows = doc.get("data")
    if not ordered and isinstance(rows, list):
        rows = sorted(rows, key=lambda r: json.dumps(r, sort_keys=True))
    return {"meta": doc.get("meta"), "data": rows}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--oracle", required=True)
    ap.add_argument("--oracle-user", default="default")
    ap.add_argument("--house", required=True)
    ap.add_argument("--house-user", default="default")
    ap.add_argument("--database", required=True)
    ap.add_argument("--insecure", action="store_true", help="skip TLS verification of the House")
    ap.add_argument("--strip", action="store_true", help="discovery: drop skip indexes and TTL")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    oracle = Server(args.oracle, args.oracle_user, os.environ.get("HE0_ORACLE_PASSWORD", ""), args.database, False)
    house = Server(args.house, args.house_user, os.environ.get("HE0_HOUSE_PASSWORD", ""), args.database,
                   args.insecure)

    oracle.must("SYSTEM FLUSH LOGS", {"log_comment": "he0-harness"})
    log = oracle.must(
        "SELECT query_kind, query, Settings FROM system.query_log "
        "WHERE type = 'QueryFinish' AND is_initial_query AND log_comment != 'he0-harness' "
        "AND (current_database = {db:String} OR query ILIKE 'CREATE DATABASE%') "
        "ORDER BY event_time_microseconds FORMAT JSONEachRow",
        {"param_db": args.database, "log_comment": "he0-harness"})
    statements = [json.loads(line) for line in log.splitlines() if line.strip()]
    report = {"ddl": [], "data": [], "select": [], "refused_settings": collections.Counter(),
              "strip": args.strip}

    def settings(row):
        return {k: v for k, v in row["Settings"].items() if k not in SKIP_SETTINGS}

    for row in statements:
        sql, kind = row["query"], row["query_kind"]
        head = sql.lstrip().upper()
        if kind in ("Select", "Insert") or head.startswith(("SELECT", "WITH", "INSERT", "SYSTEM")):
            continue
        status, text, headers, refused = run(house, strip(sql) if args.strip else sql, settings(row))
        report["refused_settings"].update(refused)
        report["ddl"].append({"query": sql, "status": status, "refused_settings": refused,
                              "code": None if status == 200 else code_of(text, headers),
                              "error": None if status == 200 else text[:600]})

    tables = oracle.must(
        "SELECT name FROM system.tables WHERE database = {db:String} AND engine NOT IN "
        "('MaterializedView', 'View') AND NOT startsWith(name, '.inner') AND total_rows > 0 "
        "ORDER BY name FORMAT TSV", {"param_db": args.database, "log_comment": "he0-harness"}).split()
    for table in tables:
        # One INSERT per oracle part, oldest block first, rows in part order:
        # the House writes each as ClickHouse wrote that part (merged within
        # it, R9.4), so unmerged versions stay unmerged, as on the oracle.
        parts = oracle.must(
            "SELECT name FROM system.parts WHERE database = {db:String} AND table = {t:String} "
            "AND active ORDER BY min_block_number, partition_id FORMAT TSV",
            {"param_db": args.database, "param_t": table, "log_comment": "he0-harness"}).split()
        copied, status, text = 0, 200, ""
        for part in parts:
            rows = oracle.must(
                f"SELECT * FROM `{table}` WHERE _part = {{part:String}} ORDER BY _part_offset "
                f"FORMAT JSONEachRow", {"param_part": part, "log_comment": "he0-harness"}).encode()
            copied += rows.count(b"\n")
            status, text, _ = house.post(f"INSERT INTO `{table}` FORMAT JSONEachRow", {}, rows)
            if status != 200:
                break
        report["data"].append({"table": table, "rows": copied, "parts": len(parts), "status": status,
                               "error": None if status == 200 else text[:600]})

    merging_tables = oracle.must(
        "SELECT name FROM system.tables WHERE database = {db:String} AND match(engine, "
        "'(Replacing|Summing|Collapsing|Aggregating)MergeTree|MaterializedView') FORMAT TSV",
        {"param_db": args.database, "log_comment": "he0-harness"}).split()

    def merged_answers(text_sql, st, ordered):
        merged = {**st, "final": "1"}
        so, to, _ = oracle.post(text_sql, {**merged, "log_comment": "he0-harness"})
        sh, th, _, _ = run(house, text_sql, merged)
        return so == 200 and sh == 200 and answer(to, ordered) == answer(th, ordered)

    seen = set()
    for row in statements:
        sql = row["query"]
        if row["query_kind"] != "Select" or re.search(r"\bsystem\.", sql, re.I):
            continue
        key = re.sub(r"\s+", " ", sql)
        if key in seen:
            continue
        seen.add(key)
        text_sql = without_format(sql) + "\nFORMAT JSON"
        ordered = re.search(r"\bORDER\s+BY\b", re.sub(r"\([^()]*\)", "", sql), re.I) is not None
        st = {**settings(row), "output_format_json_quote_64bit_integers": "1"}
        merging = any(re.search(rf"\b{re.escape(t)}\b", sql) for t in merging_tables)
        so, to, _ = oracle.post(text_sql, {**st, "log_comment": "he0-harness"})
        sh, th, hh, refused = run(house, text_sql, st)
        report["refused_settings"].update(refused)
        entry = {"query": sql, "refused_settings": refused}
        if so != 200:
            entry.update(verdict="oracle-error", error=to[:300])
        elif sh != 200:
            entry.update(verdict="error", code=code_of(th, hh), error=th[:600])
        elif answer(to, ordered) == answer(th, ordered):
            entry["verdict"] = "match"
        elif ordered and answer(to, False) == answer(th, False):
            # The same rows in another order: an ORDER BY with ties (listed,
            # not failed; a wrong order on distinct keys still shows here).
            entry["verdict"] = "order-ties"
        elif merging and merged_answers(text_sql, st, ordered):
            # Reads of a merging table (Replacing, Summing, Collapsing,
            # Aggregating) without FINAL see whatever each server's background
            # merges have done so far; with `final = 1` on both they must agree.
            entry["verdict"] = "merge-timing"
        else:
            entry.update(verdict="mismatch", oracle=answer(to, ordered), house=answer(th, ordered))
        report["select"].append(entry)

    report["refused_settings"] = dict(report["refused_settings"])
    with open(args.out, "w") as f:
        json.dump(report, f, indent=1)
    ddl = collections.Counter("ok" if e["status"] == 200 else f"code {e['code']}" for e in report["ddl"])
    sel = collections.Counter(e["verdict"] for e in report["select"])
    print(f"replay: ddl {dict(ddl)}")
    print(f"replay: data {[(e['table'], e['status']) for e in report['data']]}")
    print(f"replay: select {dict(sel)}")
    print(f"replay: refused settings {report['refused_settings']}")
    failed = (any(e["status"] != 200 for e in report["ddl"] + report["data"])
              or any(e["verdict"] in ("error", "mismatch") for e in report["select"]))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
