"""Loams SQL (TiDB) and Loams Postgres, with the Chinook sample database
(lerocha/chinook-database 1.4.5, data/chinook): 11 tables, about 15k rows.

- TiDB (host port 14000, root without a password, as the release stack
  starts it): database `Chinook` from Chinook_MySql.sql;
- Loams Postgres through pg-control (127.0.0.1:7680, the API key up.sh minted
  in $E2E_HOME/pg-api-key): project `chinook` (Postgres 17), a read-write
  endpoint, role `app`, database `chinook` from Chinook_PostgreSql.sql. The
  role's password is written to $E2E_HOME/pg-chinook.env (mode 0600), never
  printed.

The SQL files are data: they are split into statements and sent through the
drivers (PyMySQL, psycopg), never through a client that runs meta-commands,
and a file with a client meta-command or a server-side file or program
statement is refused.

    venv/bin/python -I scripts/local-e2e/seed/relational.py [--only tidb|postgres]
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import PG_CONTROL, TIDB, HttpError, Report, connect, data_path, log  # noqa: E402

HOME = os.environ.get("E2E_HOME", "/mnt/Projects/loams-e2e")
UNSAFE = re.compile(r"(?im)^\s*(\\!|system\s|source\s)|\bLOAD\s+DATA\b|\bINTO\s+(OUT|DUMP)FILE\b|"
                    r"\bCOPY\b[^;]*\bPROGRAM\b|\blo_import\b|\bpg_read_file\b")


def statements(path, drop=()):
    text = open(path, encoding="utf-8-sig").read()
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    lines = [ln for ln in lines if not any(re.match(d, ln.strip(), re.I) for d in drop)]
    text = "\n".join(lines)
    bad = UNSAFE.search(text)
    if bad:
        raise SystemExit(f"{path}: refusing to load, it contains {bad.group(0)!r}")
    return [s.strip() for s in re.split(r";\s*\n", text) if s.strip()]


def tidb(report):
    import pymysql
    host, port = TIDB.split(":")
    stmts = statements(data_path("chinook", "Chinook_MySql.sql"))
    conn = pymysql.connect(host=host, port=int(port), user="root", password="", autocommit=True,
                           connect_timeout=15, charset="utf8mb4")
    with conn, conn.cursor() as cur:
        cur.execute("SELECT tidb_version()")
        report.add("tidb: version", True, cur.fetchone()[0].splitlines()[0])
        t0 = time.time()
        for s in stmts:
            cur.execute(s)
        report.add("tidb: load Chinook", True, f"{len(stmts)} statements in {time.time() - t0:.0f}s")
        cur.execute("SELECT table_name, table_rows FROM information_schema.tables WHERE table_schema='Chinook'")
        report.add("tidb: tables", True, cur.fetchall())
        cur.execute("SELECT g.Name, count(*) n FROM Chinook.Track t JOIN Chinook.Genre g USING (GenreId) "
                    "GROUP BY g.Name ORDER BY n DESC LIMIT 5")
        report.add("tidb: tracks per genre", True, cur.fetchall())


def pg_call(method, body, accept=(200,)):
    key = open(os.path.join(HOME, "pg-api-key")).read().strip()
    from common import request
    status, reply = request("POST", f"{PG_CONTROL}/loams.postgres.v1.PostgresService/{method}", body,
                            headers={"authorization": f"Bearer {key}", "connect-protocol-version": "1"})
    if status not in accept:
        raise HttpError(status, reply)
    return reply


def until(what, fn, within=600, every=3):
    deadline = time.time() + within
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(every)
    raise AssertionError(f"timed out waiting for {what}")


def postgres(report):
    ns = "default"
    projects = pg_call("ListProjects", {"namespace": ns}).get("projects", [])
    project = next((p for p in projects if p.get("name") == "chinook"), None)
    if not project:
        reply = pg_call("CreateProject", {"namespace": ns, "name": "chinook", "pgVersion": 17,
                                          "idempotencyKey": "e2e-chinook"})
        project = reply.get("project") or {}
        report.add("pg: CreateProject chinook", True, f"{project.get('id')} {reply.get('operation', {}).get('name')}")
    pid = project["id"]
    project = until("the project is ready", lambda: (lambda p: p if p.get("state") == "PROJECT_STATE_READY" else None)(
        pg_call("GetProject", {"namespace": ns, "projectId": pid}).get("project", {})))
    branch = project["defaultBranchId"]
    report.add("pg: project ready", True, f"{pid} branch {branch}")
    eps = pg_call("ListEndpoints", {"namespace": ns, "projectId": pid}).get("endpoints", [])
    ep = next((e for e in eps if e.get("branchId") == branch and e.get("type") == "ENDPOINT_TYPE_READ_WRITE"), None)
    if not ep:
        ep = pg_call("CreateEndpoint", {"namespace": ns, "projectId": pid, "branchId": branch,
                                        "type": "ENDPOINT_TYPE_READ_WRITE", "minCu": 0.25, "maxCu": 1,
                                        "idempotencyKey": "e2e-chinook-rw"}).get("endpoint", {})
        report.add("pg: CreateEndpoint", True, ep.get("id"))
    env = os.path.join(HOME, "pg-chinook.env")
    if not os.path.exists(env):
        roles = [r.get("name") for r in pg_call("ListRoles", {"namespace": ns, "projectId": pid, "branchId": branch}).get("roles", [])]
        if "app" in roles:
            password = pg_call("ResetRolePassword", {"namespace": ns, "projectId": pid, "branchId": branch,
                                                     "name": "app"}).get("password")
        else:
            password = pg_call("CreateRole", {"namespace": ns, "projectId": pid, "branchId": branch, "name": "app",
                                              "idempotencyKey": "e2e-chinook-app"}).get("password")
        fd = os.open(env, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.write(fd, f"PGPASSWORD={password}\n".encode())
        os.close(fd)
    password = open(env).read().split("=", 1)[1].strip()
    dbs = [d.get("name") for d in pg_call("ListDatabases", {"namespace": ns, "projectId": pid, "branchId": branch}).get("databases", [])]
    if "chinook" not in dbs:
        pg_call("CreateDatabase", {"namespace": ns, "projectId": pid, "branchId": branch, "name": "chinook",
                                   "ownerRole": "app", "idempotencyKey": "e2e-chinook-db"})
    info = pg_call("GetConnectionInfo", {"namespace": ns, "projectId": pid, "branchId": branch,
                                         "role": "app", "database": "chinook"}).get("connection", {})
    report.add("pg: GetConnectionInfo", True, {k: v for k, v in info.items() if k != "caPem"})
    ca = os.path.join(HOME, "pg-chinook-ca.pem")
    if info.get("caPem"):
        open(ca, "w").write(info["caPem"])
    import psycopg
    def dial():
        try:
            return psycopg.connect(host="127.0.0.1", port=int(info["port"]), user=info.get("user", "app"),
                                   password=password, dbname="chinook", sslmode="require", connect_timeout=10,
                                   autocommit=True)
        except psycopg.OperationalError as e:
            log("waiting for the compute:", str(e).splitlines()[0])
            return None
    conn = until("the compute accepts connections", dial, within=600, every=5)
    with conn:
        have = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='track'").fetchone()[0]
        if not have:
            stmts = statements(data_path("chinook", "Chinook_PostgreSql.sql"),
                               drop=(r"DROP DATABASE", r"CREATE DATABASE", r"\\c\b"))
            t0 = time.time()
            for s in stmts:
                conn.execute(s)
            report.add("pg: load Chinook", True, f"{len(stmts)} statements in {time.time() - t0:.0f}s")
        report.add("pg: track count", True, conn.execute("SELECT count(*) FROM track").fetchone()[0])
        report.add("pg: revenue per country", True, conn.execute(
            "SELECT billing_country, sum(total) FROM invoice GROUP BY 1 ORDER BY 2 DESC LIMIT 5").fetchall())
    report.add("pg: branches", True, [b.get("name") for b in pg_call(
        "ListBranches", {"namespace": ns, "projectId": pid}).get("branches", [])])


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--only", choices=["tidb", "postgres"])
    a = p.parse_args()
    report = Report("relational")
    if a.only in (None, "tidb"):
        report.attempt("tidb", lambda: tidb(report) or "done")
    if a.only in (None, "postgres"):
        report.attempt("postgres", lambda: postgres(report) or "done")
    return report.done()


if __name__ == "__main__":
    sys.exit(main())
