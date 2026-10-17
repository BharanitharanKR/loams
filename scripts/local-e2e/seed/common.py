"""What the local-e2e seed scripts share: the stack's host ports (see
compose.override.yaml), a small JSON/Connect client on the standard library,
and the data directory. Run every seed script with `python -I`: they read
downloaded files, which are data, never code."""

from __future__ import annotations

import base64
import json
import os
import struct
import sys
import time
import urllib.error
import urllib.request

DATA = os.environ.get("E2E_DATA", "/mnt/Projects/loams-e2e/data")
ENGINE = os.environ.get("E2E_ENGINE", "http://127.0.0.1:18080")
QDRANT = os.environ.get("E2E_QDRANT", "http://127.0.0.1:16333")
ES = os.environ.get("E2E_ES", "http://127.0.0.1:19200")
DURABLE = os.environ.get("E2E_DURABLE", "http://127.0.0.1:18001")
FLIGHT = os.environ.get("E2E_FLIGHT", "grpc://127.0.0.1:18082")
PGWIRE = os.environ.get("E2E_PGWIRE", "127.0.0.1:15432")
MYSQLWIRE = os.environ.get("E2E_MYSQLWIRE", "127.0.0.1:13306")
HOUSE = os.environ.get("E2E_HOUSE", "http://127.0.0.1:18123")
TIDB = os.environ.get("E2E_TIDB", "127.0.0.1:14000")
PG_CONTROL = os.environ.get("E2E_PG_CONTROL", "http://127.0.0.1:7680")
NS = os.environ.get("E2E_NAMESPACE", "e2e")


def log(*args):
    print(time.strftime("%H:%M:%S"), *args, flush=True)


class HttpError(Exception):
    def __init__(self, status, body):
        super().__init__(f"HTTP {status}: {str(body)[:600]}")
        self.status, self.body = status, body


def request(method, url, body=None, headers=None, timeout=120, raw=False):
    data = body if isinstance(body, (bytes, bytearray)) or body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    if not isinstance(body, (bytes, bytearray)):
        req.add_header("content-type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            out = resp.read()
            if raw:
                return resp.status, out
            return resp.status, (json.loads(out) if out.strip() else None)
    except urllib.error.HTTPError as e:
        out = e.read()
        try:
            parsed = json.loads(out) if out.strip() else None
        except ValueError:
            parsed = out.decode(errors="replace")
        return e.code, parsed


def ok(method, path, body=None, accept=(200, 201), base=ENGINE, **kw):
    status, reply = request(method, base.rstrip("/") + path, body, **kw)
    if status not in accept:
        raise HttpError(status, reply)
    return reply


def connect(rpc, body=None, accept=(200,), base=ENGINE, timeout=120):
    """A unary Connect call in JSON (`/<package>.<Service>/<Method>`)."""
    status, reply = request("POST", f"{base.rstrip('/')}/{rpc}", body or {},
                            headers={"connect-protocol-version": "1"}, timeout=timeout)
    if status not in accept:
        raise HttpError(status, reply)
    return reply


def connect_stream(rpc, body=None, base=ENGINE, timeout=300):
    """A server-streaming Connect call in JSON: (messages, end-of-stream error)."""
    payload = json.dumps(body or {}).encode()
    frame = struct.pack(">BI", 0, len(payload)) + payload
    req = urllib.request.Request(f"{base.rstrip('/')}/{rpc}", data=frame, method="POST")
    req.add_header("content-type", "application/connect+json")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    messages, end, at = [], None, 0
    while at + 5 <= len(raw):
        flags, size = struct.unpack(">BI", raw[at:at + 5])
        chunk = json.loads(raw[at + 5:at + 5 + size] or b"{}")
        at += 5 + size
        if flags & 0x02:
            end = chunk.get("error")
        else:
            messages.append(chunk)
    return messages, end


def namespace(name=NS):
    status, reply = request("POST", f"{ENGINE}/v1/namespaces", {"name": name})
    if status not in (200, 201, 409):
        raise HttpError(status, reply)


def until(what, check, within=120.0, every=1.0):
    deadline = time.monotonic() + within
    last = None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except (HttpError, AssertionError, OSError) as e:
            last = e
        time.sleep(every)
    raise AssertionError(f"timed out waiting until {what} (last: {last})")


def b64(data) -> str:
    if isinstance(data, str):
        data = data.encode()
    return base64.b64encode(data).decode()


def data_path(*parts):
    path = os.path.join(DATA, *parts)
    if not os.path.exists(path):
        sys.exit(f"missing {path}: run scripts/local-e2e/fetch-data.sh first")
    return path


class Report:
    """Collects what each check found, printed at the end (and as JSON)."""

    def __init__(self, name):
        self.name, self.rows = name, []

    def add(self, check, ok_, detail=""):
        self.rows.append({"check": check, "ok": bool(ok_), "detail": str(detail)[:400]})
        log(("PASS" if ok_ else "FAIL"), check, "-", str(detail)[:300])

    def attempt(self, check, fn):
        try:
            detail = fn()
            self.add(check, True, detail)
            return detail
        except Exception as e:  # a failed check is recorded, the seed goes on
            self.add(check, False, f"{type(e).__name__}: {e}")
            return None

    def done(self):
        out = os.path.join(os.environ.get("E2E_HOME", "/mnt/Projects/loams-e2e"), "logs", f"seed-{self.name}.json")
        with open(out, "w") as f:
            json.dump(self.rows, f, indent=2)
        bad = [r for r in self.rows if not r["ok"]]
        log(f"{self.name}: {len(self.rows) - len(bad)} passed, {len(bad)} failed ({out})")
        return 1 if bad else 0
