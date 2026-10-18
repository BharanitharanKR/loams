"""Loams Durable (the embedded Resonate server, its listener on 18001): a
small order workflow's promises in every state, invocations that create tasks,
and schedules.

- `order-<n>` root promises with `resonate:target` tags (each creates a task
  for the `poll://any@order-workers` group), some resolved, rejected,
  canceled or left pending;
- `order-<n>:charge` / `:ship` child promises;
- schedules `nightly-report` (cron 0 2 * * *) and `hourly-sync` (0 * * * *).

Idempotent: a promise or schedule that exists is answered as it is (409s are
fine).

    venv/bin/python -I scripts/local-e2e/seed/durable.py
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import DURABLE, Report, request  # noqa: E402

VERSION = "2026-04-01"


def call(kind, data):
    status, reply = request("POST", DURABLE.rstrip("/") + "/",
                            {"kind": kind, "head": {"corrId": f"e2e-{kind}-{time.time_ns()}", "version": VERSION},
                             "data": data})
    head = (reply or {}).get("head", {}) if isinstance(reply, dict) else {}
    return head.get("status", status), (reply or {}).get("data") if isinstance(reply, dict) else reply


def payload(obj):
    return {"headers": {}, "data": base64.b64encode(json.dumps(obj).encode()).decode()}


def main():
    report = Report("durable")
    now = int(time.time() * 1000)
    far = now + 30 * 24 * 3600 * 1000
    states = ["resolved", "resolved", "rejected", "pending", "resolved", "rejected_canceled", "pending", "resolved"]
    for n, state in enumerate(states, start=1):
        root = f"order-{1000 + n}"
        s, d = call("promise.create", {
            "id": root, "timeoutAt": far, "param": payload({"order": 1000 + n, "items": n, "total": 19.5 * n}),
            "tags": {"resonate:target": "poll://any@order-workers",
                     "workflow": "checkout"}})
        report.add(f"promise.create {root}", s in (200, 201, 409), f"{s} {str(d)[:160]}")
        for step in ("charge", "ship"):
            cid = f"{root}:{step}"  # a root's child joins with ":" (resonate-core)
            s, d = call("promise.create", {"id": cid, "timeoutAt": far, "param": payload({"step": step}),
                                           "tags": {"resonate:root": root, "resonate:parent": root, "workflow": "checkout"}})
            report.add(f"promise.create {cid}", s in (200, 201, 409), f"{s} {str(d)[:160]}")
            if state != "pending":
                s, d = call("promise.settle", {"id": cid, "state": "resolved", "value": payload({"ok": True})})
                report.add(f"promise.settle {cid}", s in (200, 201, 409), f"{s}")
        if state != "pending":
            value = payload({"status": "done"} if state == "resolved" else {"error": "card declined"})
            s, d = call("promise.settle", {"id": root, "state": state, "value": value})
            report.add(f"promise.settle {root} {state}", s in (200, 201, 409), f"{s} {str(d)[:160]}")
    # A short-lived one that times out (rejected_timedout once its time passes).
    s, d = call("promise.create", {"id": "order-timeout-1", "timeoutAt": now + 5000, "param": payload({}), "tags": {}})
    report.add("promise.create order-timeout-1", s in (200, 201, 409), f"{s}")
    for sid, cron in (("nightly-report", "0 2 * * *"), ("hourly-sync", "0 * * * *")):
        s, d = call("schedule.create", {"id": sid, "cron": cron, "promiseId": f"{sid}.{{{{.timestamp}}}}",
                                        "promiseTimeout": 3600000, "promiseParam": payload({"schedule": sid}),
                                        "promiseTags": {"resonate:target": "poll://any@report-workers"}})
        report.add(f"schedule.create {sid}", s in (200, 201, 409), f"{s} {str(d)[:200]}")
    report.attempt("promise.search order-*", lambda: len((call("promise.search", {"id": "order-*", "limit": 100})[1]
                                                         or {}).get("promises", [])))
    report.attempt("task.search", lambda: str(call("task.search", {"limit": 50})[1])[:300])
    report.attempt("schedule.search", lambda: str(call("schedule.search", {"id": "*", "limit": 50})[1])[:300])
    return report.done()


if __name__ == "__main__":
    sys.exit(main())
