#!/usr/bin/env python3
"""An ACP agent that asks for a tool permission while session/new is still
open (before any prompt), and answers session/new with the outcome it got."""
import json
import sys


def emit(frame):
    sys.stdout.write(json.dumps(frame) + "\n")
    sys.stdout.flush()


pending = None
for line in sys.stdin:
    frame = json.loads(line)
    method = frame.get("method")
    ident = frame.get("id")
    if method == "session/new":
        pending = ident
        emit({"id": 700, "method": "session/request_permission", "params": {
            "toolCall": {"toolCallId": "setup-1", "kind": "execute", "title": "rm -rf build"},
            "options": [
                {"optionId": "yes", "name": "Allow", "kind": "allow_once"},
                {"optionId": "no", "name": "Reject", "kind": "reject_once"},
            ]}})
    elif method is None and ident == 700:
        emit({"id": pending, "result": {"sessionId": "s", "permission": frame["result"]}})
