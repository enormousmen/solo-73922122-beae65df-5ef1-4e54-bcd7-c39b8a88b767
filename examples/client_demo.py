#!/usr/bin/env python3
"""Callable Python example for the canary rollout API (stdlib only).

Run the API first (docker compose up -d, or: uvicorn app.main:app), then:

    python examples/client_demo.py

The script creates a rollout, drives one healthy batch to a successful
promote, then feeds a window that breaches the error-rate threshold and
follows the resulting rollback decision to its terminal state.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

BASE = os.environ.get("BASE", "http://127.0.0.1:8000")


def call(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:  # surface API validation errors
        body = exc.read().decode()
        raise SystemExit(f"HTTP {exc.code} on {method} {path}: {body}") from None


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> None:
    print("== create rollout ==")
    rollout = call("POST", "/rollouts", {
        "service": "checkout-service",
        "target_version": "v2.3.1",
        "total_replicas": 4,
        "max_replicas_per_batch": 1,
        "min_stable_window_seconds": 30,
        "error_rate_threshold": 0.05,
    })
    rid = rollout["id"]
    print("created", rid, "status:", rollout["status"])

    print("\n== healthy canary batch, generation 0 ==")
    call("POST", f"/rollouts/{rid}/replicas", {
        "generation": 0, "replica_id": "canary-1",
        "state": "ready", "observed_version": "v2.3.1",
    })
    call("POST", f"/rollouts/{rid}/metrics", {
        "generation": 0,
        "window_started_at": iso_now(),
        "window_seconds": 30,
        "request_count": 1000,
        "error_count": 4,  # 0.4% < 5%
    })
    decision = call("POST", f"/rollouts/{rid}/evaluate", {})
    print("decision:", decision["decision"]["action"], "-", decision["message"])
    assert decision["decision"]["action"] == "promote"

    print("\n== platform executes the promote successfully ==")
    result = call(
        "POST",
        f"/rollouts/{rid}/decisions/{decision['decision']['id']}/result",
        {"status": "succeeded", "detail": "scaled to 1"},
    )
    print("status:", result["rollout"]["status"],
          "replicas:", result["rollout"]["current_replicas"],
          "generation:", result["rollout"]["current_generation"])

    print("\n== late generation-0 report is ignored ==")
    ack = call("POST", f"/rollouts/{rid}/replicas", {
        "generation": 0, "replica_id": "late-canary", "state": "ready",
    })
    print("ignored:", ack["ignored"], "-", ack["reason"])
    assert ack["ignored"] is True

    print("\n== generation 1 window breaches the threshold -> rollback ==")
    call("POST", f"/rollouts/{rid}/replicas", {
        "generation": 1, "replica_id": "canary-1",
        "state": "ready", "observed_version": "v2.3.1",
    })
    call("POST", f"/rollouts/{rid}/metrics", {
        "generation": 1,
        "window_started_at": iso_now(),
        "window_seconds": 30,
        "request_count": 100,
        "error_count": 20,  # 20% > 5%
    })
    decision = call("POST", f"/rollouts/{rid}/evaluate", {})
    print("decision:", decision["decision"]["action"], "-", decision["message"])
    assert decision["decision"]["action"] == "rollback"

    result = call(
        "POST",
        f"/rollouts/{rid}/decisions/{decision['decision']['id']}/result",
        {"status": "succeeded", "detail": "traffic shifted to v2.3.0"},
    )
    print("final status:", result["rollout"]["status"])
    assert result["rollout"]["status"] == "rolled_back"

    print("\nall decisions:")
    for d in call("GET", f"/rollouts/{rid}/decisions")["decisions"]:
        print(f"  gen {d['generation']:>2} {d['action']:<9} "
              f"{d['from_replicas']}->{d['to_replicas']} {d['execution_status']}")
    print("\nOK")


if __name__ == "__main__":
    sys.exit(main())
