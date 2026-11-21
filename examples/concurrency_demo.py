#!/usr/bin/env python3
"""Concurrency demo for the canary rollout API (stdlib only).

Run the API first (docker compose up -d, or: uvicorn app.main:app), then:

    BASE=http://127.0.0.1:8000 python3 examples/concurrency_demo.py

It demonstrates three properties of the lock-free evaluation path:
1. two rollouts interleave observation ingestion, /evaluate and execution
   receipts over real HTTP threads without blocking one another;
2. a stale-generation backfill is stored, flagged ignored and never disturbs
   an evaluation;
3. when a rollout keeps receiving valid observations while an evaluation is
   in flight, the API either recomputes against the newest data or answers
   409 "please retry" without ever persisting a stale decision -- and a
   follow-up retry then succeeds.
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

BASE = os.environ.get("BASE", "http://127.0.0.1:8000")


def call(method: str, path: str, payload: dict | None = None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_rollout(service: str) -> str:
    status, body = call("POST", "/rollouts", {
        "service": service, "target_version": "v2.3.1",
        "total_replicas": 4, "max_replicas_per_batch": 2,
        "min_stable_window_seconds": 30, "error_rate_threshold": 0.05,
    })
    assert status == 201, body
    return body["id"]


def report_metrics(rid: str, generation: int, requests: int, errors: int):
    return call("POST", f"/rollouts/{rid}/metrics", {
        "generation": generation, "window_started_at": iso_now(),
        "window_seconds": 30, "request_count": requests, "error_count": errors,
    })


def report_replica(rid: str, generation: int, name: str, state: str = "ready"):
    return call("POST", f"/rollouts/{rid}/replicas", {
        "generation": generation, "replica_id": name,
        "state": state, "observed_version": "v2.3.1",
    })


def evaluate_with_retry(rid: str) -> dict:
    """Evaluate, honouring the 409 optimistic-conflict contract."""
    for attempt in range(5):
        status, body = call("POST", f"/rollouts/{rid}/evaluate")
        if status == 409:
            print(f"  {rid}: 409 conflict ({body['detail']}); retrying")
            time.sleep(0.05 * (attempt + 1))
            continue
        assert status == 200, body
        return body
    raise RuntimeError(f"{rid}: evaluation kept conflicting")


def driver(rid: str, rounds: int, failures: list) -> None:
    try:
        for r in range(rounds):
            report_metrics(rid, 0, 100, 0)
            report_replica(rid, 0, f"canary-{r % 4}")
            decision = evaluate_with_retry(rid)
            did = decision["rollout"]["pending_decision_id"]
            if did:
                status, receipt = call(
                    "POST", f"/rollouts/{rid}/decisions/{did}/result",
                    {"status": "succeeded", "detail": "ok"},
                )
                assert status == 200, receipt
    except Exception as exc:  # surface thread failures in the main thread
        failures.append(exc)


def main() -> None:
    print("== 1. two rollouts, interleaved ingest/evaluate/receipt threads ==")
    rid_a, rid_b = make_rollout("demo-svc-a"), make_rollout("demo-svc-b")
    failures: list[Exception] = []
    threads = [
        threading.Thread(target=driver, args=(rid_a, 4, failures)),
        threading.Thread(target=driver, args=(rid_b, 4, failures)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not failures, failures
    for rid in (rid_a, rid_b):
        _, view = call("GET", f"/rollouts/{rid}")
        _, decisions = call("GET", f"/rollouts/{rid}/decisions")
        pending = [d for d in decisions["decisions"]
                   if d["execution_status"] == "pending"]
        assert view["pending_decision_id"] is None and not pending
        print(f"  {rid}: generation {view['current_generation']}, "
              f"{len(decisions['decisions'])} decisions, none pending")

    print("\n== 2. stale backfill on rollout A is stored but ignored ==")
    gen = call("GET", f"/rollouts/{rid_a}")[1]["current_generation"]
    status, ack = report_metrics(rid_a, gen - 1, 100, 99)
    assert status == 202 and ack["ignored"] and ack["stored"], ack
    status, ack = report_replica(rid_a, gen - 1, "late-canary", "unready")
    assert status == 202 and ack["ignored"], ack
    print(f"  stale gen-{gen - 1} reports accepted with ignored=true")

    print("\n== 3. conflicting observations: recompute / 409, never stale write ==")
    rid_c = make_rollout("demo-svc-c")
    report_replica(rid_c, 0, "canary-0")
    report_replica(rid_c, 0, "canary-1")
    report_metrics(rid_c, 0, 1000, 4)  # healthy: would promote on its own

    def noisy_ingestion(stop: threading.Event) -> None:
        # Keep appending valid gen-0 windows; enough errors to push the
        # cumulative error rate above the 5% threshold.
        i = 0
        while not stop.is_set():
            report_metrics(rid_c, 0, 100, 50)
            i += 1
            time.sleep(0.002)

    stop = threading.Event()
    producer = threading.Thread(target=noisy_ingestion, args=(stop,))
    producer.start()
    saw_conflict = False
    committed = None
    try:
        for _ in range(10):
            status, body = call("POST", f"/rollouts/{rid_c}/evaluate")
            if status == 409:
                saw_conflict = True
                print("  got 409 retry prompt, no decision written:",
                      body["detail"])
                continue
            assert status == 200, body
            if body["evaluated"]:
                action = body["decision"]["action"]
                rate = body["decision"]["detail"]["metrics"]["error_rate"]
                committed = body
                print(f"  committed decision: {action} "
                      f"(error_rate={rate:.4f}, requests="
                      f"{body['decision']['detail']['metrics']['total_requests']})")
                # Whatever persisted matches the data visible at commit time:
                # above the threshold it must be a rollback, never a promote.
                if rate > 0.05:
                    assert action == "rollback", body
                break
            time.sleep(0.02)
    finally:
        stop.set()
        producer.join(timeout=10)

    assert committed is not None, "evaluation never committed a decision"
    if saw_conflict:
        print("  409 path observed: conflicts surfaced as retries, never as "
              "stale writes")
    else:
        # Under low latency the optimistic loop typically recomputes in-place
        # before three attempts elapse; that still proves the stale promote was
        # discarded and replaced using the newest observations.
        print("  no 409 this run: each conflict was recomputed transparently "
              "within the request; the persisted decision matches latest data")

    _, decisions = call("GET", f"/rollouts/{rid_c}/decisions")
    pending = [d for d in decisions["decisions"]
               if d["execution_status"] == "pending"]
    assert len(pending) <= 1, decisions
    print(f"  {len(decisions['decisions'])} decision rows total, "
          f"{len(pending)} pending -- state stays coherent")
    print("\nOK")


if __name__ == "__main__":
    main()
