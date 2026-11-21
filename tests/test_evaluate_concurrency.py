"""Concurrency and optimistic-commit tests for /evaluate.

The evaluation scan runs outside the write lock; these tests prove that:
* other rollouts' writes and execution receipts commit while an evaluation
  scan is in flight (no lock blocking);
* a decision is only persisted against the state and valid observations that
  hold at commit time -- newer valid observations or a state change on the
  same rollout force a recompute (or a 409 retry prompt, never a stale write);
* stale-generation backfills and other rollouts' writes never force a retry.
"""
from __future__ import annotations

import threading
import time

import pytest
from fastapi.testclient import TestClient

from conftest import make_rollout, report_metrics, report_replica
from app import rollout
from app.db import read_tx, write_tx


@pytest.fixture()
def snapshot_hook():
    """Install and always remove the evaluation test seam around a test."""
    saved = rollout._SNAPSHOT_HOOK
    rollout._SNAPSHOT_HOOK = None
    yield lambda hook: setattr(rollout, "_SNAPSHOT_HOOK", hook)
    rollout._SNAPSHOT_HOOK = saved


def _healthy_ingest(client, rid, generation, replicas=2, requests=1000, errors=4):
    for i in range(1, replicas + 1):
        report_replica(client, rid, generation, f"canary-{i}")
    report_metrics(client, rid, generation, 30, requests, errors)


def _decisions(client, rid) -> list[dict]:
    return client.get(f"/rollouts/{rid}/decisions").json()["decisions"]


# ---------------------------------------------------------------------------
# recompute paths: same-rollout changes invalidate the in-flight decision
# ---------------------------------------------------------------------------

def test_new_valid_metrics_during_evaluation_force_recompute(
    client: TestClient, snapshot_hook
):
    rid = make_rollout(client)  # 5% threshold
    _healthy_ingest(client, rid, 0)  # snapshot alone would decide "promote"

    def hook(rollout_id, attempt, plan):
        if attempt == 1:
            # New valid observations of the SAME generation land before commit.
            # 1000 req / 100 errors added on top of 1000 / 4 -> ~5.2% > 5%.
            report_metrics(client, rid, 0, 30, 1000, 100)

    snapshot_hook(hook)
    resp = client.post(f"/rollouts/{rid}/evaluate")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["evaluated"] is True
    assert body["decision"]["action"] == "rollback"
    # The recomputed rollback reflects both windows.
    assert body["decision"]["detail"]["metrics"]["total_requests"] == 2000
    assert body["decision"]["detail"]["metrics"]["total_errors"] == 104
    # Only one decision row exists: the promote computed in attempt 1 was
    # never written.
    decisions = _decisions(client, rid)
    assert len(decisions) == 1
    assert decisions[0]["action"] == "rollback"


def test_state_change_during_evaluation_force_recompute(
    client: TestClient, snapshot_hook
):
    rid = make_rollout(client)
    _healthy_ingest(client, rid, 0)
    attempts = []

    def hook(rollout_id, attempt, plan):
        attempts.append(attempt)
        if attempt == 1:
            # Operator pauses between snapshot and commit. The state tuple
            # changes, so the first-attempt decision is discarded and the
            # evaluation re-snapshots and recomputes against the new state.
            client.post(f"/rollouts/{rid}/pause", json={"reason": "incident"})

    snapshot_hook(hook)
    body = client.post(f"/rollouts/{rid}/evaluate").json()
    assert attempts == [1, 2]  # first attempt invalidated, one retry
    assert body["evaluated"] is True
    # Under the unchanged decision rules a paused-but-healthy rollout still
    # yields a promote; what matters is it was computed AFTER the pause and
    # that the first (pre-pause) decision was never persisted.
    assert body["decision"]["action"] == "promote"
    actions = [d["action"] for d in _decisions(client, rid)]
    assert actions == ["pause", "promote"]
    assert body["rollout"]["status"] == "paused"
    assert body["rollout"]["pending_decision_id"] == body["decision"]["id"]


def test_repeated_conflicts_return_409_without_writing_decision(
    client: TestClient, snapshot_hook
):
    rid = make_rollout(client)
    _healthy_ingest(client, rid, 0)
    before = len(_decisions(client, rid))

    def hook(rollout_id, attempt, plan):
        # Conflict on every single attempt.
        report_metrics(client, rid, 0, 30, 10, 0)

    snapshot_hook(hook)
    resp = client.post(f"/rollouts/{rid}/evaluate")
    assert resp.status_code == 409
    assert "retry" in resp.json()["detail"]
    # Nothing was persisted and no pending decision appeared.
    assert len(_decisions(client, rid)) == before
    view = client.get(f"/rollouts/{rid}").json()
    assert view["pending_decision_id"] is None

    # A follow-up evaluation with no further conflicts succeeds normally.
    rollout._SNAPSHOT_HOOK = None
    body = client.post(f"/rollouts/{rid}/evaluate").json()
    assert body["evaluated"] is True
    assert body["decision"]["action"] == "promote"


# ---------------------------------------------------------------------------
# non-invalidating writes: must not trigger a retry
# ---------------------------------------------------------------------------

def test_stale_generation_backfill_does_not_invalidate_evaluation(
    client: TestClient, snapshot_hook
):
    rid = make_rollout(client)
    _healthy_ingest(client, rid, 0)
    # Advance to generation 1 so gen-0 reports become stale backfills.
    did = client.post(f"/rollouts/{rid}/evaluate").json()["decision"]["id"]
    client.post(f"/rollouts/{rid}/decisions/{did}/result", json={"status": "succeeded"})
    _healthy_ingest(client, rid, 1)

    attempts = []

    def hook(rollout_id, attempt, plan):
        attempts.append(attempt)
        # Old-generation backfill: stored but ignored, fingerprint untouched.
        report_metrics(client, rid, 0, 30, 100, 99)
        report_replica(client, rid, 0, "late-canary", "unready")

    snapshot_hook(hook)
    body = client.post(f"/rollouts/{rid}/evaluate").json()
    assert body["evaluated"] is True
    assert body["decision"]["action"] == "promote"
    assert attempts == [1]  # committed on the first attempt, no retry

    stored = client.get(
        f"/rollouts/{rid}/observations", params={"kind": "metrics"}
    ).json()["metrics"]
    stale = [m for m in stored if m["generation"] == 0 and m["ignored"] == 1]
    assert len(stale) == 1


def test_other_rollout_writes_do_not_invalidate_evaluation(
    client: TestClient, snapshot_hook
):
    rid = make_rollout(client)
    other = make_rollout(client, service="payment-service", target_version="v9")
    _healthy_ingest(client, rid, 0)

    attempts = []

    def hook(rollout_id, attempt, plan):
        attempts.append(attempt)
        # Heavy writes belonging to a different rollout must be invisible to
        # the fingerprint of rid.
        for i in range(1, 4):
            report_replica(client, other, 0, f"c{i}")
        report_metrics(client, other, 0, 30, 500, 1)

    snapshot_hook(hook)
    body = client.post(f"/rollouts/{rid}/evaluate").json()
    assert body["evaluated"] is True
    assert body["decision"]["action"] == "promote"
    assert attempts == [1]
    assert len(_decisions(client, other)) == 0


# ---------------------------------------------------------------------------
# lock behaviour: scans do not block writers; commits stay short
# ---------------------------------------------------------------------------

def test_observation_scan_does_not_block_other_rollout_writes(client: TestClient):
    rid = make_rollout(client)
    other = make_rollout(client, service="payment-service", target_version="v9")
    _healthy_ingest(client, rid, 0, replicas=4)

    writer_done = threading.Event()

    # Hold a lock-free snapshot read transaction open, mimicking a long
    # evaluation scan over a large observation history.
    def long_scan():
        with read_tx() as conn:
            rollout._current_metrics(conn, rid, 0)
            rollout._current_replica_states(conn, rid, 0)
            writer_done.wait(timeout=5)

    scanner = threading.Thread(target=long_scan)
    scanner.start()
    time.sleep(0.2)

    # A write for another rollout (and even the same one) must commit while
    # the scan transaction is still open: WAL readers never block writers.
    report_metrics(client, other, 0, 30, 100, 0)
    report_replica(client, rid, 0, "canary-extra")
    writer_done.set()
    scanner.join(timeout=5)
    assert not scanner.is_alive()

    rows = client.get(
        f"/rollouts/{rid}/observations", params={"kind": "replicas"}
    ).json()["replicas"]
    assert any(r["replica_id"] == "canary-extra" and r["ignored"] == 0 for r in rows)


def test_write_lock_hold_time_is_independent_of_scan_duration(
    client: TestClient, snapshot_hook
):
    rid = make_rollout(client)
    other = make_rollout(client, service="payment-service", target_version="v9")
    _healthy_ingest(client, rid, 0)

    # Make the lock-free phase deliberately slow.
    def slow_hook(rollout_id, attempt, plan):
        time.sleep(0.3)

    snapshot_hook(slow_hook)
    start = time.monotonic()
    eval_thread = threading.Thread(
        target=lambda: client.post(f"/rollouts/{rid}/evaluate")
    )
    eval_thread.start()
    time.sleep(0.1)  # let the evaluation enter its slow snapshot phase

    # The other rollout's write must land well before the evaluation finishes
    # (its snapshot alone takes ~0.3s), proving no write lock is held.
    write_start = time.monotonic()
    report_metrics(client, other, 0, 30, 100, 0)
    write_elapsed = time.monotonic() - write_start
    eval_thread.join(timeout=5)
    total = time.monotonic() - start

    assert write_elapsed < 0.2
    assert total >= 0.3


# ---------------------------------------------------------------------------
# real-thread interleaving (no test seam): two evaluations at once
# ---------------------------------------------------------------------------

def test_concurrent_evaluations_never_write_two_decisions_for_one_generation(
    client: TestClient,
):
    rid = make_rollout(client)
    _healthy_ingest(client, rid, 0)
    barrier = threading.Barrier(2)

    def evaluate():
        barrier.wait()
        return client.post(f"/rollouts/{rid}/evaluate").json()

    t1 = threading.Thread(target=evaluate)
    t2 = threading.Thread(target=evaluate)
    t1.start(); t2.start()
    t1.join(timeout=10); t2.join(timeout=10)
    assert not t1.is_alive() and not t2.is_alive()

    decisions = _decisions(client, rid)
    pending = [d for d in decisions if d["execution_status"] == "pending"]
    assert len(pending) == 1
    view = client.get(f"/rollouts/{rid}").json()
    assert view["pending_decision_id"] == pending[0]["id"]


def test_mixed_ingestion_and_evaluation_under_real_concurrency(client: TestClient):
    rid = make_rollout(client, total_replicas=10, max_replicas_per_batch=2)
    report_replica(client, rid, 0, "canary-1")
    report_replica(client, rid, 0, "canary-2")
    report_metrics(client, rid, 0, 30, 1000, 1)

    errors: list[Exception] = []

    def ingest_many():
        try:
            for i in range(20):
                report_metrics(client, rid, 0, 30, 100, 0)
                report_replica(client, rid, 0, f"bulk-{i}")
        except Exception as exc:  # pragma: no cover - failure signal only
            errors.append(exc)

    def evaluate_many():
        try:
            for _ in range(8):
                client.post(f"/rollouts/{rid}/evaluate")
        except Exception as exc:  # pragma: no cover - failure signal only
            errors.append(exc)

    threads = [threading.Thread(target=ingest_many) for _ in range(3)]
    threads += [threading.Thread(target=evaluate_many) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not errors, errors
    assert all(not t.is_alive() for t in threads)

    # At most one pending executable decision; the rollout stays coherent.
    view = client.get(f"/rollouts/{rid}").json()
    decisions = _decisions(client, rid)
    pending = [d for d in decisions if d["execution_status"] == "pending"]
    assert len(pending) <= 1
    if pending:
        assert view["pending_decision_id"] == pending[0]["id"]
    # Every persisted decision's snapshot detail is internally consistent.
    for d in decisions:
        m = d["detail"]["metrics"]
        assert m["total_errors"] <= m["total_requests"]
