"""Stale / late observations must be recorded but never change state."""
from __future__ import annotations

from fastapi.testclient import TestClient

from conftest import make_rollout, report_metrics, report_replica


def _advance_to_generation_one(client, rid: str) -> str:
    report_replica(client, rid, 0, "canary-1")
    report_replica(client, rid, 0, "canary-2")
    report_metrics(client, rid, 0, 30, 1000, 4)
    did = client.post(f"/rollouts/{rid}/evaluate").json()["decision"]["id"]
    client.post(f"/rollouts/{rid}/decisions/{did}/result", json={"status": "succeeded"})
    return did


def test_stale_replica_report_is_flagged_ignored(client: TestClient):
    rid = make_rollout(client)
    _advance_to_generation_one(client, rid)

    ack = report_replica(client, rid, 0, "late-canary", "unready").json()
    assert ack["ignored"] is True
    assert ack["stored"] is True
    assert "stale generation 0 < current 1" in ack["reason"]

    stored = client.get(f"/rollouts/{rid}/observations",
                        params={"kind": "replicas"}).json()["replicas"]
    late = [r for r in stored if r["replica_id"] == "late-canary"]
    assert len(late) == 1
    assert late[0]["ignored"] == 1
    assert late[0]["state"] == "unready"


def test_stale_metrics_cannot_trigger_rollback(client: TestClient):
    rid = make_rollout(client)
    _advance_to_generation_one(client, rid)  # healthy gen-0 -> now gen 1

    # A late gen-0 window with a catastrophic error rate must be ignored.
    ack = report_metrics(client, rid, 0, 30, 100, 99).json()
    assert ack["ignored"] is True

    # Gen-1 has no metrics and a healthy replica: evaluation holds, no rollback.
    report_replica(client, rid, 1, "canary-1")
    report_replica(client, rid, 1, "canary-2")
    decision = client.post(f"/rollouts/{rid}/evaluate").json()
    assert decision["decision"]["action"] == "pause"
    assert decision["rollout"]["failure_message"] is None

    # The aggregated error rate must be zero because only ignored rows exist.
    assert decision["decision"]["detail"]["metrics"]["total_requests"] == 0


def test_future_generation_is_rejected(client: TestClient):
    rid = make_rollout(client)
    resp = report_replica(client, rid, 5, "time-traveller")
    assert resp.status_code == 422
    resp = report_metrics(client, rid, 3, 30, 10, 0)
    assert resp.status_code == 422


def test_only_latest_replica_state_counts(client: TestClient):
    rid = make_rollout(client)
    # Same replica first unready, then ready within generation 0.
    report_replica(client, rid, 0, "canary-1", "unready")
    report_replica(client, rid, 0, "canary-1", "ready")
    report_metrics(client, rid, 0, 30, 100, 0)

    decision = client.post(f"/rollouts/{rid}/evaluate").json()
    detail = decision["decision"]["detail"]
    assert detail["ready_replicas"] == 1
    assert detail["unready_replicas"] == 0
    assert decision["decision"]["action"] == "promote"
