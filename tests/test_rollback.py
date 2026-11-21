"""Error-rate threshold: rollback decisions and failure handling."""
from __future__ import annotations

from fastapi.testclient import TestClient

from conftest import make_rollout, report_metrics, report_replica


def _healthy_first_batch(client, rid: str) -> None:
    report_replica(client, rid, 0, "canary-1")
    report_replica(client, rid, 0, "canary-2")
    report_metrics(client, rid, 0, 30, 1000, 4)
    did = client.post(f"/rollouts/{rid}/evaluate").json()["decision"]["id"]
    client.post(f"/rollouts/{rid}/decisions/{did}/result", json={"status": "succeeded"})


def test_threshold_breach_triggers_rollback(client: TestClient):
    rid = make_rollout(client)
    _healthy_first_batch(client, rid)  # now generation 1, 2 replicas

    report_replica(client, rid, 1, "canary-1")
    report_replica(client, rid, 1, "canary-2")
    report_metrics(client, rid, 1, 30, 100, 20)  # 20% > 5%

    decision = client.post(f"/rollouts/{rid}/evaluate").json()
    assert decision["decision"]["action"] == "rollback"
    assert decision["rollout"]["pending_decision_id"] == decision["decision"]["id"]
    assert decision["decision"]["detail"]["metrics"]["error_rate"] == 0.2

    done = client.post(
        f"/rollouts/{rid}/decisions/{decision['decision']['id']}/result",
        json={"status": "succeeded", "detail": "traffic back on v2.3.0"},
    ).json()
    assert done["rollout"]["status"] == "rolled_back"
    assert done["rollout"]["current_replicas"] == 0
    assert done["rollout"]["current_generation"] == 2
    assert done["decision"]["execution_status"] == "succeeded"


def test_rollback_failure_marks_rollout_failed_and_keeps_state(client: TestClient):
    rid = make_rollout(client)
    _healthy_first_batch(client, rid)  # 2 replicas live, generation 1

    report_replica(client, rid, 1, "canary-1")
    report_metrics(client, rid, 1, 30, 100, 30)  # 30% > 5%
    rb = client.post(f"/rollouts/{rid}/evaluate").json()
    assert rb["decision"]["action"] == "rollback"

    failed = client.post(
        f"/rollouts/{rid}/decisions/{rb['decision']['id']}/result",
        json={"status": "failed", "detail": "load balancer rejected change"},
    ).json()
    # Previous executable state is retained.
    assert failed["rollout"]["status"] == "failed"
    assert failed["rollout"]["current_replicas"] == 2
    assert failed["rollout"]["current_generation"] == 1
    assert failed["rollout"]["pending_decision_id"] is None
    assert "rollback execution failed" in failed["rollout"]["failure_message"]
    assert failed["decision"]["execution_status"] == "failed"

    # A failed rollout is terminal: no further automation.
    again = client.post(f"/rollouts/{rid}/evaluate").json()
    assert again["evaluated"] is False
    assert "terminal" in again["message"]


def test_zero_traffic_does_not_trigger_rollback(client: TestClient):
    rid = make_rollout(client)
    report_replica(client, rid, 0, "canary-1")
    report_replica(client, rid, 0, "canary-2")
    report_metrics(client, rid, 0, 30, 0, 0)

    decision = client.post(f"/rollouts/{rid}/evaluate").json()
    assert decision["decision"]["action"] != "rollback"


def test_error_rate_equal_to_threshold_is_not_a_breach(client: TestClient):
    rid = make_rollout(client)  # threshold 0.05
    report_replica(client, rid, 0, "canary-1")
    report_replica(client, rid, 0, "canary-2")
    report_metrics(client, rid, 0, 30, 100, 5)  # exactly 5%

    decision = client.post(f"/rollouts/{rid}/evaluate").json()
    assert decision["decision"]["action"] == "promote"
