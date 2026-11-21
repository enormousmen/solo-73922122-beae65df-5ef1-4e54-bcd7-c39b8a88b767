"""Failed execution, manual pause/resume and request validation."""
from __future__ import annotations

from fastapi.testclient import TestClient

from conftest import make_rollout, report_metrics, report_replica


def test_failed_promote_keeps_previous_state_and_allows_retry(client: TestClient):
    rid = make_rollout(client)  # total 4, cap 2
    report_replica(client, rid, 0, "canary-1")
    report_replica(client, rid, 0, "canary-2")
    report_metrics(client, rid, 0, 30, 1000, 4)
    decision = client.post(f"/rollouts/{rid}/evaluate").json()
    did = decision["decision"]["id"]
    assert decision["decision"]["action"] == "promote"

    failed = client.post(
        f"/rollouts/{rid}/decisions/{did}/result",
        json={"status": "failed", "detail": "deployment controller timeout"},
    ).json()
    # Previous executable state (0 replicas, gen 0) is retained.
    assert failed["rollout"]["status"] == "paused"
    assert failed["rollout"]["current_replicas"] == 0
    assert failed["rollout"]["current_generation"] == 0
    assert failed["rollout"]["pending_decision_id"] is None
    assert "promote execution failed" in failed["rollout"]["failure_message"]
    assert failed["decision"]["execution_status"] == "failed"

    # Automation is stopped and the previous state (0 replicas, gen 0) is
    # intact. The operator explicitly resumes, then the same healthy
    # observations drive a fresh promote decision on the same generation.
    resumed = client.post(f"/rollouts/{rid}/resume").json()
    assert resumed["status"] == "pending"
    assert resumed["failure_message"] is None
    assert resumed["current_generation"] == 0

    retry = client.post(f"/rollouts/{rid}/evaluate").json()
    assert retry["decision"]["action"] == "promote"
    assert retry["decision"]["id"] != did
    ok = client.post(
        f"/rollouts/{rid}/decisions/{retry['decision']['id']}/result",
        json={"status": "succeeded"},
    ).json()
    assert ok["rollout"]["status"] == "promoting"
    assert ok["rollout"]["current_replicas"] == 2


def test_failed_promote_can_also_be_reevaluated_without_resume(client: TestClient):
    rid = make_rollout(client)
    report_replica(client, rid, 0, "canary-1")
    report_replica(client, rid, 0, "canary-2")
    report_metrics(client, rid, 0, 30, 1000, 4)
    did = client.post(f"/rollouts/{rid}/evaluate").json()["decision"]["id"]
    client.post(
        f"/rollouts/{rid}/decisions/{did}/result",
        json={"status": "failed", "detail": "controller timeout"},
    )

    # State is still gen 0 with the same observations, so evaluation may
    # immediately re-issue the promote without an explicit resume.
    retry = client.post(f"/rollouts/{rid}/evaluate").json()
    assert retry["decision"]["action"] == "promote"
    assert retry["decision"]["id"] != did
    assert retry["rollout"]["current_generation"] == 0


def test_execution_result_cannot_be_rewritten(client: TestClient):
    rid = make_rollout(client)
    report_replica(client, rid, 0, "canary-1")
    report_metrics(client, rid, 0, 30, 100, 0)
    did = client.post(f"/rollouts/{rid}/evaluate").json()["decision"]["id"]
    client.post(f"/rollouts/{rid}/decisions/{did}/result", json={"status": "succeeded"})

    rewrite = client.post(
        f"/rollouts/{rid}/decisions/{did}/result", json={"status": "failed"}
    )
    assert rewrite.status_code == 409

    view = client.get(f"/rollouts/{rid}").json()
    assert view["current_replicas"] == 2  # committed state untouched


def test_manual_pause_and_resume(client: TestClient):
    rid = make_rollout(client)
    paused = client.post(f"/rollouts/{rid}/pause",
                         json={"reason": "incident on dependent service"}).json()
    assert paused["rollout"]["status"] == "paused"
    assert paused["decision"]["detail"]["manual"] is True

    # Pausing again is allowed but never creates a pending decision.
    again = client.post(f"/rollouts/{rid}/pause", json={}).json()
    assert again["rollout"]["status"] == "paused"
    assert again["rollout"]["pending_decision_id"] is None

    resumed = client.post(f"/rollouts/{rid}/resume").json()
    assert resumed["status"] == "pending"
    assert resumed["current_generation"] == 0


def test_unready_replica_blocks_promotion(client: TestClient):
    rid = make_rollout(client)
    report_replica(client, rid, 0, "canary-1", "ready")
    report_replica(client, rid, 0, "canary-2", "unready")
    report_metrics(client, rid, 0, 30, 100, 0)

    decision = client.post(f"/rollouts/{rid}/evaluate").json()
    assert decision["decision"]["action"] == "pause"
    assert "unready" in decision["message"]
    assert decision["rollout"]["current_replicas"] == 0


def test_short_window_blocks_promotion(client: TestClient):
    rid = make_rollout(client, min_stable_window_seconds=30)
    report_replica(client, rid, 0, "canary-1")
    report_metrics(client, rid, 0, 10, 100, 0)  # 10s < 30s

    decision = client.post(f"/rollouts/{rid}/evaluate").json()
    assert decision["decision"]["action"] == "pause"
    assert "no window of 30s" in decision["message"]


def test_request_validation(client: TestClient):
    bad_batch = client.post("/rollouts", json={
        "service": "svc", "target_version": "v1", "total_replicas": 2,
        "max_replicas_per_batch": 5, "min_stable_window_seconds": 10,
        "error_rate_threshold": 0.1,
    })
    assert bad_batch.status_code == 422

    bad_threshold = client.post("/rollouts", json={
        "service": "svc", "target_version": "v1", "total_replicas": 2,
        "max_replicas_per_batch": 1, "min_stable_window_seconds": 10,
        "error_rate_threshold": 1.5,
    })
    assert bad_threshold.status_code == 422

    rid = make_rollout(client)
    bad_metrics = client.post(f"/rollouts/{rid}/metrics", json={
        "generation": 0,
        "window_started_at": "not-a-timestamp",
        "window_seconds": 10,
        "request_count": 10,
        "error_count": 20,
    })
    assert bad_metrics.status_code == 422

    missing = client.get("/rollouts/rl_does_not_exist")
    assert missing.status_code == 404
