"""Promotion flow: hold -> batches -> completion, and restart recovery."""
from __future__ import annotations

from fastapi.testclient import TestClient

from conftest import make_rollout, report_metrics, report_replica


def test_evaluate_without_data_holds_without_consuming_generation(client: TestClient):
    rid = make_rollout(client)

    first = client.post(f"/rollouts/{rid}/evaluate").json()
    assert first["evaluated"] is True
    assert first["decision"]["action"] == "pause"
    assert first["rollout"]["status"] == "paused"
    assert first["rollout"]["current_generation"] == 0

    # A hold must not consume the generation: evaluation runs again.
    second = client.post(f"/rollouts/{rid}/evaluate").json()
    assert second["evaluated"] is True
    assert second["decision"]["action"] == "pause"
    assert second["rollout"]["current_generation"] == 0


def test_full_promotion_batches_to_completion(client: TestClient):
    rid = make_rollout(client)  # 4 replicas, cap 2, 30s window, 5% threshold

    # ---- generation 0: first canary batch of 2 healthy replicas ----
    for i in (1, 2):
        report_replica(client, rid, 0, f"canary-{i}")
    report_metrics(client, rid, 0, 30, 1000, 4)  # 0.4% errors

    eval1 = client.post(f"/rollouts/{rid}/evaluate").json()
    assert eval1["decision"]["action"] == "promote"
    assert (eval1["decision"]["from_replicas"],
            eval1["decision"]["to_replicas"]) == (0, 2)
    did = eval1["decision"]["id"]

    # A pending decision blocks further evaluation.
    blocked = client.post(f"/rollouts/{rid}/evaluate").json()
    assert blocked["evaluated"] is False
    assert blocked["decision"]["id"] == did

    ok = client.post(
        f"/rollouts/{rid}/decisions/{did}/result",
        json={"status": "succeeded", "detail": "scaled to 2"},
    ).json()
    assert ok["rollout"]["status"] == "promoting"
    assert ok["rollout"]["current_replicas"] == 2
    assert ok["rollout"]["current_generation"] == 1
    assert ok["rollout"]["pending_decision_id"] is None

    # ---- generation 1: scale 2 -> 4 ----
    for i in range(1, 5):
        report_replica(client, rid, 1, f"canary-{i}")
    report_metrics(client, rid, 1, 30, 2000, 10)  # 0.5% errors

    eval2 = client.post(f"/rollouts/{rid}/evaluate").json()
    assert eval2["decision"]["action"] == "promote"
    assert (eval2["decision"]["from_replicas"],
            eval2["decision"]["to_replicas"]) == (2, 4)
    client.post(
        f"/rollouts/{rid}/decisions/{eval2['decision']['id']}/result",
        json={"status": "succeeded"},
    )

    # ---- generation 2: full fleet stable -> completion ----
    for i in range(1, 5):
        report_replica(client, rid, 2, f"canary-{i}")
    report_metrics(client, rid, 2, 30, 3000, 6)

    final = client.post(f"/rollouts/{rid}/evaluate").json()
    assert final["decision"]["action"] == "complete"
    assert final["rollout"]["status"] == "completed"
    assert final["rollout"]["current_replicas"] == 4

    actions = [d["action"] for d in
               client.get(f"/rollouts/{rid}/decisions").json()["decisions"]]
    # Hold decisions may precede the first data-driven promotion; the rollout
    # itself is still two promotes followed by completion.
    assert [a for a in actions if a != "pause"] == ["promote", "promote", "complete"]

    # A completed rollout makes no further decisions.
    again = client.post(f"/rollouts/{rid}/evaluate").json()
    assert again["evaluated"] is False
    assert "terminal" in again["message"]


def test_state_survives_process_restart(client: TestClient):
    rid = make_rollout(client)
    report_replica(client, rid, 0, "canary-1")
    report_replica(client, rid, 0, "canary-2")
    report_metrics(client, rid, 0, 30, 1000, 1)
    did = client.post(f"/rollouts/{rid}/evaluate").json()["decision"]["id"]

    # Simulate a process restart: brand new client/app runtime, same SQLite DB.
    restarted = TestClient(app=client.app)
    with restarted as c2:
        view = c2.get(f"/rollouts/{rid}").json()
        assert view["pending_decision_id"] == did
        assert view["status"] == "pending"

        # The pending decision is still there and still blocks evaluation.
        blocked = c2.post(f"/rollouts/{rid}/evaluate").json()
        assert blocked["evaluated"] is False
        assert blocked["decision"]["id"] == did

        # Execution can complete after the restart and the rollout continues.
        done = c2.post(
            f"/rollouts/{rid}/decisions/{did}/result",
            json={"status": "succeeded", "detail": "after restart"},
        ).json()
        assert done["rollout"]["status"] == "promoting"
        assert done["rollout"]["current_generation"] == 1
        assert len(c2.get(f"/rollouts/{rid}/decisions").json()["decisions"]) == 1
