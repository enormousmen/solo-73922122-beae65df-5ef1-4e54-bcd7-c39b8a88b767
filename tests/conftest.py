"""Test fixtures: each test gets an isolated SQLite database and API client."""
from __future__ import annotations

import os
import tempfile

import pytest

# Point the app at an isolated database before importing app modules.
_TMP = tempfile.mkdtemp(prefix="canary-test-")
os.environ["CANARY_DB_PATH"] = os.path.join(_TMP, "test.db")

from fastapi.testclient import TestClient  # noqa: E402

from app.db import init_db  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture()
def client() -> TestClient:
    # Fresh schema for every test.
    path = os.environ["CANARY_DB_PATH"]
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass
    init_db(path)
    with TestClient(app) as c:
        yield c


def make_rollout(client: TestClient, **overrides) -> str:
    payload = {
        "service": "checkout-service",
        "target_version": "v2.3.1",
        "total_replicas": 4,
        "max_replicas_per_batch": 2,
        "min_stable_window_seconds": 30,
        "error_rate_threshold": 0.05,
    }
    payload.update(overrides)
    resp = client.post("/rollouts", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


def report_replica(client, rid, generation, replica_id, state="ready", version="v2.3.1"):
    return client.post(f"/rollouts/{rid}/replicas", json={
        "generation": generation,
        "replica_id": replica_id,
        "state": state,
        "observed_version": version,
    })


def report_metrics(client, rid, generation, window_seconds, requests, errors):
    return client.post(f"/rollouts/{rid}/metrics", json={
        "generation": generation,
        "window_started_at": "2026-11-21T00:00:00+00:00",
        "window_seconds": window_seconds,
        "request_count": requests,
        "error_count": errors,
    })
