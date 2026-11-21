"""FastAPI application: canary rollout control API."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query

from . import rollout
from .db import init_db
from .models import (
    CreateRolloutRequest,
    DecisionResultRequest,
    EvaluateResponse,
    ObservationAck,
    PauseRequest,
    ReplicaStatusRequest,
    RolloutView,
    WindowMetricsRequest,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Canary Rollout API",
    version="1.0.0",
    description=(
        "Drives gradual (canary) rollouts from operator-reported replica "
        "status and window metrics. Decisions and execution results are "
        "durable in SQLite; stale generations are recorded but ignored."
    ),
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/rollouts", response_model=RolloutView, status_code=201)
def create_rollout(req: CreateRolloutRequest) -> dict:
    return rollout.create_rollout(req)


@app.get("/rollouts", response_model=list[RolloutView])
def list_rollouts(limit: int = Query(default=50, ge=1, le=200)) -> list[dict]:
    return rollout.list_rollouts(limit)


@app.get("/rollouts/{rollout_id}", response_model=RolloutView)
def get_rollout(rollout_id: str) -> dict:
    return rollout.get_rollout(rollout_id)


@app.post(
    "/rollouts/{rollout_id}/replicas",
    response_model=ObservationAck,
    status_code=202,
)
def report_replica(rollout_id: str, req: ReplicaStatusRequest) -> dict:
    return rollout.ingest_replica(rollout_id, req)


@app.post(
    "/rollouts/{rollout_id}/metrics",
    response_model=ObservationAck,
    status_code=202,
)
def report_metrics(rollout_id: str, req: WindowMetricsRequest) -> dict:
    return rollout.ingest_metrics(rollout_id, req)


@app.post("/rollouts/{rollout_id}/evaluate", response_model=EvaluateResponse)
def evaluate(rollout_id: str) -> dict:
    return rollout.evaluate(rollout_id)


@app.post(
    "/rollouts/{rollout_id}/decisions/{decision_id}/result",
    response_model=EvaluateResponse,
)
def execution_result(
    rollout_id: str, decision_id: str, req: DecisionResultRequest
) -> dict:
    return rollout.report_execution(rollout_id, decision_id, req)


@app.post("/rollouts/{rollout_id}/pause", response_model=EvaluateResponse)
def pause(rollout_id: str, req: PauseRequest | None = None) -> dict:
    reason = req.reason if req else None
    return rollout.pause(rollout_id, reason)


@app.post("/rollouts/{rollout_id}/resume", response_model=RolloutView)
def resume(rollout_id: str) -> dict:
    return rollout.resume(rollout_id)


@app.get("/rollouts/{rollout_id}/decisions")
def list_decisions(rollout_id: str) -> dict:
    return {"rollout_id": rollout_id, "decisions": rollout.list_decisions(rollout_id)}


@app.get("/rollouts/{rollout_id}/observations")
def list_observations(
    rollout_id: str, kind: str | None = Query(default=None, pattern="^(replicas|metrics)$")
) -> dict:
    return rollout.list_observations(rollout_id, kind)
