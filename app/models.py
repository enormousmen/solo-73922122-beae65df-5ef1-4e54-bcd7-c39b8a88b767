"""Pydantic request/response models for the canary rollout API."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, ConfigDict

# ---- release lifecycle states (stored in releases.status) ----
# pending    : rollout created, no decision yet
# promoting  : at least one promote decision succeeded, release in progress
# paused     : automation paused (explicit pause, failed execution, or pause decision)
# rolling_back: a rollback decision has succeeded, rollout in progress
# completed  : all replicas reached target version
# rolled_back: rollback finished
# failed     : terminal failure (e.g. rollback execution failed, awaiting manual action)

# ---- decision actions ----
ACTION_PROMOTE = "promote"
ACTION_PAUSE = "pause"
ACTION_ROLLBACK = "rollback"
ACTION_COMPLETE = "complete"

# ---- decision execution states ----
EXEC_PENDING = "pending"
EXEC_SUCCEEDED = "succeeded"
EXEC_FAILED = "failed"
EXEC_NOT_REQUIRED = "not_required"

ReplicaState = Literal["pending", "ready", "unready", "terminating"]


class CreateRolloutRequest(BaseModel):
    service: str = Field(..., min_length=1, max_length=128, examples=["checkout-service"])
    target_version: str = Field(..., min_length=1, max_length=128, examples=["v2.3.1"])
    total_replicas: int = Field(..., ge=1, le=100_000)
    max_replicas_per_batch: int = Field(..., ge=1, le=100_000)
    min_stable_window_seconds: int = Field(..., ge=1)
    error_rate_threshold: float = Field(..., gt=0.0, le=1.0)


class ReplicaStatusRequest(BaseModel):
    generation: int = Field(..., ge=0)
    replica_id: str = Field(..., min_length=1, max_length=128)
    state: ReplicaState
    observed_version: Optional[str] = Field(default=None, max_length=128)


class WindowMetricsRequest(BaseModel):
    generation: int = Field(..., ge=0)
    window_started_at: str = Field(
        ...,
        description="ISO-8601 UTC timestamp marking the start of the observation window",
    )
    window_seconds: float = Field(..., gt=0)
    request_count: int = Field(..., ge=0)
    error_count: int = Field(..., ge=0)


class DecisionResultRequest(BaseModel):
    status: Literal["succeeded", "failed"]
    detail: Optional[str] = Field(default=None, max_length=1024)


class PauseRequest(BaseModel):
    reason: Optional[str] = Field(default=None, max_length=512)


class ObservationAck(BaseModel):
    model_config = ConfigDict(extra="ignore")
    rollout_id: str
    generation: int
    current_generation: int
    ignored: bool
    reason: Optional[str] = None
    stored: bool


class DecisionView(BaseModel):
    id: str
    rollout_id: str
    generation: int
    action: str
    from_replicas: int
    to_replicas: int
    reason: str
    detail: dict
    execution_status: str
    execution_detail: Optional[str]
    created_at: str
    decided_at: str
    executed_at: Optional[str]


class RolloutView(BaseModel):
    id: str
    service: str
    target_version: str
    total_replicas: int
    max_replicas_per_batch: int
    min_stable_window_seconds: int
    error_rate_threshold: float
    status: str
    current_generation: int
    current_replicas: int
    pending_decision_id: Optional[str]
    last_decision_id: Optional[str]
    failure_message: Optional[str]
    created_at: str
    updated_at: str


class EvaluateResponse(BaseModel):
    rollout: RolloutView
    decision: Optional[DecisionView] = None
    evaluated: bool
    message: str
