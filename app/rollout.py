"""Rollout state machine: observation ingestion, decisions, execution feedback.

Guarantees:
* Every decision is persisted in SQLite before it is returned to the caller.
* Observation rows from stale generations are stored but flagged ignored and
  never affect any decision or committed state.
* A failed promote/rollback keeps the previous executable state: the release
  row is not advanced, the decision is marked failed and automation stops
  (paused for promote failure, failed for rollback failure).
* All state lives in SQLite, so after a process restart pending decisions and
  in-flight rollouts resume unchanged.
* Pause/hold decisions do not consume a generation: operators may keep
  observing the same generation and re-evaluate later. Promote, rollback and
  complete decisions do consume it (the generation advances on execution).
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime

from fastapi import HTTPException

from .db import utcnow, write_tx, get_conn
from .models import (
    ACTION_COMPLETE,
    ACTION_PAUSE,
    ACTION_PROMOTE,
    ACTION_ROLLBACK,
    EXEC_FAILED,
    EXEC_NOT_REQUIRED,
    EXEC_PENDING,
    EXEC_SUCCEEDED,
    CreateRolloutRequest,
    DecisionResultRequest,
    ReplicaStatusRequest,
    WindowMetricsRequest,
)

TERMINAL_STATUSES = {"completed", "rolled_back", "failed"}
# Statuses that indicate at least one promote has taken effect.
PROMOTED_STATUSES = {"promoting", "paused"}
# Decisions that consume a generation.
GENERATION_CONSUMING = {ACTION_PROMOTE, ACTION_ROLLBACK, ACTION_COMPLETE}


# ---------------------------------------------------------------------------
# row -> view mapping
# ---------------------------------------------------------------------------

def rollout_view(row) -> dict:
    return {
        "id": row["id"],
        "service": row["service"],
        "target_version": row["target_version"],
        "total_replicas": row["total_replicas"],
        "max_replicas_per_batch": row["max_replicas_per_batch"],
        "min_stable_window_seconds": row["min_stable_window_seconds"],
        "error_rate_threshold": row["error_rate_threshold"],
        "status": row["status"],
        "current_generation": row["current_generation"],
        "current_replicas": row["current_replicas"],
        "pending_decision_id": row["pending_decision_id"],
        "last_decision_id": row["last_decision_id"],
        "failure_message": row["failure_message"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def decision_view(row) -> dict:
    return {
        "id": row["id"],
        "rollout_id": row["rollout_id"],
        "generation": row["generation"],
        "action": row["action"],
        "from_replicas": row["from_replicas"],
        "to_replicas": row["to_replicas"],
        "reason": row["reason"],
        "detail": json.loads(row["detail_json"]),
        "execution_status": row["execution_status"],
        "execution_detail": row["execution_detail"],
        "created_at": row["created_at"],
        "decided_at": row["decided_at"],
        "executed_at": row["executed_at"],
    }


def _get_rollout(conn, rollout_id: str):
    row = conn.execute(
        "SELECT * FROM releases WHERE id = ?", (rollout_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"rollout '{rollout_id}' not found")
    return row


def _generation_consumed(conn, rollout_id: str, generation: int) -> bool:
    """True if an effective promote/rollback/complete decision exists for gen.

    Failed decisions do not count: the state never moved, so after an operator
    resumes a failed promote the same generation may be evaluated again.
    """
    placeholders = ",".join("?" for _ in GENERATION_CONSUMING)
    row = conn.execute(
        f"""
        SELECT 1 FROM decisions
        WHERE rollout_id = ? AND generation = ?
          AND action IN ({placeholders})
          AND execution_status != ?
        LIMIT 1
        """,
        (rollout_id, generation, *GENERATION_CONSUMING, EXEC_FAILED),
    ).fetchone()
    return row is not None


def _insert_decision(
    conn,
    rollout_id: str,
    generation: int,
    action: str,
    from_replicas: int,
    to_replicas: int,
    reason: str,
    detail: dict,
    execution_status: str = EXEC_PENDING,
) -> str:
    decision_id = f"dec_{uuid.uuid4().hex}"
    now = utcnow()
    conn.execute(
        """
        INSERT INTO decisions (
            id, rollout_id, generation, action, from_replicas, to_replicas,
            reason, detail_json, execution_status, execution_detail,
            created_at, decided_at, executed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, NULL)
        """,
        (
            decision_id, rollout_id, generation, action, from_replicas,
            to_replicas, reason, json.dumps(detail, ensure_ascii=False),
            execution_status, now, now,
        ),
    )
    return decision_id


# ---------------------------------------------------------------------------
# rollout lifecycle
# ---------------------------------------------------------------------------

def create_rollout(req: CreateRolloutRequest) -> dict:
    if req.max_replicas_per_batch > req.total_replicas:
        raise HTTPException(
            status_code=422,
            detail="max_replicas_per_batch cannot exceed total_replicas",
        )
    rollout_id = f"rl_{uuid.uuid4().hex[:16]}"
    now = utcnow()
    with write_tx() as conn:
        conn.execute(
            """
            INSERT INTO releases (
                id, service, target_version, total_replicas,
                max_replicas_per_batch, min_stable_window_seconds,
                error_rate_threshold, status, current_generation,
                current_replicas, pending_decision_id, last_decision_id,
                failure_message, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, NULL, NULL, NULL, ?, ?)
            """,
            (
                rollout_id, req.service, req.target_version, req.total_replicas,
                req.max_replicas_per_batch, req.min_stable_window_seconds,
                req.error_rate_threshold, now, now,
            ),
        )
        return rollout_view(_get_rollout(conn, rollout_id))


def list_rollouts(limit: int = 50) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM releases ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [rollout_view(r) for r in rows]


def get_rollout(rollout_id: str) -> dict:
    with get_conn() as conn:
        return rollout_view(_get_rollout(conn, rollout_id))


def list_decisions(rollout_id: str) -> list[dict]:
    with get_conn() as conn:
        _get_rollout(conn, rollout_id)
        rows = conn.execute(
            "SELECT * FROM decisions WHERE rollout_id = ? ORDER BY created_at",
            (rollout_id,),
        ).fetchall()
        return [decision_view(r) for r in rows]


def list_observations(rollout_id: str, kind: str | None = None) -> dict:
    with get_conn() as conn:
        _get_rollout(conn, rollout_id)
        out: dict[str, list] = {}
        if kind in (None, "replicas"):
            out["replicas"] = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM replica_reports WHERE rollout_id = ? ORDER BY id",
                    (rollout_id,),
                ).fetchall()
            ]
        if kind in (None, "metrics"):
            out["metrics"] = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM metric_windows WHERE rollout_id = ? ORDER BY id",
                    (rollout_id,),
                ).fetchall()
            ]
        return out


# ---------------------------------------------------------------------------
# observation ingestion
# ---------------------------------------------------------------------------

def _stale_ack(rollout_id: str, generation: int, current: int, reason: str) -> dict:
    return {
        "rollout_id": rollout_id,
        "generation": generation,
        "current_generation": current,
        "ignored": True,
        "reason": reason,
        "stored": True,
    }


def ingest_replica(rollout_id: str, req: ReplicaStatusRequest) -> dict:
    with write_tx() as conn:
        rollout = _get_rollout(conn, rollout_id)
        current = rollout["current_generation"]
        now = utcnow()
        if req.generation > current:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"generation {req.generation} is ahead of current "
                    f"{current}; reports must not reference future generations"
                ),
            )
        if req.generation < current:
            reason = (
                f"stale generation {req.generation} < current {current}; "
                "recorded but ignored"
            )
            conn.execute(
                """
                INSERT INTO replica_reports (
                    rollout_id, generation, replica_id, state, observed_version,
                    ignored, ignore_reason, received_at
                ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (rollout_id, req.generation, req.replica_id, req.state,
                 req.observed_version, reason, now),
            )
            return _stale_ack(rollout_id, req.generation, current, reason)

        conn.execute(
            """
            INSERT INTO replica_reports (
                rollout_id, generation, replica_id, state, observed_version,
                ignored, ignore_reason, received_at
            ) VALUES (?, ?, ?, ?, ?, 0, NULL, ?)
            """,
            (rollout_id, req.generation, req.replica_id, req.state,
             req.observed_version, now),
        )
        conn.execute(
            "UPDATE releases SET updated_at = ? WHERE id = ?", (now, rollout_id)
        )
        return {
            "rollout_id": rollout_id,
            "generation": req.generation,
            "current_generation": current,
            "ignored": False,
            "reason": None,
            "stored": True,
        }


def ingest_metrics(rollout_id: str, req: WindowMetricsRequest) -> dict:
    # Validate timestamp up front so bad data never reaches the tables.
    try:
        started = datetime.fromisoformat(req.window_started_at)
        if started.tzinfo is None:
            raise ValueError("timezone is required (use ISO-8601 UTC)")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid window_started_at: {exc}")
    if req.error_count > req.request_count:
        raise HTTPException(
            status_code=422,
            detail="error_count cannot exceed request_count",
        )
    with write_tx() as conn:
        rollout = _get_rollout(conn, rollout_id)
        current = rollout["current_generation"]
        now = utcnow()
        if req.generation > current:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"generation {req.generation} is ahead of current "
                    f"{current}; reports must not reference future generations"
                ),
            )
        if req.generation < current:
            reason = (
                f"stale generation {req.generation} < current {current}; "
                "recorded but ignored"
            )
            conn.execute(
                """
                INSERT INTO metric_windows (
                    rollout_id, generation, window_started_at, window_seconds,
                    request_count, error_count, ignored, ignore_reason, received_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (rollout_id, req.generation, req.window_started_at,
                 req.window_seconds, req.request_count, req.error_count,
                 reason, now),
            )
            return _stale_ack(rollout_id, req.generation, current, reason)

        conn.execute(
            """
            INSERT INTO metric_windows (
                rollout_id, generation, window_started_at, window_seconds,
                request_count, error_count, ignored, ignore_reason, received_at
            ) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, ?)
            """,
            (rollout_id, req.generation, req.window_started_at,
             req.window_seconds, req.request_count, req.error_count, now),
        )
        conn.execute(
            "UPDATE releases SET updated_at = ? WHERE id = ?", (now, rollout_id)
        )
        return {
            "rollout_id": rollout_id,
            "generation": req.generation,
            "current_generation": current,
            "ignored": False,
            "reason": None,
            "stored": True,
        }


# ---------------------------------------------------------------------------
# observation aggregation (current generation, non-ignored rows only)
# ---------------------------------------------------------------------------

def _current_replica_states(conn, rollout_id: str, generation: int) -> dict[str, str]:
    """Latest reported state per replica for the given generation."""
    rows = conn.execute(
        """
        SELECT replica_id, MAX(id) AS max_id
        FROM replica_reports
        WHERE rollout_id = ? AND generation = ? AND ignored = 0
        GROUP BY replica_id
        """,
        (rollout_id, generation),
    ).fetchall()
    latest: dict[str, str] = {}
    for r in rows:
        detail = conn.execute(
            "SELECT state FROM replica_reports WHERE id = ?", (r["max_id"],)
        ).fetchone()
        latest[r["replica_id"]] = detail["state"]
    return latest


def _current_metrics(conn, rollout_id: str, generation: int) -> dict:
    """Aggregate metrics for one generation.

    Window length is the longest single reported window; the caller decides
    whether it covers the minimum stable window.
    """
    rows = conn.execute(
        """
        SELECT window_seconds, request_count, error_count
        FROM metric_windows
        WHERE rollout_id = ? AND generation = ? AND ignored = 0
        ORDER BY id
        """,
        (rollout_id, generation),
    ).fetchall()
    total_requests = sum(r["request_count"] for r in rows)
    total_errors = sum(r["error_count"] for r in rows)
    error_rate = (total_errors / total_requests) if total_requests else 0.0
    return {
        "window_count": len(rows),
        "total_requests": total_requests,
        "total_errors": total_errors,
        "error_rate": error_rate,
        "max_window_seconds": max((r["window_seconds"] for r in rows), default=0.0),
    }


# ---------------------------------------------------------------------------
# evaluation / decisions
# ---------------------------------------------------------------------------

def evaluate(rollout_id: str) -> dict:
    """Make at most one new decision from the current generation's data.

    Priority: rollback (threshold breach) > complete > promote > hold(pause).
    Hold/pause decisions do not consume the generation.
    """
    with write_tx() as conn:
        rollout = _get_rollout(conn, rollout_id)
        view = rollout_view(rollout)

        if rollout["pending_decision_id"]:
            pending = conn.execute(
                "SELECT * FROM decisions WHERE id = ?",
                (rollout["pending_decision_id"],),
            ).fetchone()
            return {
                "rollout": view,
                "decision": decision_view(pending),
                "evaluated": False,
                "message": (
                    "a decision is already pending execution; report its result "
                    "before evaluating again"
                ),
            }

        if rollout["status"] in TERMINAL_STATUSES:
            return {
                "rollout": view,
                "decision": None,
                "evaluated": False,
                "message": f"rollout is terminal ({rollout['status']})",
            }

        generation = rollout["current_generation"]
        if _generation_consumed(conn, rollout_id, generation):
            return {
                "rollout": view,
                "decision": None,
                "evaluated": False,
                "message": (
                    f"generation {generation} already has a committed "
                    "promote/rollback/complete decision"
                ),
            }

        replicas = _current_replica_states(conn, rollout_id, generation)
        metrics = _current_metrics(conn, rollout_id, generation)
        ready = sum(1 for s in replicas.values() if s == "ready")
        unready = sum(1 for s in replicas.values() if s == "unready")
        from_replicas = rollout["current_replicas"]
        threshold = rollout["error_rate_threshold"]
        min_window = rollout["min_stable_window_seconds"]
        total = rollout["total_replicas"]
        batch_cap = rollout["max_replicas_per_batch"]
        to_replicas = min(total, from_replicas + batch_cap)

        detail = {
            "generation": generation,
            "ready_replicas": ready,
            "unready_replicas": unready,
            "reported_replicas": len(replicas),
            "committed_replicas": from_replicas,
            "metrics": metrics,
            "threshold": threshold,
            "min_stable_window_seconds": min_window,
        }

        # 1) Hard rollback on error-rate threshold breach (needs real traffic).
        if metrics["total_requests"] > 0 and metrics["error_rate"] > threshold:
            reason = (
                f"error rate {metrics['error_rate']:.4f} exceeds threshold "
                f"{threshold:.4f}"
            )
            decision_id = _insert_decision(
                conn, rollout_id, generation, ACTION_ROLLBACK,
                from_replicas, 0, reason, detail,
            )
            conn.execute(
                """
                UPDATE releases
                SET pending_decision_id = ?, last_decision_id = ?,
                    failure_message = ?, updated_at = ?
                WHERE id = ?
                """,
                (decision_id, decision_id, reason, utcnow(), rollout_id),
            )
            return _eval_response(conn, rollout_id, decision_id, reason)

        # 2) Completion: last batch took effect, every replica ready.
        if (
            rollout["status"] in PROMOTED_STATUSES
            and from_replicas >= total
            and ready >= total
            and unready == 0
            and metrics["max_window_seconds"] >= min_window
        ):
            reason = (
                f"all {total} replicas ready on target version with a stable window"
            )
            decision_id = _insert_decision(
                conn, rollout_id, generation, ACTION_COMPLETE,
                from_replicas, total, reason, detail,
                execution_status=EXEC_NOT_REQUIRED,
            )
            now = utcnow()
            conn.execute(
                """
                UPDATE releases
                SET status = 'completed', pending_decision_id = NULL,
                    last_decision_id = ?, failure_message = NULL, updated_at = ?
                WHERE id = ?
                """,
                (decision_id, now, rollout_id),
            )
            conn.execute(
                "UPDATE decisions SET executed_at = ? WHERE id = ?",
                (now, decision_id),
            )
            return _eval_response(conn, rollout_id, decision_id, reason)

        # 3) Promote: stable window, all-ready, cap not yet reached.
        window_ok = metrics["max_window_seconds"] >= min_window
        healthy = unready == 0
        if window_ok and healthy and from_replicas < total:
            reason = (
                f"stable for {metrics['max_window_seconds']:.0f}s "
                f"(>= {min_window}s) with error rate "
                f"{metrics['error_rate']:.4f} <= {threshold:.4f}; "
                f"promote {from_replicas} -> {to_replicas} replicas"
            )
            decision_id = _insert_decision(
                conn, rollout_id, generation, ACTION_PROMOTE,
                from_replicas, to_replicas, reason, detail,
            )
            conn.execute(
                """
                UPDATE releases
                SET pending_decision_id = ?, last_decision_id = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (decision_id, decision_id, utcnow(), rollout_id),
            )
            return _eval_response(conn, rollout_id, decision_id, reason)

        # 4) Otherwise hold (pause) and explain what is blocking promotion.
        blockers = []
        if not window_ok:
            blockers.append(
                f"no window of {min_window}s yet "
                f"(best {metrics['max_window_seconds']:.0f}s)"
            )
        if not healthy:
            blockers.append(f"{unready} unready replica(s)")
        if metrics["total_requests"] == 0:
            blockers.append("no traffic metrics reported")
        reason = "holding: " + "; ".join(blockers) if blockers else "holding"
        decision_id = _insert_decision(
            conn, rollout_id, generation, ACTION_PAUSE,
            from_replicas, from_replicas, reason, detail,
            execution_status=EXEC_NOT_REQUIRED,
        )
        now = utcnow()
        # Hold does not move replicas and does not consume the generation.
        conn.execute(
            """
            UPDATE releases
            SET status = 'paused', pending_decision_id = NULL,
                last_decision_id = ?, updated_at = ?
            WHERE id = ?
            """,
            (decision_id, now, rollout_id),
        )
        conn.execute(
            "UPDATE decisions SET executed_at = ? WHERE id = ?",
            (now, decision_id),
        )
        return _eval_response(conn, rollout_id, decision_id, reason)


def _eval_response(conn, rollout_id: str, decision_id: str, message: str) -> dict:
    rollout = _get_rollout(conn, rollout_id)
    decision = conn.execute(
        "SELECT * FROM decisions WHERE id = ?", (decision_id,)
    ).fetchone()
    return {
        "rollout": rollout_view(rollout),
        "decision": decision_view(decision),
        "evaluated": True,
        "message": message,
    }


# ---------------------------------------------------------------------------
# execution result callback
# ---------------------------------------------------------------------------

def report_execution(
    rollout_id: str, decision_id: str, req: DecisionResultRequest
) -> dict:
    with write_tx() as conn:
        rollout = _get_rollout(conn, rollout_id)
        decision = conn.execute(
            "SELECT * FROM decisions WHERE id = ? AND rollout_id = ?",
            (decision_id, rollout_id),
        ).fetchone()
        if decision is None:
            raise HTTPException(
                status_code=404,
                detail=f"decision '{decision_id}' not found for this rollout",
            )
        if decision["execution_status"] != EXEC_PENDING:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"decision already {decision['execution_status']}; "
                    "committed results cannot be rewritten"
                ),
            )

        now = utcnow()
        if req.status == EXEC_SUCCEEDED:
            action = decision["action"]
            if action == ACTION_PROMOTE:
                # Generation advances: the new revision is now live on
                # to_replicas replicas.
                conn.execute(
                    """
                    UPDATE releases
                    SET status = 'promoting',
                        current_replicas = ?,
                        current_generation = current_generation + 1,
                        pending_decision_id = NULL,
                        failure_message = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (decision["to_replicas"], now, rollout_id),
                )
            elif action == ACTION_ROLLBACK:
                # Platform confirmed traffic shifted back to the last good
                # revision; rollout reaches its terminal state directly.
                conn.execute(
                    """
                    UPDATE releases
                    SET status = 'rolled_back',
                        current_replicas = 0,
                        current_generation = current_generation + 1,
                        pending_decision_id = NULL,
                        failure_message = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, rollout_id),
                )
            else:
                raise HTTPException(
                    status_code=422,
                    detail=f"action '{action}' does not take execution results",
                )
            conn.execute(
                "UPDATE decisions SET execution_status = ?, execution_detail = ?, "
                "executed_at = ? WHERE id = ?",
                (EXEC_SUCCEEDED, req.detail, now, decision_id),
            )
        else:
            # Execution failed: keep the previous executable state. Neither
            # current_replicas nor current_generation moves; detach the failed
            # decision and stop automation pending operator intervention.
            conn.execute(
                "UPDATE decisions SET execution_status = ?, execution_detail = ?, "
                "executed_at = ? WHERE id = ?",
                (EXEC_FAILED, req.detail, now, decision_id),
            )
            if decision["action"] == ACTION_ROLLBACK:
                # A failed rollback is not safely auto-recoverable.
                new_status = "failed"
                failure = (
                    f"rollback execution failed: {req.detail or 'no detail'}; "
                    "previous state retained, manual intervention required"
                )
            else:
                new_status = "paused"
                failure = (
                    f"promote execution failed: {req.detail or 'no detail'}; "
                    "previous replica count retained"
                )
            conn.execute(
                """
                UPDATE releases
                SET status = ?, pending_decision_id = NULL,
                    failure_message = ?, updated_at = ?
                WHERE id = ?
                """,
                (new_status, failure, now, rollout_id),
            )

        rollout = _get_rollout(conn, rollout_id)
        updated = conn.execute(
            "SELECT * FROM decisions WHERE id = ?", (decision_id,)
        ).fetchone()
        return {
            "rollout": rollout_view(rollout),
            "decision": decision_view(updated),
            "evaluated": False,
            "message": f"execution result '{req.status}' recorded",
        }


# ---------------------------------------------------------------------------
# operator actions
# ---------------------------------------------------------------------------

def pause(rollout_id: str, reason: str | None) -> dict:
    with write_tx() as conn:
        rollout = _get_rollout(conn, rollout_id)
        if rollout["status"] in TERMINAL_STATUSES:
            raise HTTPException(
                status_code=409,
                detail=f"cannot pause a terminal rollout ({rollout['status']})",
            )
        if rollout["pending_decision_id"]:
            raise HTTPException(
                status_code=409,
                detail="a decision is pending execution; report its result first",
            )
        decision_id = _insert_decision(
            conn, rollout_id, rollout["current_generation"], ACTION_PAUSE,
            rollout["current_replicas"], rollout["current_replicas"],
            reason or "manual pause by operator",
            {"manual": True},
            execution_status=EXEC_NOT_REQUIRED,
        )
        now = utcnow()
        conn.execute(
            """
            UPDATE releases SET status = 'paused', last_decision_id = ?,
                updated_at = ? WHERE id = ?
            """,
            (decision_id, now, rollout_id),
        )
        conn.execute(
            "UPDATE decisions SET executed_at = ? WHERE id = ?",
            (now, decision_id),
        )
        return {
            "rollout": rollout_view(_get_rollout(conn, rollout_id)),
            "decision": decision_view(
                conn.execute(
                    "SELECT * FROM decisions WHERE id = ?", (decision_id,)
                ).fetchone()
            ),
            "evaluated": False,
            "message": "rollout paused",
        }


def resume(rollout_id: str) -> dict:
    """Clear an automation hold so evaluation can continue.

    A 'failed' rollout (failed rollback) stays terminal for manual handling.
    A paused rollout with no pending decision returns to 'promoting', or to
    'pending' before the first batch has ever been applied.
    """
    with write_tx() as conn:
        rollout = _get_rollout(conn, rollout_id)
        if rollout["status"] != "paused":
            raise HTTPException(
                status_code=409,
                detail=f"only paused rollouts can be resumed (is {rollout['status']})",
            )
        if rollout["pending_decision_id"]:
            raise HTTPException(
                status_code=409, detail="pending decision must be resolved first"
            )
        new_status = "promoting" if rollout["current_replicas"] > 0 else "pending"
        now = utcnow()
        conn.execute(
            "UPDATE releases SET status = ?, failure_message = NULL, "
            "updated_at = ? WHERE id = ?",
            (new_status, now, rollout_id),
        )
        return rollout_view(_get_rollout(conn, rollout_id))
