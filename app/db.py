"""SQLite persistence layer.

All state lives in SQLite so that a process restart can resume an in-flight
rollout. Connections are opened per request; a process-wide lock serialises
writers and keeps decision/state transitions atomic.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

DB_PATH = os.environ.get("CANARY_DB_PATH", "/data/canary.db")

# One lock per database file, shared across requests in this process.
_WRITE_LOCK = threading.RLock()
_WRITE_LOCKS: dict[str, threading.RLock] = {}
_LOCK_GUARD = threading.Lock()

DDL = """
CREATE TABLE IF NOT EXISTS releases (
    id TEXT PRIMARY KEY,
    service TEXT NOT NULL,
    target_version TEXT NOT NULL,
    total_replicas INTEGER NOT NULL,
    max_replicas_per_batch INTEGER NOT NULL,
    min_stable_window_seconds INTEGER NOT NULL,
    error_rate_threshold REAL NOT NULL,
    status TEXT NOT NULL,
    current_generation INTEGER NOT NULL DEFAULT 0,
    current_replicas INTEGER NOT NULL DEFAULT 0,
    pending_decision_id TEXT,
    last_decision_id TEXT,
    failure_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS replica_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rollout_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    replica_id TEXT NOT NULL,
    state TEXT NOT NULL,
    observed_version TEXT,
    ignored INTEGER NOT NULL DEFAULT 0,
    ignore_reason TEXT,
    received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_replica_rollout_gen
    ON replica_reports(rollout_id, generation);

CREATE TABLE IF NOT EXISTS metric_windows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rollout_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    window_started_at TEXT NOT NULL,
    window_seconds REAL NOT NULL,
    request_count INTEGER NOT NULL,
    error_count INTEGER NOT NULL,
    ignored INTEGER NOT NULL DEFAULT 0,
    ignore_reason TEXT,
    received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_metrics_rollout_gen
    ON metric_windows(rollout_id, generation);

CREATE TABLE IF NOT EXISTS decisions (
    id TEXT PRIMARY KEY,
    rollout_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    action TEXT NOT NULL,
    from_replicas INTEGER NOT NULL,
    to_replicas INTEGER NOT NULL,
    reason TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    execution_status TEXT NOT NULL,
    execution_detail TEXT,
    created_at TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    executed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_decisions_rollout
    ON decisions(rollout_id, created_at);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(db_path: str | None = None) -> None:
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with _connect(path) as conn:
        conn.executescript(DDL)


def get_write_lock(db_path: str | None = None) -> threading.RLock:
    path = os.path.abspath(db_path or DB_PATH)
    with _LOCK_GUARD:
        lock = _WRITE_LOCKS.get(path)
        if lock is None:
            lock = threading.RLock()
            _WRITE_LOCKS[path] = lock
        return lock


@contextmanager
def get_conn(db_path: str | None = None) -> Iterator[sqlite3.Connection]:
    """Connection without taking the write lock (read-only call paths)."""
    path = db_path or DB_PATH
    conn = _connect(path)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def write_tx(db_path: str | None = None) -> Iterator[sqlite3.Connection]:
    """Serialised, atomic read-modify-write transaction.

    BEGIN IMMEDIATE acquires the SQLite reserved lock up front so two
    application workers (or a restarted process) cannot interleave
    decision/state updates.
    """
    path = db_path or DB_PATH
    lock = get_write_lock(path)
    with lock:
        conn = _connect(path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
