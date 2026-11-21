"""Existing SQLite databases must work without a migration step."""
from __future__ import annotations

import os
import sqlite3
import tempfile

from app.db import init_db

# DDL as shipped before the fingerprint indexes existed: identical tables and
# original indexes only. A database file created with it is the legacy state.
LEGACY_DDL = """
CREATE TABLE releases (
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
CREATE TABLE replica_reports (
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
CREATE INDEX idx_replica_rollout_gen
    ON replica_reports(rollout_id, generation);
CREATE TABLE metric_windows (
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
CREATE INDEX idx_metrics_rollout_gen
    ON metric_windows(rollout_id, generation);
CREATE TABLE decisions (
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
CREATE INDEX idx_decisions_rollout
    ON decisions(rollout_id, created_at);
"""


def _index_names(path: str) -> set[str]:
    conn = sqlite3.connect(path)
    try:
        return {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'index'"
            ).fetchall()
        }
    finally:
        conn.close()


def test_existing_database_gets_fingerprint_indexes_idempotently():
    tmp = tempfile.mkdtemp(prefix="canary-legacy-")
    legacy_path = os.path.join(tmp, "legacy.db")

    conn = sqlite3.connect(legacy_path)
    conn.executescript(LEGACY_DDL)
    conn.execute(
        "INSERT INTO releases VALUES "
        "('rl_x','svc','v1',4,2,30,0.05,'pending',0,0,NULL,NULL,NULL,"
        "'2026-11-21T00:00:00+00:00','2026-11-21T00:00:00+00:00')"
    )
    conn.commit()
    conn.close()

    indexes_before = _index_names(legacy_path)
    assert "idx_replica_fingerprint" not in indexes_before
    assert "idx_metrics_fingerprint" not in indexes_before

    # New release starts against the old file: init_db adds the indexes
    # without touching existing data; running it again stays idempotent.
    init_db(legacy_path)
    init_db(legacy_path)

    indexes_after = _index_names(legacy_path)
    assert "idx_replica_fingerprint" in indexes_after
    assert "idx_metrics_fingerprint" in indexes_after

    check = sqlite3.connect(legacy_path)
    row = check.execute(
        "SELECT service, status, current_generation FROM releases WHERE id = 'rl_x'"
    ).fetchone()
    check.close()
    assert row == ("svc", "pending", 0)
