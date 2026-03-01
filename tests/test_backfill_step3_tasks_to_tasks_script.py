"""Tests for step3_tasks -> tasks backfill script."""

from pathlib import Path
import sqlite3

from scripts.backfill_step3_tasks_to_tasks import backfill_step3_tasks_to_tasks


MIG_0014 = Path(__file__).resolve().parents[1] / "migrations" / "0014_step3_local_service_engine.sql"
MIG_0004 = Path(__file__).resolve().parents[1] / "migrations" / "0004_pagespeed_monitoring.sql"
MIG_0015 = Path(__file__).resolve().parents[1] / "migrations" / "0015_unified_d1_step2_step3.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(MIG_0004.read_text())
    conn.executescript(MIG_0014.read_text())
    conn.executescript(MIG_0015.read_text())
    return conn


def test_backfill_inserts_canonical_task_and_event():
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO sites (site_id, user_id, production_url, default_strategy)
            VALUES (?, ?, ?, ?)
            """,
            ("site_1", "user_1", "https://example.com", "mobile"),
        )
        conn.execute(
            """
            INSERT INTO keyword_sets (id, site_id, source, is_active)
            VALUES (?, ?, ?, ?)
            """,
            ("kset_1", "site_1", "auto", 1),
        )
        conn.execute(
            """
            INSERT INTO site_runs (id, site_id, keyword_set_id, run_type, run_date, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("run_1", "site_1", "kset_1", "baseline", "2026-02-28", "success"),
        )
        conn.execute(
            """
            INSERT INTO step3_runs (
              run_id, site_id, date_yyyymmdd, source_step2_date, status, summary_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("s3run_1", "site_1", "2026-02-28", "2026-02-28", "success", "{}", 1735689600, 1735689600),
        )
        conn.execute(
            """
            INSERT INTO step3_tasks (
              task_id, run_id, site_id, task_group, task_type, execution_mode, priority,
              title, why_text, details_json, target_slug, target_url, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "task_legacy_1",
                "s3run_1",
                "site_1",
                "on_site",
                "faq_schema_add",
                "auto_safe",
                2,
                "Legacy FAQ task",
                "Needs FAQ schema",
                "{}",
                "/service-city",
                "https://example.com/service-city",
                "planned",
                1735689600,
            ),
        )

        result = backfill_step3_tasks_to_tasks(conn, with_events=True)
        assert result.scanned == 1
        assert result.inserted_or_updated == 1

        task_row = conn.execute(
            "SELECT id, site_id, category, type, priority, mode, status FROM tasks WHERE id = ?",
            ("task_legacy_1",),
        ).fetchone()
        assert task_row is not None
        assert task_row[0] == "task_legacy_1"
        assert task_row[1] == "site_1"
        assert task_row[2] == "ON_PAGE"
        assert task_row[4] == "P1"
        assert task_row[5] == "AUTO"
        assert task_row[6] == "READY"

        events = conn.execute(
            "SELECT COUNT(1) FROM task_status_events WHERE task_id = ?",
            ("task_legacy_1",),
        ).fetchone()
        assert events is not None
        assert events[0] == 1
    finally:
        conn.close()
