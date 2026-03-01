"""Focused tests for Step 3 local-service execution migration."""

from pathlib import Path
import sqlite3


MIG_0011 = Path(__file__).resolve().parents[1] / "migrations" / "0011_step2_daily_harvest.sql"
MIG_0013 = Path(__file__).resolve().parents[1] / "migrations" / "0013_step2_baseline_delta.sql"
MIG_0014 = Path(__file__).resolve().parents[1] / "migrations" / "0014_step3_local_service_engine.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(MIG_0011.read_text())
    conn.executescript(MIG_0013.read_text())
    conn.executescript(MIG_0014.read_text())
    return conn


def test_step3_tables_exist():
    conn = _connect()
    try:
        tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table'
                  AND name IN (
                    'step3_runs',
                    'step3_tasks',
                    'step3_competitors',
                    'step3_social_signals',
                    'step3_risk_flags',
                    'step3_reports'
                  )
                """
            )
        }
        assert tables == {
            "step3_runs",
            "step3_tasks",
            "step3_competitors",
            "step3_social_signals",
            "step3_risk_flags",
            "step3_reports",
        }
    finally:
        conn.close()


def test_step3_task_mode_and_fk_constraints():
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO step3_runs (
              run_id, site_id, date_yyyymmdd, source_step2_date, status, summary_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "s3run_1",
                "site_1",
                "2026-02-28",
                "2026-02-28",
                "success",
                "{}",
                1735689600000,
                1735689600000,
            ),
        )

        conn.execute(
            """
            INSERT INTO step3_tasks (
              task_id, run_id, site_id, task_group, task_type, execution_mode, priority,
              title, why_text, details_json, target_slug, target_url, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "s3task_1",
                "s3run_1",
                "site_1",
                "on_site",
                "schema_upgrade",
                "auto_safe",
                1,
                "Add FAQ schema",
                "Top competitors use FAQ schema",
                "{}",
                "/water-heater-repair-los-angeles",
                "https://example.com/water-heater-repair-los-angeles",
                "planned",
                1735689600000,
            ),
        )

        try:
            conn.execute(
                """
                INSERT INTO step3_tasks (
                  task_id, run_id, site_id, task_group, task_type, execution_mode, priority,
                  title, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "s3task_2",
                    "s3run_1",
                    "site_1",
                    "authority",
                    "outreach",
                    "invalid_mode",
                    2,
                    "Bad mode",
                    "{}",
                    1735689600001,
                ),
            )
            assert False, "Expected sqlite3.IntegrityError on execution_mode CHECK"
        except sqlite3.IntegrityError:
            pass

        try:
            conn.execute(
                """
                INSERT INTO step3_tasks (
                  task_id, run_id, site_id, task_group, task_type, execution_mode, priority,
                  title, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "s3task_3",
                    "missing_run",
                    "site_1",
                    "authority",
                    "outreach",
                    "assisted",
                    2,
                    "Missing run fk",
                    "{}",
                    1735689600002,
                ),
            )
            assert False, "Expected sqlite3.IntegrityError on run_id FK"
        except sqlite3.IntegrityError:
            pass
    finally:
        conn.close()
