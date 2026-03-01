"""Focused tests for Step 2 baseline/delta migration."""

from pathlib import Path
import sqlite3


MIG_0011 = Path(__file__).resolve().parents[1] / "migrations" / "0011_step2_daily_harvest.sql"
MIG_0012 = Path(__file__).resolve().parents[1] / "migrations" / "0012_step2_cache_and_provider.sql"
MIG_0013 = Path(__file__).resolve().parents[1] / "migrations" / "0013_step2_baseline_delta.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(MIG_0011.read_text())
    conn.executescript(MIG_0012.read_text())
    conn.executescript(MIG_0013.read_text())
    return conn


def test_step2_baseline_delta_tables_exist():
    conn = _connect()
    try:
        tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table'
                  AND name IN (
                    'step2_baselines',
                    'step2_serp_diffs',
                    'step2_url_diffs',
                    'step2_daily_reports'
                  )
                """
            )
        }
        assert tables == {
            "step2_baselines",
            "step2_serp_diffs",
            "step2_url_diffs",
            "step2_daily_reports",
        }
    finally:
        conn.close()


def test_step2_daily_reports_run_type_constraint():
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO step2_daily_reports (
              report_id, site_id, date_yyyymmdd, run_type, baseline_snapshot_id, summary_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("rep_1", "site_1", "2026-02-28", "baseline", None, "{}", 1735689600000),
        )
        try:
            conn.execute(
                """
                INSERT INTO step2_daily_reports (
                  report_id, site_id, date_yyyymmdd, run_type, baseline_snapshot_id, summary_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("rep_2", "site_1", "2026-02-28", "invalid_mode", None, "{}", 1735689601000),
            )
            assert False, "Expected sqlite3.IntegrityError on run_type CHECK"
        except sqlite3.IntegrityError:
            pass
    finally:
        conn.close()
