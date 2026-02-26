"""Focused tests for jobs/artifacts migration."""

from pathlib import Path
import sqlite3


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "migrations" / "0008_jobs_artifacts.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(MIGRATION_PATH.read_text())
    return conn


def test_jobs_and_artifacts_tables_exist():
    conn = _connect()
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('jobs', 'artifacts')"
            )
        }
        assert tables == {"jobs", "artifacts"}
    finally:
        conn.close()


def test_job_can_store_error_and_cost_accounting():
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO jobs (
              job_id, user_id, site_id, type, status,
              attempts, max_attempts, cost_units,
              error_code, error_json, request_json,
              created_at, updated_at, started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "job_1",
                "user_1",
                "site_1",
                "serp_top20",
                "failed",
                1,
                3,
                2,
                "no_results",
                '{"message":"No SERP rows found for keyword."}',
                '{"keyword":"plumber near me"}',
                1735689600000,
                1735689601000,
                1735689600000,
                1735689601000,
            ),
        )

        row = conn.execute(
            "SELECT status, cost_units, error_code FROM jobs WHERE job_id = 'job_1'"
        ).fetchone()
        assert row == ("failed", 2, "no_results")
    finally:
        conn.close()


def test_artifact_links_to_job_with_checksum():
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO jobs (
              job_id, type, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("job_2", "speed_check", "succeeded", 1735689600000, 1735689601000),
        )

        conn.execute(
            """
            INSERT INTO artifacts (
              artifact_id, job_id, kind, r2_key, checksum, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "art_1",
                "job_2",
                "psi.response.json",
                "r2://artifacts/job_2/psi.response.json",
                "abcdef123456",
                '{"performance_score":0.82}',
                1735689601000,
            ),
        )

        row = conn.execute(
            "SELECT job_id, kind, checksum FROM artifacts WHERE artifact_id = 'art_1'"
        ).fetchone()
        assert row == ("job_2", "psi.response.json", "abcdef123456")
    finally:
        conn.close()
