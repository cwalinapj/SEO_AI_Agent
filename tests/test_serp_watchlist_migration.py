"""Focused tests for SERP watchlist migration."""

from pathlib import Path
import sqlite3

import pytest


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "migrations" / "0007_serp_watchlist.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(MIGRATION_PATH.read_text())
    return conn


def test_watchlist_table_exists_with_expected_columns():
    conn = _connect()
    try:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info('serp_watchlist')").fetchall()
        }
        assert {
            "watch_id",
            "user_id",
            "phrase",
            "region_json",
            "device",
            "max_results",
            "active",
            "created_at",
            "updated_at",
            "last_run_at",
            "last_serp_id",
            "last_status",
            "last_error",
        }.issubset(columns)
    finally:
        conn.close()


def test_watchlist_unique_scope_enforces_single_row_per_keyword_region_device():
    conn = _connect()
    try:
        row = (
            "watch_1",
            "user_1",
            "plumber near me",
            '{"country":"US","language":"en"}',
            "desktop",
            20,
            1,
            1735689600000,
            1735689600000,
        )
        conn.execute(
            """
            INSERT INTO serp_watchlist (
              watch_id, user_id, phrase, region_json, device,
              max_results, active, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row,
        )

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO serp_watchlist (
                  watch_id, user_id, phrase, region_json, device,
                  max_results, active, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "watch_2",
                    "user_1",
                    "plumber near me",
                    '{"country":"US","language":"en"}',
                    "desktop",
                    20,
                    1,
                    1735689601000,
                    1735689601000,
                ),
            )
    finally:
        conn.close()


def test_watchlist_active_query_and_last_run_fields():
    conn = _connect()
    try:
        conn.executemany(
            """
            INSERT INTO serp_watchlist (
              watch_id, user_id, phrase, region_json, device,
              max_results, active, created_at, updated_at, last_run_at, last_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "watch_active",
                    "user_1",
                    "plumber near me",
                    '{"country":"US"}',
                    "desktop",
                    20,
                    1,
                    1735689600000,
                    1735689600000,
                    None,
                    None,
                ),
                (
                    "watch_inactive",
                    "user_1",
                    "plumbers in minden, nv",
                    '{"country":"US"}',
                    "desktop",
                    20,
                    0,
                    1735689600000,
                    1735689600000,
                    1735776000000,
                    "ok",
                ),
            ],
        )

        active_rows = conn.execute(
            "SELECT watch_id FROM serp_watchlist WHERE user_id = 'user_1' AND active = 1"
        ).fetchall()
        assert active_rows == [("watch_active",)]

        row = conn.execute(
            "SELECT last_run_at, last_status FROM serp_watchlist WHERE watch_id = 'watch_inactive'"
        ).fetchone()
        assert row == (1735776000000, "ok")
    finally:
        conn.close()
