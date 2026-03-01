"""Focused tests for unified D1 Step2/Step3 migration."""

from pathlib import Path
import sqlite3


MIG_0002 = Path(__file__).resolve().parents[1] / "migrations" / "0002_serp.sql"
MIG_0004 = Path(__file__).resolve().parents[1] / "migrations" / "0004_pagespeed_monitoring.sql"
MIG_0015 = Path(__file__).resolve().parents[1] / "migrations" / "0015_unified_d1_step2_step3.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(MIG_0002.read_text())
    conn.executescript(MIG_0004.read_text())
    conn.executescript(MIG_0015.read_text())
    return conn


def test_unified_tables_exist():
    conn = _connect()
    try:
        expected = {
            "site_signals",
            "site_briefs",
            "keyword_sets",
            "keyword_metrics",
            "site_runs",
            "site_run_jobs",
            "urls",
            "page_snapshots",
            "backlink_url_metrics",
            "backlink_domain_metrics",
            "internal_graph_runs",
            "internal_graph_url_stats",
            "tasks",
            "task_status_events",
            "task_board_cache",
            "competitor_sets",
            "competitor_social_profiles",
        }
        rows = {
            row[0]
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                """
            )
        }
        assert expected.issubset(rows)
    finally:
        conn.close()


def test_keywords_cap_trigger_enforced_per_keyword_set():
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
        for i in range(20):
            conn.execute(
                """
                INSERT INTO keywords (
                  kw_id, user_id, phrase, region_json, created_at,
                  keyword_set_id, keyword, keyword_norm, priority, cluster
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"kw_{i}",
                    "user_1",
                    f"keyword phrase {i}",
                    '{"country":"US"}',
                    1735689600 + i,
                    "kset_1",
                    f"keyword phrase {i}",
                    f"keyword phrase {i}",
                    1 if i < 10 else 2,
                    "cluster-a",
                ),
            )

        try:
            conn.execute(
                """
                INSERT INTO keywords (
                  kw_id, user_id, phrase, region_json, created_at,
                  keyword_set_id, keyword, keyword_norm, priority, cluster
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "kw_21",
                    "user_1",
                    "keyword phrase 21",
                    '{"country":"US"}',
                    1735689700,
                    "kset_1",
                    "keyword phrase 21",
                    "keyword phrase 21",
                    2,
                    "cluster-a",
                ),
            )
            assert False, "Expected sqlite3.IntegrityError for >20 keywords in a set"
        except sqlite3.IntegrityError:
            pass
    finally:
        conn.close()
