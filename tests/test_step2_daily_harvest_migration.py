"""Focused tests for Step 2 daily harvest migration."""

from pathlib import Path
import sqlite3


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "migrations" / "0011_step2_daily_harvest.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(MIGRATION_PATH.read_text())
    return conn


def test_step2_tables_exist():
    conn = _connect()
    try:
        tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                  AND name IN (
                    'step2_serp_snapshots',
                    'step2_serp_results',
                    'step2_page_extracts',
                    'step2_url_backlinks',
                    'step2_domain_backlinks',
                    'step2_internal_graph_edges',
                    'step2_ai_analyses'
                  )
                """
            )
        }
        assert tables == {
            "step2_serp_snapshots",
            "step2_serp_results",
            "step2_page_extracts",
            "step2_url_backlinks",
            "step2_domain_backlinks",
            "step2_internal_graph_edges",
            "step2_ai_analyses",
        }
    finally:
        conn.close()


def test_step2_serp_results_fk_and_page_extract_unique():
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO step2_serp_snapshots (
              serp_id, site_id, keyword, cluster, intent, geo, date_yyyymmdd, serp_features_json, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "s2serp_1",
                "site_1",
                "plumber near me",
                "plumber",
                "transactional",
                "US",
                "2026-02-28",
                "[]",
                1735689600000,
            ),
        )
        conn.execute(
            """
            INSERT INTO step2_serp_results (
              result_id, serp_id, rank, url, url_hash, domain, page_type, title_snippet, desc_snippet, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "s2res_1",
                "s2serp_1",
                1,
                "https://example.com/plumbing",
                "hash_1",
                "example.com",
                "service",
                "Plumbing Services",
                "Fast local plumber",
                1735689600000,
            ),
        )

        conn.execute(
            """
            INSERT INTO step2_page_extracts (
              extract_id, url_hash, url, domain, date_yyyymmdd, title, h2_json, h3_json, schema_types_json,
              internal_anchors_json, external_anchors_json, keyword_placement_flags_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "s2ext_1",
                "hash_1",
                "https://example.com/plumbing",
                "example.com",
                "2026-02-28",
                "Plumbing Services",
                "[]",
                "[]",
                "[]",
                "[]",
                "[]",
                "{}",
                1735689600000,
                1735689600000,
            ),
        )

        try:
            conn.execute(
                """
                INSERT INTO step2_page_extracts (
                  extract_id, url_hash, url, domain, date_yyyymmdd, title, h2_json, h3_json, schema_types_json,
                  internal_anchors_json, external_anchors_json, keyword_placement_flags_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "s2ext_2",
                    "hash_1",
                    "https://example.com/plumbing",
                    "example.com",
                    "2026-02-28",
                    "Plumbing Services",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "{}",
                    1735689601000,
                    1735689601000,
                ),
            )
            assert False, "Expected sqlite3.IntegrityError for duplicate url_hash/date row"
        except sqlite3.IntegrityError:
            pass
    finally:
        conn.close()
