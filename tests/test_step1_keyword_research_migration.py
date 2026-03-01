"""Focused tests for Step 1 keyword research migration."""

from pathlib import Path
import sqlite3


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "migrations" / "0010_step1_keyword_research.sql"
)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(MIGRATION_PATH.read_text())
    return conn


def test_step1_tables_exist():
    conn = _connect()
    try:
        tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table'
                  AND name IN (
                    'wp_ai_seo_sites',
                    'wp_ai_seo_keywords',
                    'wp_ai_seo_keyword_metrics',
                    'wp_ai_seo_selections'
                  )
                """
            )
        }
        assert tables == {
            "wp_ai_seo_sites",
            "wp_ai_seo_keywords",
            "wp_ai_seo_keyword_metrics",
            "wp_ai_seo_selections",
        }
    finally:
        conn.close()


def test_step1_keyword_metrics_fk_and_selection_uniqueness():
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO wp_ai_seo_sites (
              site_id, site_url, input_json, site_profile_json,
              last_analysis_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "site_1",
                "https://example.com",
                "{}",
                '{"local_mode":false}',
                1735689600000,
                1735689600000,
                1735689600000,
            ),
        )
        conn.execute(
            """
            INSERT INTO wp_ai_seo_keywords (
              keyword_id, site_id, research_run_id, keyword, keyword_norm, cluster, intent, local_intent,
              page_type, recommended_slug, opportunity_score, selected_tier, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "kw_1",
                "site_1",
                "krr_1",
                "plumber near me",
                "plumber near me",
                "plumber",
                "transactional",
                "yes",
                "location landing",
                "locations/plumber-near-me",
                73.2,
                "primary",
                1735689600000,
                1735689600000,
            ),
        )
        conn.execute(
            """
            INSERT INTO wp_ai_seo_keyword_metrics (
              metric_id, keyword_id, volume_us, kd, cpc, competitive_density, serp_features_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "met_1",
                "kw_1",
                1700,
                42.5,
                12.6,
                0.83,
                '["local_pack","ads_top"]',
                1735689600000,
            ),
        )
        conn.execute(
            """
            INSERT INTO wp_ai_seo_selections (
              selection_id, site_id, research_run_id, selection_type, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("sel_1", "site_1", "krr_1", "keyword_research_results", '{"ok":true}', 1735689600000),
        )
        # Unique constraint on site+run+selection_type should reject duplicate row.
        try:
            conn.execute(
                """
                INSERT INTO wp_ai_seo_selections (
                  selection_id, site_id, research_run_id, selection_type, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("sel_2", "site_1", "krr_1", "keyword_research_results", '{"ok":true}', 1735689601000),
            )
            assert False, "Expected sqlite3.IntegrityError for duplicate selection"
        except sqlite3.IntegrityError:
            pass
    finally:
        conn.close()
