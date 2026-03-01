"""Tests for Moz profile and usage migration."""

from pathlib import Path
import sqlite3


MIG_0010 = Path(__file__).resolve().parents[1] / "migrations" / "0010_step1_keyword_research.sql"
MIG_0018 = Path(__file__).resolve().parents[1] / "migrations" / "0018_moz_profiles_and_usage.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(MIG_0010.read_text())
    conn.executescript(MIG_0018.read_text())
    return conn


def test_moz_profile_tables_exist():
    conn = _connect()
    try:
        expected = {"moz_site_profiles", "moz_job_row_usage"}
        rows = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert expected.issubset(rows)
    finally:
        conn.close()


def test_moz_profile_defaults_and_constraints():
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO wp_ai_seo_sites (
              site_id, site_url, site_name, business_address, primary_location_hint, site_type_hint,
              local_mode, input_json, site_profile_json, last_analysis_at, last_research_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 0, '{}', '{}', 1, NULL, 1, 1)
            """,
            ("site_1", "https://example.com", "Example", None, None, None),
        )

        conn.execute("INSERT INTO moz_site_profiles (site_id) VALUES ('site_1')")
        row = conn.execute(
            "SELECT moz_profile, monthly_rows_budget, weekly_focus_url_count, daily_keyword_depth FROM moz_site_profiles WHERE site_id='site_1'"
        ).fetchone()
        assert row == ("single_site_max", 15000, 20, 20)

        try:
            conn.execute(
                "INSERT INTO moz_site_profiles (site_id, moz_profile) VALUES ('site_1', 'bad')"
            )
            assert False, "Expected integrity error for invalid profile"
        except sqlite3.IntegrityError:
            pass
    finally:
        conn.close()
