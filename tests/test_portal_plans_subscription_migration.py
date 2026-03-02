"""Tests for portal plans/subscriptions migration (0025)."""

from pathlib import Path
import sqlite3


def apply_sql(conn: sqlite3.Connection, path: Path) -> None:
    conn.executescript(path.read_text())


def test_portal_subscriptions_table_and_constraints() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")

    apply_sql(conn, Path("migrations/0010_step1_keyword_research.sql"))
    apply_sql(conn, Path("migrations/0025_portal_plans_and_subscriptions.sql"))

    conn.execute(
        """
        INSERT INTO wp_ai_seo_sites (
          site_id, site_url, site_name, business_address, primary_location_hint, site_type_hint,
          local_mode, input_json, site_profile_json, last_analysis_at, last_research_at, created_at, updated_at
        ) VALUES (
          'site_1', 'https://example.com', 'Example', NULL, NULL, NULL,
          0, '{}', '{}', 0, NULL, 0, 0
        )
        """
    )

    conn.execute(
        """
        INSERT INTO billing_site_subscriptions (
          subscription_id, site_id, plan_key, company_name, contact_email, source, status, notes_json
        ) VALUES (
          'sub_1', 'site_1', 'growth', 'Example Co', 'owner@example.com', 'portal', 'lead', '{}'
        )
        """
    )

    row = conn.execute(
        "SELECT site_id, plan_key, contact_email, status FROM billing_site_subscriptions WHERE subscription_id='sub_1'"
    ).fetchone()
    assert row == ("site_1", "growth", "owner@example.com", "lead")

    try:
        conn.execute(
            "INSERT INTO billing_site_subscriptions (subscription_id, plan_key, contact_email) VALUES ('sub_2','bad','x@y.com')"
        )
        assert False, "Expected plan_key check constraint"
    except sqlite3.IntegrityError:
        pass

    fk_rows = list(conn.execute("PRAGMA foreign_key_check"))
    assert fk_rows == []
