"""Tests for device policy / conversion priors hardening migration (0023)."""

from pathlib import Path
import sqlite3


def apply_sql(conn: sqlite3.Connection, path: Path) -> None:
    conn.executescript(path.read_text())


def test_device_policy_conversion_priors_hardening() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")

    apply_sql(conn, Path("migrations/0002_serp.sql"))
    apply_sql(conn, Path("migrations/0003_device_policy.sql"))
    apply_sql(conn, Path("migrations/0023_device_policy_conversion_priors_fixes.sql"))

    kw_policy_cols = {
        row[1]: row[4] for row in conn.execute("PRAGMA table_info('kw_device_policy')")
    }
    assert kw_policy_cols["updated_at"] is not None

    conversion_cols = {
        row[1]: row[4] for row in conn.execute("PRAGMA table_info('conversion_events')")
    }
    assert conversion_cols["created_at"] is not None

    # New indexes for performance.
    conversion_indexes = {row[1] for row in conn.execute("PRAGMA index_list('conversion_events')")}
    assert "idx_conversion_events_cohort" in conversion_indexes
    assert "idx_conversion_events_type_time" in conversion_indexes

    policy_indexes = {row[1] for row in conn.execute("PRAGMA index_list('kw_device_policy')")}
    assert "idx_kw_device_policy_updated" in policy_indexes

    # cohort_key must be canonical string, not hex() derived.
    view_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='view' AND name='cohort_device_priors'"
    ).fetchone()[0].lower()
    assert "lower(hex(" not in view_sql
    assert "cohort_key" in view_sql

    # Trigger should still produce policy rows and include expanded fallback rule.
    trigger_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='keywords_device_policy_ai'"
    ).fetchone()[0].lower()
    assert "new.intent_bucket is null" in trigger_sql
    assert "new.price_point is null" in trigger_sql
    assert "new.business_hours_profile is null" in trigger_sql

    conn.execute(
        """
        INSERT INTO keywords (
          kw_id, user_id, phrase, region_json, created_at,
          vertical, service_model, cta_type, business_hours_profile, price_point, intent_bucket
        ) VALUES (
          'kw1', 'u1', 'water heater repair', 'us-ca', 1000,
          NULL, NULL, NULL, NULL, NULL, NULL
        )
        """
    )

    row = conn.execute(
        "SELECT mode, reason FROM kw_device_policy WHERE kw_id='kw1'"
    ).fetchone()
    assert row == ("both", "unknown_safe_fallback")

    fk_rows = list(conn.execute("PRAGMA foreign_key_check"))
    assert fk_rows == []
