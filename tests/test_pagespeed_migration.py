"""Focused tests for PageSpeed monitoring migration."""

from pathlib import Path
import sqlite3

import pytest


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "migrations" / "0004_pagespeed_monitoring.sql"
)


@pytest.fixture()
def conn():
    connection = sqlite3.connect(":memory:")
    connection.executescript(MIGRATION_PATH.read_text())
    yield connection
    connection.close()


def test_speed_schema_tables_exist(conn):
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('sites', 'speed_snapshots')"
        )
    }
    assert tables == {"sites", "speed_snapshots"}


def test_speed_snapshot_cooldown_per_site_strategy(conn):
    conn.execute(
        """
        INSERT INTO sites (site_id, user_id, production_url, default_strategy, last_deploy_hash)
        VALUES ('s1', 'u1', 'https://example.pages.dev/', 'mobile', 'abc123')
        """
    )
    conn.execute(
        """
        INSERT INTO speed_snapshots (
          snapshot_id, site_id, created_at, strategy, lcp_ms, cls, tbt_ms, fcp_ms, field_lcp_pctl, field_cls_pctl, field_inp_pctl,
          performance_score, psi_fetch_time, trigger_reason, deploy_hash
        ) VALUES (
          'snap_1', 's1', 1735689600000, 'mobile', 2200, 0.08, 120, 1100, 2500, 0.08, 200, 0.82, '2026-01-01T00:00:00Z', 'deploy', 'abc123'
        )
        """
    )

    with pytest.raises(sqlite3.IntegrityError, match="speed snapshot cooldown active"):
        conn.execute(
            """
            INSERT INTO speed_snapshots (
              snapshot_id, site_id, created_at, strategy, lcp_ms, cls, tbt_ms, fcp_ms, field_lcp_pctl, field_cls_pctl, field_inp_pctl,
              performance_score, psi_fetch_time, trigger_reason, deploy_hash
            ) VALUES (
              'snap_2', 's1', 1735693200000, 'mobile', 2300, 0.09, 140, 1200, 2550, 0.09, 220, 0.80, '2026-01-01T01:00:00Z', 'manual', 'abc123'
            )
            """
        )


def test_speed_delta_view_sets_warn_and_note(conn):
    conn.execute(
        """
        INSERT INTO sites (site_id, user_id, production_url, default_strategy)
        VALUES ('s2', 'u2', 'https://example.com/', 'desktop')
        """
    )
    conn.execute(
        """
        INSERT INTO speed_snapshots (
          snapshot_id, site_id, created_at, strategy, lcp_ms, cls, tbt_ms, fcp_ms, field_lcp_pctl, field_cls_pctl, field_inp_pctl,
          performance_score, psi_fetch_time, trigger_reason, deploy_hash
        ) VALUES (
          'snap_3', 's2', 1735689600000, 'desktop', 1800, 0.05, 90, 1000, 2000, 0.05, 150, 0.91, '2026-01-01T00:00:00Z', 'deploy', 'abc111'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO speed_snapshots (
          snapshot_id, site_id, created_at, strategy, lcp_ms, cls, tbt_ms, fcp_ms, field_lcp_pctl, field_cls_pctl, field_inp_pctl,
          performance_score, psi_fetch_time, trigger_reason, deploy_hash
        ) VALUES (
          'snap_4', 's2', 1735736400000, 'desktop', 2200, 0.09, 260, 1100, 2250, 0.09, 280, 0.80, '2026-01-01T13:00:00Z', 'deploy', 'abc123'
        )
        """
    )

    row = conn.execute(
        """
        SELECT delta_lcp_ms, delta_cls, delta_tbt_ms, delta_field_lcp_pctl, delta_field_inp_pctl, severity, should_create_note, suggested_note
        FROM speed_snapshot_deltas
        WHERE site_id = 's2'
        ORDER BY date DESC
        LIMIT 1
        """
    ).fetchone()

    assert row[0] == 400
    assert row[1] == pytest.approx(0.04)
    assert row[2:] == (170, 250, 130, "warn", 1, "LCP regressed +400ms after deploy abc123 (desktop).")
