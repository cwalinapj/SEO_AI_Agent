"""Tests for Moz snapshots and budgeting migration."""

from pathlib import Path
import sqlite3


MIG_0002 = Path(__file__).resolve().parents[1] / "migrations" / "0002_serp.sql"
MIG_0010 = Path(__file__).resolve().parents[1] / "migrations" / "0010_step1_keyword_research.sql"
MIG_0004 = Path(__file__).resolve().parents[1] / "migrations" / "0004_pagespeed_monitoring.sql"
MIG_0015 = Path(__file__).resolve().parents[1] / "migrations" / "0015_unified_d1_step2_step3.sql"
MIG_0017 = Path(__file__).resolve().parents[1] / "migrations" / "0017_moz_snapshots_and_budgeting.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(MIG_0002.read_text())
    conn.executescript(MIG_0010.read_text())
    conn.executescript(MIG_0004.read_text())
    conn.executescript(MIG_0015.read_text())
    conn.executescript(MIG_0017.read_text())
    return conn


def test_moz_tables_exist():
    conn = _connect()
    try:
        expected = {
            "moz_url_metrics_snapshots",
            "moz_anchor_text_snapshots",
            "moz_linking_root_domains_snapshots",
            "moz_link_intersect_snapshots",
            "moz_usage_snapshots",
            "moz_index_metadata_snapshots",
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


def test_moz_url_metrics_unique_per_url_day_geo():
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO urls (id, url, url_hash, domain, created_at)
            VALUES ('url_1', 'https://example.com/a', 'hash_a', 'example.com', 1700000000)
            """
        )
        conn.execute(
            """
            INSERT INTO moz_url_metrics_snapshots (
              snapshot_id, url_id, collected_day, geo_key, page_authority, domain_authority, metrics_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("snap_1", "url_1", "2026-03-01", "us", 20.0, 40.0, "{}", 1700000000),
        )

        try:
            conn.execute(
                """
                INSERT INTO moz_url_metrics_snapshots (
                  snapshot_id, url_id, collected_day, geo_key, page_authority, domain_authority, metrics_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("snap_2", "url_1", "2026-03-01", "us", 21.0, 41.0, "{}", 1700000001),
            )
            assert False, "Expected sqlite3.IntegrityError for duplicate url/day/geo"
        except sqlite3.IntegrityError:
            pass
    finally:
        conn.close()
