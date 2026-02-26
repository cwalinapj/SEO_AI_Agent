"""Focused tests for SERP canonicalization migration."""

from pathlib import Path
import sqlite3


BASE_SCHEMA = Path(__file__).resolve().parents[1] / "migrations" / "0002_serp.sql"
MIGRATION_PATH = Path(__file__).resolve().parents[1] / "migrations" / "0009_serp_canonicalization.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(BASE_SCHEMA.read_text())
    conn.executescript(MIGRATION_PATH.read_text())
    return conn


def test_serp_runs_has_canonicalization_columns():
    conn = _connect()
    try:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info('serp_runs')").fetchall()
        }
        assert {
            "keyword_norm",
            "region_key",
            "device_key",
            "serp_key",
            "parser_version",
            "raw_payload_sha256",
            "extractor_mode",
        }.issubset(columns)
    finally:
        conn.close()


def test_serp_runs_accepts_hash_metadata_for_dedupe_and_drift():
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO serp_runs (
              serp_id, user_id, phrase, region_json, device, engine, provider, actor,
              run_id, status, error,
              keyword_norm, region_key, device_key, serp_key,
              parser_version, raw_payload_sha256, extractor_mode,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "serp_1",
                "user_1",
                "Plumber Near Me",
                '{"country":"US","language":"en"}',
                "desktop",
                "google",
                "apify-headless-chrome",
                "apify/google-search-scraper",
                None,
                "ok",
                None,
                "plumber near me",
                "US-en",
                "desktop",
                "abc123",
                "apify-google-v1",
                "deadbeef",
                "apify",
                1735689600000,
            ),
        )

        row = conn.execute(
            "SELECT keyword_norm, region_key, device_key, serp_key, parser_version, raw_payload_sha256, extractor_mode FROM serp_runs WHERE serp_id = 'serp_1'"
        ).fetchone()
        assert row == (
            "plumber near me",
            "US-en",
            "desktop",
            "abc123",
            "apify-google-v1",
            "deadbeef",
            "apify",
        )
    finally:
        conn.close()
