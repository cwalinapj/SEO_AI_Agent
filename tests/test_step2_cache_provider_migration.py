"""Focused tests for Step 2 cache/provider migration."""

from pathlib import Path
import sqlite3


MIGRATION_0011 = Path(__file__).resolve().parents[1] / "migrations" / "0011_step2_daily_harvest.sql"
MIGRATION_0012 = Path(__file__).resolve().parents[1] / "migrations" / "0012_step2_cache_and_provider.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(MIGRATION_0011.read_text())
    conn.executescript(MIGRATION_0012.read_text())
    return conn


def test_authority_provider_columns_added():
    conn = _connect()
    try:
        url_cols = {row[1] for row in conn.execute("PRAGMA table_info('step2_url_backlinks')")}
        domain_cols = {row[1] for row in conn.execute("PRAGMA table_info('step2_domain_backlinks')")}
        assert "authority_provider" in url_cols
        assert "authority_provider" in domain_cols
    finally:
        conn.close()


def test_cache_tables_exist():
    conn = _connect()
    try:
        tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table'
                  AND name IN ('step2_html_cache', 'step2_backlink_cache', 'step2_domain_graph_cache')
                """
            )
        }
        assert tables == {"step2_html_cache", "step2_backlink_cache", "step2_domain_graph_cache"}
    finally:
        conn.close()
