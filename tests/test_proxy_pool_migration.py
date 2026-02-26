"""Focused tests for residential proxy pool migration."""

from pathlib import Path
import sqlite3


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "migrations" / "0006_proxy_pool.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(MIGRATION_PATH.read_text())
    return conn


def test_proxy_pool_tables_exist():
    conn = _connect()
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('residential_proxies', 'proxy_leases')"
            )
        }
        assert tables == {"residential_proxies", "proxy_leases"}
    finally:
        conn.close()


def test_proxy_inventory_view_counts_active_leases_and_capacity():
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO residential_proxies (
              proxy_id, label, proxy_url, country, region, metro_area,
              max_concurrent_leases, hourly_rate_usd, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "proxy_1",
                "San Jose pool 1",
                "http://user:pass@proxy.example:9000",
                "us",
                "ca",
                "san jose",
                3,
                1.5,
                "active",
                1735689600000,
                1735689600000,
            ),
        )

        conn.execute(
            """
            INSERT INTO proxy_leases (
              lease_id, proxy_id, user_id, keyword, metro_area, status,
              leased_at, expires_at, released_at, hourly_rate_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "lease_active",
                "proxy_1",
                "user_1",
                "plumber san jose",
                "san jose",
                "active",
                1735689600000,
                9999999999999,
                None,
                1.5,
            ),
        )
        conn.execute(
            """
            INSERT INTO proxy_leases (
              lease_id, proxy_id, user_id, keyword, metro_area, status,
              leased_at, expires_at, released_at, hourly_rate_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "lease_expired",
                "proxy_1",
                "user_2",
                "plumber san jose",
                "san jose",
                "expired",
                1735689600000,
                1735689601000,
                None,
                1.5,
            ),
        )

        row = conn.execute(
            "SELECT active_leases, available_slots FROM proxy_inventory_status WHERE proxy_id = 'proxy_1'"
        ).fetchone()
        assert row == (1, 2)
    finally:
        conn.close()
