"""Tests for human-verified task marketplace migration."""

from pathlib import Path
import sqlite3


MIG_0002 = Path(__file__).resolve().parents[1] / "migrations" / "0002_serp.sql"
MIG_0004 = Path(__file__).resolve().parents[1] / "migrations" / "0004_pagespeed_monitoring.sql"
MIG_0015 = Path(__file__).resolve().parents[1] / "migrations" / "0015_unified_d1_step2_step3.sql"
MIG_0016 = Path(__file__).resolve().parents[1] / "migrations" / "0016_human_verified_task_marketplace.sql"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(MIG_0002.read_text())
    conn.executescript(MIG_0004.read_text())
    conn.executescript(MIG_0015.read_text())
    conn.executescript(MIG_0016.read_text())
    return conn


def test_marketplace_tables_exist():
    conn = _connect()
    try:
        expected = {
            "task_market_tasks",
            "task_market_claims",
            "task_market_evidence",
            "task_market_verifications",
            "task_market_payout_authorizations",
            "task_idempotency_keys",
            "task_audit_log",
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


def test_only_one_active_claim_per_task():
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO task_market_tasks (
              task_id, status, platform, issuer_wallet,
              payout_amount, payout_token, payout_chain,
              deadline_at, task_spec_json, task_spec_hash, created_at, updated_at
            ) VALUES (?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "task_1",
                "x",
                "0xissuer",
                "10",
                "USDC",
                "base",
                1999999999000,
                '{"title":"task"}',
                "hash_1",
                1700000000000,
                1700000000000,
            ),
        )
        conn.execute(
            """
            INSERT INTO task_market_claims (
              claim_id, task_id, worker_wallet, status, claimed_at, expires_at
            ) VALUES (?, ?, ?, 'active', ?, ?)
            """,
            ("claim_1", "task_1", "0xworkerA", 1700000000000, 1700003600000),
        )

        try:
            conn.execute(
                """
                INSERT INTO task_market_claims (
                  claim_id, task_id, worker_wallet, status, claimed_at, expires_at
                ) VALUES (?, ?, ?, 'active', ?, ?)
                """,
                ("claim_2", "task_1", "0xworkerB", 1700000001000, 1700003601000),
            )
            assert False, "Expected sqlite3.IntegrityError for second active claim"
        except sqlite3.IntegrityError:
            pass

        conn.execute(
            """
            UPDATE task_market_claims
            SET status = 'released', released_at = ?, release_reason = 'manual'
            WHERE claim_id = ?
            """,
            (1700000100000, "claim_1"),
        )
        conn.execute(
            """
            INSERT INTO task_market_claims (
              claim_id, task_id, worker_wallet, status, claimed_at, expires_at
            ) VALUES (?, ?, ?, 'active', ?, ?)
            """,
            ("claim_3", "task_1", "0xworkerB", 1700000200000, 1700003800000),
        )
    finally:
        conn.close()
