"""Focused tests for 0022 keywords/serp/inspiration hardening migration."""

from pathlib import Path
import sqlite3


def apply_sql(conn: sqlite3.Connection, path: Path) -> None:
    conn.executescript(path.read_text())


def test_0022_hardens_legacy_tables_and_preserves_runtime_compat() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")

    apply_sql(conn, Path("migrations/0002_serp.sql"))
    apply_sql(conn, Path("migrations/0003_worker_endpoints.sql"))
    apply_sql(conn, Path("migrations/0009_serp_canonicalization.sql"))
    apply_sql(conn, Path("migrations/0015_unified_d1_step2_step3.sql"))

    # Seed minimal pre-0022 rows.
    conn.execute(
        """
        INSERT INTO keywords (
          kw_id, user_id, phrase, region_json, created_at,
          keyword_set_id, keyword, keyword_norm, priority, cluster, intent, is_local_intent, target_page_type, target_slug
        ) VALUES (
          'kw1', 'u1', 'Water Heater Repair', '{"country":"US","language":"en"}', 1000,
          NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL
        )
        """
    )
    conn.execute(
        "INSERT INTO kw_metrics (kw_id, avg_cpc_micros, monthly_volume, difficulty, updated_at) VALUES ('kw1', 1200000, 400, 35, 1000)"
    )
    conn.execute(
        """
        INSERT INTO serp_runs (
          serp_id, user_id, phrase, region_json, device, engine, provider, actor, run_id, status, error,
          keyword_norm, region_key, device_key, serp_key, parser_version, raw_payload_sha256, extractor_mode,
          fallback_reason, created_at
        ) VALUES (
          's1', 'u1', 'Water Heater Repair', '{"country":"US","language":"en"}', 'mobile', 'google',
          'decodo', 'legacy-actor', NULL, 'ok', NULL,
          'water heater repair', 'us-en', 'mobile', 'key-1', 'v1', NULL, 'parsed',
          NULL, 1000
        )
        """
    )
    conn.execute(
        "INSERT INTO serp_results (serp_id, rank, url, domain, title, snippet) VALUES ('s1', 1, 'https://a.example/path', 'a.example', 'A', 'B')"
    )
    conn.execute(
        "INSERT INTO inspiration_links (insp_id, user_id, source, url, meta_json, created_at) VALUES ('i1', 'u1', 'serp', 'https://a.example/path', '{}', 1000)"
    )

    apply_sql(conn, Path("migrations/0022_keywords_serp_inspiration_hardening.sql"))

    # keywords table remains present and queryable.
    row = conn.execute("SELECT phrase FROM keywords WHERE kw_id='kw1'").fetchone()
    assert row == ("Water Heater Repair",)

    # kw_metrics FK enforced.
    with conn:
        try:
            conn.execute(
                "INSERT INTO kw_metrics (kw_id, updated_at) VALUES ('missing_kw', 1000)"
            )
            assert False, "Expected sqlite3.IntegrityError for kw_metrics FK"
        except sqlite3.IntegrityError:
            pass

    # serp_runs actor nullable + helper columns retained.
    cols = {r[1] for r in conn.execute("PRAGMA table_info('serp_runs')")}
    assert {"fallback_reason", "serp_key", "keyword_norm", "region_key", "device_key", "geo_key"}.issubset(cols)

    # actor is nullable after hardening.
    conn.execute(
        """
        INSERT INTO serp_runs (
          serp_id, user_id, phrase, region_json, device, engine, provider, actor, run_id, status, error,
          keyword_norm, region_key, device_key, serp_key, parser_version, raw_payload_sha256, extractor_mode,
          fallback_reason, created_at
        ) VALUES (
          's2', 'u1', 'Water Heater Repair', '{"country":"US","language":"en"}', 'mobile', 'google',
          'decodo', NULL, NULL, 'ok', NULL,
          'water heater repair', 'us-en', 'mobile', 'key-2', 'v1', NULL, 'parsed',
          NULL, 1001
        )
        """
    )

    # serp_results has FK + normalized helper columns.
    srow = conn.execute(
        "SELECT url_hash, root_domain FROM serp_results WHERE serp_id='s1' AND rank=1"
    ).fetchone()
    assert srow == ("https://a.example/path", "a.example")

    with conn:
        try:
            conn.execute(
                "INSERT INTO serp_results (serp_id, rank, url, domain) VALUES ('missing_serp', 1, 'https://x.example', 'x.example')"
            )
            assert False, "Expected sqlite3.IntegrityError for serp_results FK"
        except sqlite3.IntegrityError:
            pass

    # inspiration_links hash dedupe unique key.
    with conn:
        try:
            conn.execute(
                "INSERT INTO inspiration_links (insp_id, user_id, source, url, url_hash, meta_json, created_at) VALUES ('i2', 'u1', 'serp', 'https://a.example/path', 'https://a.example/path', '{}', 1001)"
            )
            assert False, "Expected sqlite3.IntegrityError for user_id+url_hash uniqueness"
        except sqlite3.IntegrityError:
            pass

    fk_rows = list(conn.execute("PRAGMA foreign_key_check"))
    assert fk_rows == []
