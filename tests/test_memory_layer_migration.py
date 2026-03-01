"""Tests for semantic memory layer migration (0024)."""

from pathlib import Path
import sqlite3


def apply_sql(conn: sqlite3.Connection, path: Path) -> None:
    conn.executescript(path.read_text())


def test_memory_layer_tables_indexes_and_constraints() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")

    apply_sql(conn, Path("migrations/0010_step1_keyword_research.sql"))
    apply_sql(conn, Path("migrations/0024_memory_layer_semantic_store.sql"))

    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "memory_items" in tables
    assert "memory_events" in tables

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
        INSERT INTO memory_items (
          memory_id, site_id, type, scope_key, collected_day, geo_key, title, text_summary,
          tags_json, source_r2_key, source_sha256, vector_namespace, vector_id
        ) VALUES (
          'm1', 'site_1', 'serp_keyword_summary', 'kw_1', '2026-03-01', 'us', 'k1', 'summary',
          '{}', 'memory/v1/site/site_1/day/2026-03-01/geo/us/type/serp_keyword_summary/scope/kw_1/m1.json',
          'abc', 'mem_v1', 'mem_v1:site_1:serp_keyword_summary:us:2026-03-01:kw_1'
        )
        """
    )

    # unique scope/day/geo/type
    try:
      conn.execute(
          """
          INSERT INTO memory_items (
            memory_id, site_id, type, scope_key, collected_day, geo_key, text_summary,
            tags_json, source_r2_key, vector_namespace, vector_id
          ) VALUES (
            'm2', 'site_1', 'serp_keyword_summary', 'kw_1', '2026-03-01', 'us', 'summary2',
            '{}', 'memory/v1/site/site_1/day/2026-03-01/geo/us/type/serp_keyword_summary/scope/kw_1/m2.json',
            'mem_v1', 'mem_v1:site_1:serp_keyword_summary:us:2026-03-01:kw_1'
          )
          """
      )
      assert False, "Expected unique scope/day/geo/type violation"
    except sqlite3.IntegrityError:
      pass

    # FK enforce site
    try:
      conn.execute(
          """
          INSERT INTO memory_items (
            memory_id, site_id, type, collected_day, geo_key, text_summary,
            tags_json, source_r2_key, vector_namespace, vector_id
          ) VALUES (
            'm3', 'missing', 'cluster_summary', '2026-03-01', 'us', 'x',
            '{}', 'memory/v1/site/missing/day/2026-03-01/geo/us/type/cluster_summary/scope/na/m3.json',
            'mem_v1', 'mem_v1:missing:cluster_summary:us:2026-03-01:na'
          )
          """
      )
      assert False, "Expected FK violation for missing site"
    except sqlite3.IntegrityError:
      pass

    fk_rows = list(conn.execute("PRAGMA foreign_key_check"))
    assert fk_rows == []
