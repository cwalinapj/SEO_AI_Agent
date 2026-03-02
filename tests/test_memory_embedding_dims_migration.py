"""Tests for 0026 memory embedding dims migration."""

from pathlib import Path
import sqlite3


def apply_sql(conn: sqlite3.Connection, path: Path) -> None:
    conn.executescript(path.read_text())


def test_memory_embedding_dims_added_and_backfilled() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")

    apply_sql(conn, Path("migrations/0010_step1_keyword_research.sql"))
    apply_sql(conn, Path("migrations/0024_memory_layer_semantic_store.sql"))

    conn.execute(
        """
        INSERT INTO wp_ai_seo_sites (
          site_id, site_url, site_name, business_address, primary_location_hint, site_type_hint,
          local_mode, input_json, site_profile_json, last_analysis_at, last_research_at, created_at, updated_at
        ) VALUES ('site_1','https://example.com','Example',NULL,NULL,NULL,0,'{}','{}',0,NULL,0,0)
        """
    )
    conn.execute(
        """
        INSERT INTO memory_items (
          memory_id, site_id, type, scope_key, collected_day, geo_key,
          title, text_summary, tags_json, source_r2_key, source_sha256,
          vector_namespace, vector_id, embedding_model, token_count
        ) VALUES (
          'm1','site_1','cluster_summary','scope','2026-03-01','us',
          't','summary','{}','memory/v1/site/site_1/day/2026-03-01/geo/us/type/cluster_summary/scope/scope/m1.json','x',
          'mem_v1','m_abc','text-embedding-3-large',12
        )
        """
    )

    apply_sql(conn, Path("migrations/0026_memory_embedding_dims.sql"))

    cols = {row[1] for row in conn.execute("PRAGMA table_info(memory_items)")}
    assert "embedding_dims" in cols

    dims = conn.execute("SELECT embedding_dims FROM memory_items WHERE memory_id='m1'").fetchone()[0]
    assert dims == 3072
