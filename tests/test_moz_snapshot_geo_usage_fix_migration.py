from pathlib import Path
import sqlite3


def apply_sql(conn: sqlite3.Connection, path: Path) -> None:
    conn.executescript(path.read_text())


def test_moz_snapshot_geo_usage_fix_migration() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    apply_sql(conn, Path("migrations/0010_step1_keyword_research.sql"))
    apply_sql(conn, Path("migrations/0015_unified_d1_step2_step3.sql"))
    apply_sql(conn, Path("migrations/0017_moz_snapshots_and_budgeting.sql"))
    apply_sql(conn, Path("migrations/0018_moz_profiles_and_usage.sql"))
    apply_sql(conn, Path("migrations/0021_moz_snapshot_geo_usage_fix.sql"))

    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert "moz_job_usage" in tables

    anchor_cols = {row[1] for row in conn.execute("PRAGMA table_info(moz_anchor_text_snapshots)")}
    root_cols = {row[1] for row in conn.execute("PRAGMA table_info(moz_linking_root_domains_snapshots)")}
    inter_cols = {row[1] for row in conn.execute("PRAGMA table_info(moz_link_intersect_snapshots)")}
    assert {"geo_key", "rows_used", "site_run_id"}.issubset(anchor_cols)
    assert {"geo_key", "rows_used", "site_run_id"}.issubset(root_cols)
    assert {"rows_used", "site_run_id"}.issubset(inter_cols)

    # UNIQUE now includes geo_key.
    conn.execute(
        "INSERT INTO urls (id, url, url_hash, domain, created_at) VALUES ('u1','https://a.test/x','h1','a.test',0)"
    )
    conn.execute(
        """
        INSERT INTO moz_anchor_text_snapshots
        (snapshot_id, target_url_id, collected_day, geo_key, top_anchors_json, total_anchor_rows, totals_json, rows_used, created_at)
        VALUES ('s1','u1','2026-03-01','us','[]',0,'{}',0,0)
        """
    )
    conn.execute(
        """
        INSERT INTO moz_anchor_text_snapshots
        (snapshot_id, target_url_id, collected_day, geo_key, top_anchors_json, total_anchor_rows, totals_json, rows_used, created_at)
        VALUES ('s2','u1','2026-03-01','metro:los-angeles-ca','[]',0,'{}',0,0)
        """
    )
