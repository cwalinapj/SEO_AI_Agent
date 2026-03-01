from pathlib import Path
import sqlite3


def apply_sql(conn: sqlite3.Connection, path: Path) -> None:
    conn.executescript(path.read_text())


def test_site_provider_profiles_table_and_defaults() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    apply_sql(conn, Path("migrations/0010_step1_keyword_research.sql"))
    apply_sql(conn, Path("migrations/0019_site_provider_profiles.sql"))

    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert "site_provider_profiles" in tables

    conn.execute(
        """
        INSERT INTO wp_ai_seo_sites (
          site_id, site_url, site_name, business_address, primary_location_hint, site_type_hint,
          local_mode, input_json, site_profile_json, last_analysis_at, created_at, updated_at
        ) VALUES ('site_1', 'https://example.com', 'Example', NULL, NULL, NULL, 0, '{}', '{}', 0, 0, 0)
        """
    )
    conn.execute("INSERT INTO site_provider_profiles (site_id) VALUES ('site_1')")
    row = conn.execute(
        "SELECT serp_provider, page_provider, geo_provider FROM site_provider_profiles WHERE site_id='site_1'"
    ).fetchone()
    assert row == ("headless_google", "direct_fetch", "proxy_lease_pool")


def test_site_provider_profiles_constraints() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    apply_sql(conn, Path("migrations/0010_step1_keyword_research.sql"))
    apply_sql(conn, Path("migrations/0019_site_provider_profiles.sql"))
    conn.execute(
        """
        INSERT INTO wp_ai_seo_sites (
          site_id, site_url, site_name, business_address, primary_location_hint, site_type_hint,
          local_mode, input_json, site_profile_json, last_analysis_at, created_at, updated_at
        ) VALUES ('site_1', 'https://example.com', 'Example', NULL, NULL, NULL, 0, '{}', '{}', 0, 0, 0)
        """
    )
    try:
        conn.execute(
            "INSERT INTO site_provider_profiles (site_id, serp_provider) VALUES ('site_1', 'bad')"
        )
        assert False, "Expected CHECK constraint failure for invalid serp_provider"
    except sqlite3.IntegrityError:
        pass
