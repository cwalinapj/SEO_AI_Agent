from pathlib import Path
import sqlite3


def apply_sql(conn: sqlite3.Connection, path: Path) -> None:
    conn.executescript(path.read_text())


def test_serp_runs_adds_fallback_reason() -> None:
    conn = sqlite3.connect(":memory:")
    apply_sql(conn, Path("migrations/0002_serp.sql"))
    apply_sql(conn, Path("migrations/0020_serp_runs_fallback_reason.sql"))
    columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(serp_runs)")
    }
    assert "fallback_reason" in columns
