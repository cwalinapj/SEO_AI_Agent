"""Focused SQL tests for daily SERP graphing and domain averages."""

import sqlite3


def _seed_serp_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE serp_runs (
          serp_id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          phrase TEXT NOT NULL,
          region_json TEXT NOT NULL,
          device TEXT NOT NULL,
          engine TEXT NOT NULL,
          provider TEXT NOT NULL,
          actor TEXT NOT NULL,
          run_id TEXT,
          status TEXT NOT NULL,
          error TEXT,
          created_at INTEGER NOT NULL
        );

        CREATE TABLE serp_results (
          serp_id TEXT NOT NULL,
          rank INTEGER NOT NULL,
          url TEXT NOT NULL,
          domain TEXT NOT NULL,
          title TEXT,
          snippet TEXT,
          PRIMARY KEY (serp_id, rank)
        );
        """
    )


def test_graph_query_uses_latest_run_per_day():
    conn = sqlite3.connect(":memory:")
    try:
        _seed_serp_tables(conn)
        # Day 1 older run
        conn.execute(
            """
            INSERT INTO serp_runs
            (serp_id, user_id, phrase, region_json, device, engine, provider, actor, run_id, status, error, created_at)
            VALUES ('serp_old', 'u1', 'kw', '{}', 'desktop', 'google', 'apify', 'actor', NULL, 'ok', NULL, 1735689600000)
            """
        )
        conn.execute(
            """
            INSERT INTO serp_results (serp_id, rank, url, domain, title, snippet)
            VALUES ('serp_old', 1, 'https://a.com', 'a.com', 'A old', 'old')
            """
        )

        # Day 1 newer run (should win for graph)
        conn.execute(
            """
            INSERT INTO serp_runs
            (serp_id, user_id, phrase, region_json, device, engine, provider, actor, run_id, status, error, created_at)
            VALUES ('serp_new', 'u1', 'kw', '{}', 'desktop', 'google', 'apify', 'actor', NULL, 'ok', NULL, 1735693200000)
            """
        )
        conn.execute(
            """
            INSERT INTO serp_results (serp_id, rank, url, domain, title, snippet)
            VALUES ('serp_new', 1, 'https://b.com', 'b.com', 'B new', 'new')
            """
        )

        rows = conn.execute(
            """
            WITH runs AS (
              SELECT
                serp_id,
                date(created_at / 1000, 'unixepoch') AS day,
                created_at,
                ROW_NUMBER() OVER (
                  PARTITION BY date(created_at / 1000, 'unixepoch')
                  ORDER BY created_at DESC
                ) AS rn
              FROM serp_runs
              WHERE phrase = 'kw'
                AND status = 'ok'
            )
            SELECT runs.day, r.url
            FROM runs
            JOIN serp_results r ON r.serp_id = runs.serp_id
            WHERE runs.rn = 1
            """
        ).fetchall()

        assert rows == [("2025-01-01", "https://b.com")]
    finally:
        conn.close()


def test_domain_average_best_rank_per_keyword():
    conn = sqlite3.connect(":memory:")
    try:
        _seed_serp_tables(conn)
        conn.execute(
            """
            INSERT INTO serp_runs
            (serp_id, user_id, phrase, region_json, device, engine, provider, actor, run_id, status, error, created_at)
            VALUES ('s1', 'u1', 'kw1', '{}', 'desktop', 'google', 'apify', 'actor', NULL, 'ok', NULL, 1735689600000)
            """
        )
        conn.execute(
            """
            INSERT INTO serp_runs
            (serp_id, user_id, phrase, region_json, device, engine, provider, actor, run_id, status, error, created_at)
            VALUES ('s2', 'u1', 'kw2', '{}', 'desktop', 'google', 'apify', 'actor', NULL, 'ok', NULL, 1735776000000)
            """
        )
        conn.executemany(
            """
            INSERT INTO serp_results (serp_id, rank, url, domain, title, snippet) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("s1", 3, "https://www.callbighorn.com/a", "www.callbighorn.com", "", ""),
                ("s1", 6, "https://hoffmanplumbing.com/a", "hoffmanplumbing.com", "", ""),
                ("s2", 6, "https://www.callbighorn.com/b", "www.callbighorn.com", "", ""),
                ("s2", 9, "https://hoffmanplumbing.com/b", "hoffmanplumbing.com", "", ""),
            ],
        )

        # best rank per keyword and domain
        kw1 = dict(
            conn.execute(
                """
                SELECT domain, MIN(rank) FROM serp_results WHERE serp_id = 's1' GROUP BY domain
                """
            ).fetchall()
        )
        kw2 = dict(
            conn.execute(
                """
                SELECT domain, MIN(rank) FROM serp_results WHERE serp_id = 's2' GROUP BY domain
                """
            ).fetchall()
        )

        callbighorn_avg = (kw1["www.callbighorn.com"] + kw2["www.callbighorn.com"]) / 2
        hoffman_avg = (kw1["hoffmanplumbing.com"] + kw2["hoffmanplumbing.com"]) / 2

        assert callbighorn_avg == 4.5
        assert hoffman_avg == 7.5
        assert callbighorn_avg < hoffman_avg
    finally:
        conn.close()
