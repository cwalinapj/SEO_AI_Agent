CREATE TABLE IF NOT EXISTS serp_watchlist (
  watch_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  phrase TEXT NOT NULL,
  region_json TEXT NOT NULL,
  device TEXT NOT NULL DEFAULT 'desktop',
  max_results INTEGER NOT NULL DEFAULT 20,
  active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  last_run_at INTEGER,
  last_serp_id TEXT,
  last_status TEXT,
  last_error TEXT,
  UNIQUE (user_id, phrase, region_json, device)
);

CREATE INDEX IF NOT EXISTS idx_serp_watchlist_user_active
  ON serp_watchlist (user_id, active, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_serp_watchlist_last_run
  ON serp_watchlist (last_run_at);
