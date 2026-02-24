CREATE TABLE IF NOT EXISTS keywords (
  kw_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  phrase TEXT NOT NULL,
  region_json TEXT NOT NULL,         -- {"country":"US","region":"CA","city":"San Jose"}
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS kw_metrics (
  kw_id TEXT PRIMARY KEY,
  avg_cpc_micros INTEGER,            -- from Ads/semrush/ahrefs if you have it
  monthly_volume INTEGER,            -- regional
  difficulty INTEGER,                -- optional
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS serp_runs (
  serp_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  phrase TEXT NOT NULL,
  region_json TEXT NOT NULL,
  device TEXT NOT NULL,
  engine TEXT NOT NULL,
  provider TEXT NOT NULL,            -- "apify"
  actor TEXT NOT NULL,               -- "apify/google-search-scraper"
  run_id TEXT,
  status TEXT NOT NULL,              -- pending|ok|error
  error TEXT,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS serp_results (
  serp_id TEXT NOT NULL,
  rank INTEGER NOT NULL,
  url TEXT NOT NULL,
  domain TEXT NOT NULL,
  title TEXT,
  snippet TEXT,
  PRIMARY KEY (serp_id, rank)
);

CREATE TABLE IF NOT EXISTS inspiration_links (
  insp_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  source TEXT NOT NULL,              -- "serp" | "wayback" | "manual"
  url TEXT NOT NULL,
  meta_json TEXT NOT NULL,           -- {"rank":3,"kw":"...","palette":[...],...}
  created_at INTEGER NOT NULL
);
