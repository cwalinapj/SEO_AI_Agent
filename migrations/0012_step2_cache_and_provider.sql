ALTER TABLE step2_url_backlinks
  ADD COLUMN authority_provider TEXT NOT NULL DEFAULT 'semrush';

ALTER TABLE step2_domain_backlinks
  ADD COLUMN authority_provider TEXT NOT NULL DEFAULT 'semrush';

CREATE TABLE IF NOT EXISTS step2_html_cache (
  cache_id TEXT PRIMARY KEY,
  url_hash TEXT NOT NULL UNIQUE,
  url TEXT NOT NULL,
  etag TEXT,
  last_modified TEXT,
  content_hash TEXT NOT NULL,
  html_snapshot TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_step2_html_cache_expiry
  ON step2_html_cache (expires_at);

CREATE TABLE IF NOT EXISTS step2_backlink_cache (
  cache_id TEXT PRIMARY KEY,
  cache_key TEXT NOT NULL UNIQUE,
  scope TEXT NOT NULL,
  provider TEXT NOT NULL,
  data_json TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_step2_backlink_cache_expiry
  ON step2_backlink_cache (expires_at);

CREATE TABLE IF NOT EXISTS step2_domain_graph_cache (
  cache_id TEXT PRIMARY KEY,
  domain TEXT NOT NULL UNIQUE,
  graph_json TEXT NOT NULL,
  summary_json TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_step2_domain_graph_cache_expiry
  ON step2_domain_graph_cache (expires_at);
