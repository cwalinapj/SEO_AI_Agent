PRAGMA foreign_keys = ON;

-- Moz URL-level authority snapshots (daily, geo-scoped)
CREATE TABLE IF NOT EXISTS moz_url_metrics_snapshots (
  snapshot_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  url_id TEXT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
  collected_day TEXT NOT NULL, -- YYYY-MM-DD
  geo_key TEXT NOT NULL DEFAULT 'us',
  page_authority REAL,
  domain_authority REAL,
  spam_score REAL,
  linking_domains INTEGER,
  external_links INTEGER,
  metrics_json TEXT,
  job_id TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(url_id, collected_day, geo_key)
);

CREATE INDEX IF NOT EXISTS idx_moz_url_metrics_day_geo
  ON moz_url_metrics_snapshots(collected_day, geo_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_moz_url_metrics_url_day
  ON moz_url_metrics_snapshots(url_id, collected_day DESC);

-- Moz anchor text profile snapshots (daily per target URL)
CREATE TABLE IF NOT EXISTS moz_anchor_text_snapshots (
  snapshot_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  target_url_id TEXT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
  collected_day TEXT NOT NULL,
  top_anchors_json TEXT NOT NULL,
  total_anchor_rows INTEGER,
  totals_json TEXT,
  job_id TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(target_url_id, collected_day)
);

CREATE INDEX IF NOT EXISTS idx_moz_anchor_target_day
  ON moz_anchor_text_snapshots(target_url_id, collected_day DESC);

-- Moz linking root domains snapshots (daily per target URL)
CREATE TABLE IF NOT EXISTS moz_linking_root_domains_snapshots (
  snapshot_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  target_url_id TEXT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
  collected_day TEXT NOT NULL,
  top_domains_json TEXT NOT NULL,
  total_domain_rows INTEGER,
  totals_json TEXT,
  job_id TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(target_url_id, collected_day)
);

CREATE INDEX IF NOT EXISTS idx_moz_root_domains_target_day
  ON moz_linking_root_domains_snapshots(target_url_id, collected_day DESC);

-- Optional cluster-level intersect snapshots for team outreach tasks
CREATE TABLE IF NOT EXISTS moz_link_intersect_snapshots (
  snapshot_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_id TEXT REFERENCES sites(site_id) ON DELETE SET NULL,
  cluster TEXT,
  collected_day TEXT NOT NULL,
  intersect_json TEXT NOT NULL,
  totals_json TEXT,
  job_id TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_moz_intersect_site_day
  ON moz_link_intersect_snapshots(site_id, collected_day DESC);
CREATE INDEX IF NOT EXISTS idx_moz_intersect_cluster_day
  ON moz_link_intersect_snapshots(cluster, collected_day DESC);

-- Optional usage/index metadata snapshots for scheduler & row caps
CREATE TABLE IF NOT EXISTS moz_usage_snapshots (
  snapshot_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  collected_day TEXT NOT NULL,
  usage_json TEXT NOT NULL,
  rows_used INTEGER,
  rows_limit INTEGER,
  job_id TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(collected_day)
);

CREATE TABLE IF NOT EXISTS moz_index_metadata_snapshots (
  snapshot_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  collected_day TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  index_updated_at TEXT,
  job_id TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(collected_day)
);
