PRAGMA foreign_keys = OFF;

-- Ensure canonical URL registry exists for FK integrity in environments that
-- previously skipped unified schema migrations.
CREATE TABLE IF NOT EXISTS urls (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  url TEXT NOT NULL,
  url_hash TEXT NOT NULL,
  domain TEXT NOT NULL COLLATE NOCASE,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(url_hash)
);

CREATE INDEX IF NOT EXISTS idx_urls_domain
  ON urls(domain);

-- Ensure canonical site key is enforceable for FK targets.
CREATE UNIQUE INDEX IF NOT EXISTS idx_wp_ai_seo_sites_site_id_uq
  ON wp_ai_seo_sites(site_id);

-- Canonical usage ledger for budget explainability/enforcement.
CREATE TABLE IF NOT EXISTS moz_job_usage (
  job_id TEXT PRIMARY KEY,
  site_id TEXT REFERENCES wp_ai_seo_sites(site_id) ON DELETE SET NULL,
  collected_day TEXT NOT NULL, -- YYYY-MM-DD
  endpoint TEXT NOT NULL,      -- url_metrics|anchor_text|root_domains|intersect|usage|index_metadata|profile_run
  rows_used INTEGER NOT NULL DEFAULT 0,
  meta_json TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_moz_job_usage_site_day_v2
  ON moz_job_usage(site_id, collected_day DESC);
CREATE INDEX IF NOT EXISTS idx_moz_job_usage_site_endpoint_day
  ON moz_job_usage(site_id, endpoint, collected_day DESC);

-- Legacy table may not exist in some environments; create a minimal stub to
-- make backfill safe.
CREATE TABLE IF NOT EXISTS moz_job_row_usage (
  usage_id TEXT PRIMARY KEY,
  site_id TEXT,
  collected_day TEXT NOT NULL,
  job_id TEXT,
  job_type TEXT NOT NULL,
  rows_used INTEGER NOT NULL DEFAULT 0,
  degraded_mode INTEGER NOT NULL DEFAULT 0,
  fallback_reason TEXT,
  profile TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

-- Backfill from legacy table when present.
INSERT OR IGNORE INTO moz_job_usage (
  job_id, site_id, collected_day, endpoint, rows_used, meta_json, created_at
)
SELECT
  COALESCE(job_id, usage_id),
  site_id,
  collected_day,
  CASE
    WHEN job_type = 'moz_linking_root_domains' THEN 'root_domains'
    WHEN job_type = 'moz_url_metrics' THEN 'url_metrics'
    WHEN job_type = 'moz_anchor_text' THEN 'anchor_text'
    WHEN job_type = 'moz_link_intersect' THEN 'intersect'
    WHEN job_type = 'moz_usage_data' THEN 'usage'
    WHEN job_type = 'moz_index_metadata' THEN 'index_metadata'
    WHEN job_type = 'moz_profile_run' THEN 'profile_run'
    ELSE job_type
  END,
  rows_used,
  json_object(
    'degraded_mode', degraded_mode,
    'fallback_reason', fallback_reason,
    'profile', profile
  ),
  created_at
FROM moz_job_row_usage;

-- Rebuild anchor snapshots with geo scoping + traceability.
CREATE TABLE moz_anchor_text_snapshots_new (
  snapshot_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  target_url_id TEXT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
  collected_day TEXT NOT NULL,
  geo_key TEXT NOT NULL DEFAULT 'us',
  top_anchors_json TEXT NOT NULL,
  total_anchor_rows INTEGER,
  totals_json TEXT,
  rows_used INTEGER NOT NULL DEFAULT 0,
  site_run_id TEXT,
  job_id TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(target_url_id, collected_day, geo_key)
);

INSERT OR REPLACE INTO moz_anchor_text_snapshots_new (
  snapshot_id, target_url_id, collected_day, geo_key, top_anchors_json,
  total_anchor_rows, totals_json, rows_used, site_run_id, job_id, created_at
)
SELECT
  snapshot_id,
  target_url_id,
  collected_day,
  'us',
  top_anchors_json,
  total_anchor_rows,
  totals_json,
  COALESCE(total_anchor_rows, 0),
  NULL,
  job_id,
  created_at
FROM moz_anchor_text_snapshots;

DROP TABLE moz_anchor_text_snapshots;
ALTER TABLE moz_anchor_text_snapshots_new RENAME TO moz_anchor_text_snapshots;

CREATE INDEX IF NOT EXISTS idx_moz_anchor_target_day
  ON moz_anchor_text_snapshots(target_url_id, collected_day DESC);
CREATE INDEX IF NOT EXISTS idx_moz_anchor_day
  ON moz_anchor_text_snapshots(collected_day, geo_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_moz_anchor_job
  ON moz_anchor_text_snapshots(job_id);

-- Rebuild root-domain snapshots with geo scoping + traceability.
CREATE TABLE moz_linking_root_domains_snapshots_new (
  snapshot_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  target_url_id TEXT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
  collected_day TEXT NOT NULL,
  geo_key TEXT NOT NULL DEFAULT 'us',
  top_domains_json TEXT NOT NULL,
  total_domain_rows INTEGER,
  totals_json TEXT,
  rows_used INTEGER NOT NULL DEFAULT 0,
  site_run_id TEXT,
  job_id TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(target_url_id, collected_day, geo_key)
);

INSERT OR REPLACE INTO moz_linking_root_domains_snapshots_new (
  snapshot_id, target_url_id, collected_day, geo_key, top_domains_json,
  total_domain_rows, totals_json, rows_used, site_run_id, job_id, created_at
)
SELECT
  snapshot_id,
  target_url_id,
  collected_day,
  'us',
  top_domains_json,
  total_domain_rows,
  totals_json,
  COALESCE(total_domain_rows, 0),
  NULL,
  job_id,
  created_at
FROM moz_linking_root_domains_snapshots;

DROP TABLE moz_linking_root_domains_snapshots;
ALTER TABLE moz_linking_root_domains_snapshots_new RENAME TO moz_linking_root_domains_snapshots;

CREATE INDEX IF NOT EXISTS idx_moz_root_domains_target_day
  ON moz_linking_root_domains_snapshots(target_url_id, collected_day DESC);
CREATE INDEX IF NOT EXISTS idx_moz_root_domains_day
  ON moz_linking_root_domains_snapshots(collected_day, geo_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_moz_root_domains_job
  ON moz_linking_root_domains_snapshots(job_id);

-- Rebuild intersect snapshots with correct site FK + usage/traceability.
CREATE TABLE moz_link_intersect_snapshots_new (
  snapshot_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_id TEXT REFERENCES wp_ai_seo_sites(site_id) ON DELETE SET NULL,
  cluster TEXT NOT NULL DEFAULT '',
  collected_day TEXT NOT NULL,
  geo_key TEXT NOT NULL DEFAULT 'us',
  intersect_json TEXT NOT NULL,
  totals_json TEXT,
  rows_used INTEGER NOT NULL DEFAULT 0,
  site_run_id TEXT,
  job_id TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(site_id, cluster, collected_day, geo_key)
);

INSERT OR REPLACE INTO moz_link_intersect_snapshots_new (
  snapshot_id, site_id, cluster, collected_day, geo_key, intersect_json, totals_json,
  rows_used, site_run_id, job_id, created_at
)
SELECT
  snapshot_id,
  site_id,
  COALESCE(cluster, ''),
  collected_day,
  'us',
  intersect_json,
  totals_json,
  COALESCE(
    CAST(json_extract(totals_json, '$.rows_used') AS INTEGER),
    CAST(json_extract(totals_json, '$.total_rows') AS INTEGER),
    0
  ),
  NULL,
  job_id,
  created_at
FROM moz_link_intersect_snapshots;

DROP TABLE moz_link_intersect_snapshots;
ALTER TABLE moz_link_intersect_snapshots_new RENAME TO moz_link_intersect_snapshots;

CREATE INDEX IF NOT EXISTS idx_moz_intersect_site_day
  ON moz_link_intersect_snapshots(site_id, collected_day DESC);
CREATE INDEX IF NOT EXISTS idx_moz_intersect_cluster_day
  ON moz_link_intersect_snapshots(cluster, collected_day DESC);
CREATE INDEX IF NOT EXISTS idx_moz_intersect_day_geo
  ON moz_link_intersect_snapshots(collected_day, geo_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_moz_intersect_job
  ON moz_link_intersect_snapshots(job_id);

-- Rebuild URL metrics snapshots with usage/traceability columns.
CREATE TABLE moz_url_metrics_snapshots_new (
  snapshot_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  url_id TEXT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
  collected_day TEXT NOT NULL,
  geo_key TEXT NOT NULL DEFAULT 'us',
  page_authority REAL,
  domain_authority REAL,
  spam_score REAL,
  linking_domains INTEGER,
  external_links INTEGER,
  metrics_json TEXT,
  rows_used INTEGER NOT NULL DEFAULT 0,
  site_run_id TEXT,
  job_id TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(url_id, collected_day, geo_key)
);

INSERT OR REPLACE INTO moz_url_metrics_snapshots_new (
  snapshot_id, url_id, collected_day, geo_key, page_authority, domain_authority,
  spam_score, linking_domains, external_links, metrics_json,
  rows_used, site_run_id, job_id, created_at
)
SELECT
  snapshot_id,
  url_id,
  collected_day,
  geo_key,
  page_authority,
  domain_authority,
  spam_score,
  linking_domains,
  external_links,
  metrics_json,
  1,
  NULL,
  job_id,
  created_at
FROM moz_url_metrics_snapshots;

DROP TABLE moz_url_metrics_snapshots;
ALTER TABLE moz_url_metrics_snapshots_new RENAME TO moz_url_metrics_snapshots;

CREATE INDEX IF NOT EXISTS idx_moz_url_metrics_day_geo
  ON moz_url_metrics_snapshots(collected_day, geo_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_moz_url_metrics_url_day
  ON moz_url_metrics_snapshots(url_id, collected_day DESC);
CREATE INDEX IF NOT EXISTS idx_moz_url_metrics_job
  ON moz_url_metrics_snapshots(job_id);

PRAGMA foreign_keys = ON;
