PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS provider_actor_registry (
  provider TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  actor_url TEXT NOT NULL,
  purpose TEXT,
  is_default_initial INTEGER NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  PRIMARY KEY (provider, actor_id)
);

CREATE INDEX IF NOT EXISTS idx_provider_actor_registry_default
  ON provider_actor_registry(provider, is_default_initial, is_active);

CREATE TABLE IF NOT EXISTS site_initial_enrichment_runs (
  run_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_id TEXT NOT NULL REFERENCES wp_ai_seo_sites(site_id) ON DELETE CASCADE,
  provider TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  actor_url TEXT NOT NULL,
  domain TEXT NOT NULL COLLATE NOCASE,
  trigger_source TEXT NOT NULL,
  status TEXT NOT NULL, -- running|succeeded|failed|skipped
  item_count INTEGER NOT NULL DEFAULT 0,
  job_id TEXT,
  r2_key TEXT,
  checksum TEXT,
  payload_preview_json TEXT,
  error_json TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_site_initial_enrichment_site_time
  ON site_initial_enrichment_runs(site_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_site_initial_enrichment_status
  ON site_initial_enrichment_runs(status, updated_at DESC);

INSERT INTO provider_actor_registry (
  provider, actor_id, actor_url, purpose, is_default_initial, is_active, created_at, updated_at
) VALUES (
  'apify',
  'X2JFXEVBxFPnnHs7g',
  'https://console.apify.com/actors/X2JFXEVBxFPnnHs7g/',
  'Initial plugin install domain enrichment',
  1,
  1,
  strftime('%s','now'),
  strftime('%s','now')
)
ON CONFLICT(provider, actor_id) DO UPDATE SET
  actor_url = excluded.actor_url,
  purpose = excluded.purpose,
  is_default_initial = excluded.is_default_initial,
  is_active = excluded.is_active,
  updated_at = excluded.updated_at;
