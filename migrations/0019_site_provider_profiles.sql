PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS site_provider_profiles (
  site_id TEXT PRIMARY KEY REFERENCES wp_ai_seo_sites(site_id) ON DELETE CASCADE,
  serp_provider TEXT NOT NULL DEFAULT 'headless_google',
  page_provider TEXT NOT NULL DEFAULT 'direct_fetch',
  geo_provider TEXT NOT NULL DEFAULT 'proxy_lease_pool',
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  CHECK (serp_provider IN ('decodo_serp_api', 'headless_google')),
  CHECK (page_provider IN ('decodo_web_api', 'direct_fetch')),
  CHECK (geo_provider IN ('decodo_geo', 'proxy_lease_pool'))
);

CREATE INDEX IF NOT EXISTS idx_site_provider_profiles_updated_at
  ON site_provider_profiles(updated_at DESC);
