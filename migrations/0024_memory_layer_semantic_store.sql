PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS memory_items (
  memory_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_id TEXT NOT NULL REFERENCES wp_ai_seo_sites(site_id) ON DELETE CASCADE,
  user_id TEXT,
  type TEXT NOT NULL,
  scope_key TEXT,
  collected_day TEXT NOT NULL,
  geo_key TEXT NOT NULL DEFAULT 'us',
  title TEXT,
  text_summary TEXT NOT NULL,
  tags_json TEXT NOT NULL DEFAULT '{}',
  source_r2_key TEXT NOT NULL,
  source_sha256 TEXT,
  vector_namespace TEXT NOT NULL DEFAULT 'mem_v1',
  vector_id TEXT NOT NULL,
  embedding_model TEXT NOT NULL DEFAULT 'text-embedding-3-large',
  token_count INTEGER,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_memory_site_day
  ON memory_items(site_id, collected_day DESC);

CREATE INDEX IF NOT EXISTS idx_memory_site_type_day
  ON memory_items(site_id, type, collected_day DESC);

CREATE INDEX IF NOT EXISTS idx_memory_site_geo_day
  ON memory_items(site_id, geo_key, collected_day DESC);

CREATE INDEX IF NOT EXISTS idx_memory_vector_id
  ON memory_items(vector_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_memory_scope_day
  ON memory_items(site_id, type, COALESCE(scope_key,''), collected_day, geo_key);

CREATE TABLE IF NOT EXISTS memory_events (
  event_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  memory_id TEXT REFERENCES memory_items(memory_id) ON DELETE SET NULL,
  site_id TEXT NOT NULL REFERENCES wp_ai_seo_sites(site_id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  message TEXT,
  meta_json TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_memory_events_site_time
  ON memory_events(site_id, created_at DESC);
