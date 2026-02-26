CREATE TABLE IF NOT EXISTS wp_schema_profiles (
  session_id TEXT PRIMARY KEY,
  schema_status TEXT NOT NULL DEFAULT 'not_started',
  schema_profile_json TEXT,
  schema_jsonld TEXT,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wp_schema_profiles_updated_at
  ON wp_schema_profiles (updated_at DESC);

CREATE TABLE IF NOT EXISTS wp_redirect_profiles (
  session_id TEXT PRIMARY KEY,
  checked_link_count INTEGER NOT NULL DEFAULT 0,
  broken_link_count INTEGER NOT NULL DEFAULT 0,
  redirect_paths_json TEXT NOT NULL DEFAULT '[]',
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wp_redirect_profiles_updated_at
  ON wp_redirect_profiles (updated_at DESC);
