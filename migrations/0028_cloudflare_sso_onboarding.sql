PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS cloudflare_oauth_states (
  state TEXT PRIMARY KEY,
  site_id TEXT NOT NULL REFERENCES wp_ai_seo_sites(site_id) ON DELETE CASCADE,
  subscription_id TEXT REFERENCES billing_site_subscriptions(subscription_id) ON DELETE SET NULL,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  consumed_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_cf_oauth_states_site_time
  ON cloudflare_oauth_states(site_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_cf_oauth_states_expiry
  ON cloudflare_oauth_states(expires_at);

CREATE TABLE IF NOT EXISTS cloudflare_site_connections (
  connection_id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL UNIQUE REFERENCES wp_ai_seo_sites(site_id) ON DELETE CASCADE,
  subscription_id TEXT REFERENCES billing_site_subscriptions(subscription_id) ON DELETE SET NULL,
  status TEXT NOT NULL,
  auth_mode TEXT NOT NULL CHECK (auth_mode IN ('oauth', 'api_token')),
  cloudflare_user_id TEXT,
  cloudflare_account_id TEXT,
  cloudflare_account_name TEXT,
  scopes_json TEXT NOT NULL DEFAULT '[]',
  token_ciphertext TEXT,
  token_expires_at INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cf_connections_status_time
  ON cloudflare_site_connections(status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_cf_connections_account
  ON cloudflare_site_connections(cloudflare_account_id);
