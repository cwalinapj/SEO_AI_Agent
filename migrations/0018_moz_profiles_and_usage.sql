PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS moz_site_profiles (
  site_id TEXT PRIMARY KEY REFERENCES wp_ai_seo_sites(site_id) ON DELETE CASCADE,
  moz_profile TEXT NOT NULL DEFAULT 'single_site_max', -- single_site_max|scalable_delta
  monthly_rows_budget INTEGER NOT NULL DEFAULT 15000,
  weekly_focus_url_count INTEGER NOT NULL DEFAULT 20,
  daily_keyword_depth INTEGER NOT NULL DEFAULT 20,
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  CHECK (moz_profile IN ('single_site_max', 'scalable_delta')),
  CHECK (monthly_rows_budget >= 0),
  CHECK (weekly_focus_url_count >= 1 AND weekly_focus_url_count <= 500),
  CHECK (daily_keyword_depth >= 1 AND daily_keyword_depth <= 20)
);

CREATE TABLE IF NOT EXISTS moz_job_row_usage (
  usage_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_id TEXT REFERENCES wp_ai_seo_sites(site_id) ON DELETE SET NULL,
  collected_day TEXT NOT NULL,
  job_id TEXT,
  job_type TEXT NOT NULL,
  rows_used INTEGER NOT NULL DEFAULT 0,
  degraded_mode INTEGER NOT NULL DEFAULT 0 CHECK (degraded_mode IN (0,1)),
  fallback_reason TEXT,
  profile TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_moz_job_usage_site_day
  ON moz_job_row_usage(site_id, collected_day, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_moz_job_usage_job
  ON moz_job_row_usage(job_id);
