CREATE TABLE IF NOT EXISTS step2_baselines (
  baseline_snapshot_id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL,
  date_yyyymmdd TEXT NOT NULL,
  run_job_id TEXT,
  summary_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_step2_baselines_site_date
  ON step2_baselines (site_id, date_yyyymmdd DESC);

CREATE TABLE IF NOT EXISTS step2_serp_diffs (
  diff_id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL,
  date_yyyymmdd TEXT NOT NULL,
  keyword TEXT NOT NULL,
  geo TEXT NOT NULL,
  entered_urls_json TEXT NOT NULL DEFAULT '[]',
  dropped_urls_json TEXT NOT NULL DEFAULT '[]',
  rank_delta_json TEXT NOT NULL DEFAULT '[]',
  serp_feature_delta_json TEXT NOT NULL DEFAULT '{}',
  format_delta_json TEXT NOT NULL DEFAULT '{}',
  baseline_delta_json TEXT NOT NULL DEFAULT '[]',
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_step2_serp_diffs_site_date
  ON step2_serp_diffs (site_id, date_yyyymmdd DESC);

CREATE INDEX IF NOT EXISTS idx_step2_serp_diffs_keyword
  ON step2_serp_diffs (site_id, keyword, geo, date_yyyymmdd DESC);

CREATE TABLE IF NOT EXISTS step2_url_diffs (
  diff_id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL,
  date_yyyymmdd TEXT NOT NULL,
  keyword TEXT NOT NULL,
  url TEXT NOT NULL,
  url_hash TEXT NOT NULL,
  field_changes_json TEXT NOT NULL DEFAULT '{}',
  module_changes_json TEXT NOT NULL DEFAULT '{}',
  word_count_delta INTEGER NOT NULL DEFAULT 0,
  internal_inbound_delta INTEGER NOT NULL DEFAULT 0,
  ref_domains_delta INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_step2_url_diffs_site_date
  ON step2_url_diffs (site_id, date_yyyymmdd DESC);

CREATE INDEX IF NOT EXISTS idx_step2_url_diffs_urlhash
  ON step2_url_diffs (url_hash, date_yyyymmdd DESC);

CREATE TABLE IF NOT EXISTS step2_daily_reports (
  report_id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL,
  date_yyyymmdd TEXT NOT NULL,
  run_type TEXT NOT NULL CHECK (run_type IN ('baseline', 'delta')),
  baseline_snapshot_id TEXT,
  summary_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_step2_daily_reports_site_date
  ON step2_daily_reports (site_id, date_yyyymmdd DESC);
