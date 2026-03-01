CREATE TABLE IF NOT EXISTS step3_runs (
  run_id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL,
  date_yyyymmdd TEXT NOT NULL,
  source_step2_date TEXT,
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'partial', 'success', 'failed')),
  summary_json TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_step3_runs_site_date
  ON step3_runs (site_id, date_yyyymmdd DESC);

CREATE TABLE IF NOT EXISTS step3_tasks (
  task_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  site_id TEXT NOT NULL,
  task_group TEXT NOT NULL,
  task_type TEXT NOT NULL,
  execution_mode TEXT NOT NULL CHECK (execution_mode IN ('auto_safe', 'assisted', 'team_only')),
  priority INTEGER NOT NULL DEFAULT 3,
  title TEXT NOT NULL,
  why_text TEXT,
  details_json TEXT NOT NULL DEFAULT '{}',
  target_slug TEXT,
  target_url TEXT,
  status TEXT NOT NULL DEFAULT 'planned' CHECK (status IN ('planned', 'applied', 'draft', 'blocked')),
  created_at INTEGER NOT NULL,
  FOREIGN KEY (run_id) REFERENCES step3_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_step3_tasks_run_priority
  ON step3_tasks (run_id, priority ASC, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_step3_tasks_site_mode
  ON step3_tasks (site_id, execution_mode, created_at DESC);

CREATE TABLE IF NOT EXISTS step3_competitors (
  competitor_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  site_id TEXT NOT NULL,
  domain TEXT NOT NULL,
  source TEXT NOT NULL,
  appearance_count INTEGER NOT NULL DEFAULT 0,
  avg_rank REAL NOT NULL DEFAULT 0,
  is_directory INTEGER NOT NULL DEFAULT 0,
  sample_urls_json TEXT NOT NULL DEFAULT '[]',
  social_profiles_json TEXT NOT NULL DEFAULT '[]',
  created_at INTEGER NOT NULL,
  FOREIGN KEY (run_id) REFERENCES step3_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_step3_competitors_run
  ON step3_competitors (run_id, appearance_count DESC, avg_rank ASC);

CREATE INDEX IF NOT EXISTS idx_step3_competitors_site_domain
  ON step3_competitors (site_id, domain, created_at DESC);

CREATE TABLE IF NOT EXISTS step3_social_signals (
  signal_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  site_id TEXT NOT NULL,
  domain TEXT NOT NULL,
  platform TEXT NOT NULL,
  cadence_per_week REAL,
  engagement_proxy REAL,
  content_types_json TEXT NOT NULL DEFAULT '[]',
  recurring_themes_json TEXT NOT NULL DEFAULT '[]',
  source TEXT NOT NULL DEFAULT 'public_metadata_inference',
  created_at INTEGER NOT NULL,
  FOREIGN KEY (run_id) REFERENCES step3_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_step3_social_run_domain
  ON step3_social_signals (run_id, domain, platform);

CREATE TABLE IF NOT EXISTS step3_risk_flags (
  flag_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  site_id TEXT NOT NULL,
  risk_type TEXT NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high')),
  message TEXT NOT NULL,
  blocked_action TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (run_id) REFERENCES step3_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_step3_risk_flags_run
  ON step3_risk_flags (run_id, severity, created_at DESC);

CREATE TABLE IF NOT EXISTS step3_reports (
  report_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  site_id TEXT NOT NULL,
  date_yyyymmdd TEXT NOT NULL,
  report_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (run_id) REFERENCES step3_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_step3_reports_site_date
  ON step3_reports (site_id, date_yyyymmdd DESC, created_at DESC);
