-- Unified D1 migration for Step 2/3 entities.
-- This migration is designed to be additive with existing legacy tables.
-- It upgrades legacy `sites` and `keywords` tables in-place and creates
-- canonical run/page/link/task entities.

PRAGMA foreign_keys = ON;

------------------------------------------------------------
-- 1) SITES + SIGNALS + BRIEFS
------------------------------------------------------------

-- Ensure legacy sites table exists (older migrations may already create it).
CREATE TABLE IF NOT EXISTS sites (
  site_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  production_url TEXT NOT NULL,
  default_strategy TEXT NOT NULL DEFAULT 'mobile',
  last_deploy_hash TEXT,
  last_speed_check_at INTEGER
);

-- Add unified site metadata columns to existing `sites`.
ALTER TABLE sites ADD COLUMN wp_site_id TEXT;
ALTER TABLE sites ADD COLUMN site_url TEXT;
ALTER TABLE sites ADD COLUMN site_host TEXT COLLATE NOCASE;
ALTER TABLE sites ADD COLUMN plan_tier TEXT NOT NULL DEFAULT 'standard';
ALTER TABLE sites ADD COLUMN geo_mode TEXT NOT NULL DEFAULT 'us';
ALTER TABLE sites ADD COLUMN metro_name TEXT;
ALTER TABLE sites ADD COLUMN metro_slug TEXT;
ALTER TABLE sites ADD COLUMN timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles';
ALTER TABLE sites ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1;
ALTER TABLE sites ADD COLUMN created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'));
ALTER TABLE sites ADD COLUMN updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'));

CREATE UNIQUE INDEX IF NOT EXISTS idx_sites_wp_site_id ON sites(wp_site_id) WHERE wp_site_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sites_site_host ON sites(site_host);
CREATE INDEX IF NOT EXISTS idx_sites_is_active ON sites(is_active);

CREATE TABLE IF NOT EXISTS site_signals (
  site_id TEXT PRIMARY KEY REFERENCES sites(site_id) ON DELETE CASCADE,
  signals_json TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_site_signals_updated ON site_signals(updated_at DESC);

CREATE TABLE IF NOT EXISTS site_briefs (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_id TEXT NOT NULL REFERENCES sites(site_id) ON DELETE CASCADE,
  brief_json TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_site_briefs_site_created ON site_briefs(site_id, created_at DESC);

------------------------------------------------------------
-- 2) STEP 1 KEYWORDS (20 capped per active set)
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS keyword_sets (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_id TEXT NOT NULL REFERENCES sites(site_id) ON DELETE CASCADE,
  source TEXT NOT NULL DEFAULT 'auto',
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_keyword_sets_site_active ON keyword_sets(site_id, is_active);
CREATE INDEX IF NOT EXISTS idx_keyword_sets_site_created ON keyword_sets(site_id, created_at DESC);

-- Ensure legacy keywords exists (older migrations should already define it).
CREATE TABLE IF NOT EXISTS keywords (
  kw_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  phrase TEXT NOT NULL,
  region_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

-- Upgrade legacy keywords for unified Step 1 model.
ALTER TABLE keywords ADD COLUMN keyword_set_id TEXT REFERENCES keyword_sets(id) ON DELETE CASCADE;
ALTER TABLE keywords ADD COLUMN keyword TEXT;
ALTER TABLE keywords ADD COLUMN keyword_norm TEXT;
ALTER TABLE keywords ADD COLUMN priority INTEGER;
ALTER TABLE keywords ADD COLUMN cluster TEXT;
ALTER TABLE keywords ADD COLUMN intent TEXT;
ALTER TABLE keywords ADD COLUMN is_local_intent INTEGER NOT NULL DEFAULT 0;
ALTER TABLE keywords ADD COLUMN target_page_type TEXT;
ALTER TABLE keywords ADD COLUMN target_slug TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_keywords_set_keyword_norm
  ON keywords(keyword_set_id, keyword_norm)
  WHERE keyword_set_id IS NOT NULL AND keyword_norm IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_keywords_set_priority ON keywords(keyword_set_id, priority);
CREATE INDEX IF NOT EXISTS idx_keywords_set_cluster ON keywords(keyword_set_id, cluster);

CREATE TABLE IF NOT EXISTS keyword_metrics (
  keyword_id TEXT PRIMARY KEY REFERENCES keywords(kw_id) ON DELETE CASCADE,
  volume_us INTEGER,
  kd REAL,
  cpc_usd REAL,
  updated_at INTEGER,
  metrics_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_keyword_metrics_updated ON keyword_metrics(updated_at DESC);

CREATE TRIGGER IF NOT EXISTS trg_keywords_limit_20
BEFORE INSERT ON keywords
FOR EACH ROW
WHEN NEW.keyword_set_id IS NOT NULL
BEGIN
  SELECT CASE
    WHEN (
      SELECT COUNT(1)
      FROM keywords
      WHERE keyword_set_id = NEW.keyword_set_id
    ) >= 20
    THEN RAISE(ABORT, 'keyword_set exceeds 20 keywords cap')
  END;
END;

------------------------------------------------------------
-- 3) SITE RUNS + JOB GROUPING
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS site_runs (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_id TEXT NOT NULL REFERENCES sites(site_id) ON DELETE CASCADE,
  keyword_set_id TEXT NOT NULL REFERENCES keyword_sets(id) ON DELETE RESTRICT,
  run_type TEXT NOT NULL, -- baseline|delta|auto
  run_date TEXT NOT NULL, -- YYYY-MM-DD
  geo_mode TEXT NOT NULL DEFAULT 'us',
  status TEXT NOT NULL DEFAULT 'queued', -- queued|running|partial|success|failed
  started_at INTEGER,
  finished_at INTEGER,
  stats_json TEXT NOT NULL DEFAULT '{}',
  error_json TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(site_id, run_date, run_type)
);

CREATE INDEX IF NOT EXISTS idx_site_runs_site_date ON site_runs(site_id, run_date DESC);
CREATE INDEX IF NOT EXISTS idx_site_runs_status ON site_runs(status);

CREATE TABLE IF NOT EXISTS site_run_jobs (
  site_run_id TEXT NOT NULL REFERENCES site_runs(id) ON DELETE CASCADE,
  job_id TEXT NOT NULL,
  job_kind TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  PRIMARY KEY (site_run_id, job_id)
);

CREATE INDEX IF NOT EXISTS idx_site_run_jobs_job ON site_run_jobs(job_id);

------------------------------------------------------------
-- 4) URL NORMALIZATION + PAGE SNAPSHOTS
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS urls (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  url TEXT NOT NULL,
  url_hash TEXT NOT NULL,
  domain TEXT NOT NULL COLLATE NOCASE,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(url_hash)
);

CREATE INDEX IF NOT EXISTS idx_urls_domain ON urls(domain);

CREATE TABLE IF NOT EXISTS serp_result_url_map (
  serp_id TEXT NOT NULL,
  rank INTEGER NOT NULL,
  url_id TEXT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
  page_type TEXT,
  geo TEXT,
  PRIMARY KEY (serp_id, rank)
);

CREATE INDEX IF NOT EXISTS idx_serp_result_url_map_url ON serp_result_url_map(url_id);
CREATE INDEX IF NOT EXISTS idx_serp_result_url_map_serp ON serp_result_url_map(serp_id);

CREATE TABLE IF NOT EXISTS page_snapshots (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  url_id TEXT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
  fetched_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  http_status INTEGER,
  content_hash TEXT,
  extracted_json TEXT NOT NULL,
  raw_r2_key TEXT,
  raw_sha256 TEXT,
  parser_version TEXT,
  UNIQUE(url_id, content_hash)
);

CREATE INDEX IF NOT EXISTS idx_page_snapshots_url_time ON page_snapshots(url_id, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_page_snapshots_time ON page_snapshots(fetched_at DESC);

CREATE TABLE IF NOT EXISTS page_diffs (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_run_id TEXT NOT NULL REFERENCES site_runs(id) ON DELETE CASCADE,
  url_id TEXT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
  diff_json TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_page_diffs_run ON page_diffs(site_run_id);
CREATE INDEX IF NOT EXISTS idx_page_diffs_url_time ON page_diffs(url_id, created_at DESC);

------------------------------------------------------------
-- 5) BACKLINK METRICS
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS backlink_url_metrics (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  url_id TEXT NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
  provider TEXT NOT NULL DEFAULT 'semrush',
  collected_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  backlinks INTEGER,
  referring_domains INTEGER,
  follow_count INTEGER,
  nofollow_count INTEGER,
  top_anchors_json TEXT,
  metrics_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_bl_url_metrics_url_time ON backlink_url_metrics(url_id, collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_bl_url_metrics_provider ON backlink_url_metrics(provider);

CREATE TABLE IF NOT EXISTS backlink_domain_metrics (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  domain TEXT NOT NULL COLLATE NOCASE,
  provider TEXT NOT NULL DEFAULT 'semrush',
  collected_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  authority_score REAL,
  referring_domains INTEGER,
  backlinks INTEGER,
  topical_categories_json TEXT,
  metrics_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_bl_domain_metrics_domain_time ON backlink_domain_metrics(domain, collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_bl_domain_metrics_provider ON backlink_domain_metrics(provider);

CREATE TABLE IF NOT EXISTS link_diffs (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_run_id TEXT NOT NULL REFERENCES site_runs(id) ON DELETE CASCADE,
  scope TEXT NOT NULL, -- url|domain
  url_id TEXT REFERENCES urls(id) ON DELETE CASCADE,
  domain TEXT COLLATE NOCASE,
  diff_json TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_link_diffs_run ON link_diffs(site_run_id);
CREATE INDEX IF NOT EXISTS idx_link_diffs_scope_time ON link_diffs(scope, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_link_diffs_domain_time ON link_diffs(domain, created_at DESC);

------------------------------------------------------------
-- 6) INTERNAL GRAPH STATS
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS internal_graph_runs (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  domain TEXT NOT NULL COLLATE NOCASE,
  started_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  finished_at INTEGER,
  status TEXT NOT NULL DEFAULT 'queued',
  stats_json TEXT NOT NULL DEFAULT '{}',
  error_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_internal_graph_runs_domain_time ON internal_graph_runs(domain, started_at DESC);

CREATE TABLE IF NOT EXISTS internal_graph_url_stats (
  graph_run_id TEXT NOT NULL REFERENCES internal_graph_runs(id) ON DELETE CASCADE,
  url TEXT NOT NULL,
  inbound_count INTEGER NOT NULL DEFAULT 0,
  depth_from_home INTEGER,
  top_internal_anchors_json TEXT,
  PRIMARY KEY (graph_run_id, url)
);

CREATE INDEX IF NOT EXISTS idx_internal_graph_url_stats_inbound
  ON internal_graph_url_stats(graph_run_id, inbound_count DESC);

------------------------------------------------------------
-- 7) STEP 2 DIFFS + AI REPORTS
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS serp_diffs (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_run_id TEXT NOT NULL REFERENCES site_runs(id) ON DELETE CASCADE,
  keyword_id TEXT NOT NULL REFERENCES keywords(kw_id) ON DELETE CASCADE,
  geo TEXT NOT NULL,
  diff_json TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(site_run_id, keyword_id, geo)
);

CREATE INDEX IF NOT EXISTS idx_serp_diffs_keyword_geo_time ON serp_diffs(keyword_id, geo, created_at DESC);

CREATE TABLE IF NOT EXISTS ai_reports (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_run_id TEXT NOT NULL REFERENCES site_runs(id) ON DELETE CASCADE,
  scope TEXT NOT NULL, -- keyword|cluster|site
  keyword_id TEXT REFERENCES keywords(kw_id) ON DELETE CASCADE,
  cluster TEXT,
  rank_band TEXT,
  report_json TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_ai_reports_run_scope ON ai_reports(site_run_id, scope);
CREATE INDEX IF NOT EXISTS idx_ai_reports_keyword_time ON ai_reports(keyword_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ai_reports_cluster_time ON ai_reports(cluster, created_at DESC);

------------------------------------------------------------
-- 8) STEP 3 TASKS + BOARD CACHE
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL REFERENCES sites(site_id) ON DELETE CASCADE,
  site_run_id TEXT REFERENCES site_runs(id) ON DELETE SET NULL,
  category TEXT NOT NULL,
  type TEXT NOT NULL,
  title TEXT NOT NULL,
  priority TEXT NOT NULL, -- P0|P1|P2|P3
  mode TEXT NOT NULL, -- AUTO|DIY|TEAM
  effort TEXT NOT NULL, -- S|M|L
  status TEXT NOT NULL DEFAULT 'NEW',
  requires_access_json TEXT NOT NULL DEFAULT '[]',
  blocker_codes_json TEXT NOT NULL DEFAULT '[]',
  scope_json TEXT NOT NULL DEFAULT '{}',
  task_json TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_tasks_site_status ON tasks(site_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_site_priority ON tasks(site_id, priority);
CREATE INDEX IF NOT EXISTS idx_tasks_site_mode ON tasks(site_id, mode);
CREATE INDEX IF NOT EXISTS idx_tasks_site_category ON tasks(site_id, category);
CREATE INDEX IF NOT EXISTS idx_tasks_site_updated ON tasks(site_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS task_status_events (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  site_id TEXT NOT NULL REFERENCES sites(site_id) ON DELETE CASCADE,
  event_type TEXT NOT NULL, -- status_change|comment|auto_applied|blocked|unblocked
  from_status TEXT,
  to_status TEXT,
  actor TEXT NOT NULL DEFAULT 'system', -- system|user|team
  message TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_task_status_events_task_time ON task_status_events(task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_task_status_events_site_time ON task_status_events(site_id, created_at DESC);

CREATE TABLE IF NOT EXISTS task_board_cache (
  site_id TEXT PRIMARY KEY REFERENCES sites(site_id) ON DELETE CASCADE,
  site_run_id TEXT,
  board_json TEXT NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

------------------------------------------------------------
-- 9) COMPETITOR SETS + SOCIAL PROFILES
------------------------------------------------------------

CREATE TABLE IF NOT EXISTS competitor_sets (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_id TEXT NOT NULL REFERENCES sites(site_id) ON DELETE CASCADE,
  site_run_id TEXT REFERENCES site_runs(id) ON DELETE SET NULL,
  geo TEXT NOT NULL DEFAULT 'us',
  domains_json TEXT NOT NULL,
  derivation_json TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_competitor_sets_site_time ON competitor_sets(site_id, created_at DESC);

CREATE TABLE IF NOT EXISTS competitor_social_profiles (
  id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  competitor_set_id TEXT NOT NULL REFERENCES competitor_sets(id) ON DELETE CASCADE,
  domain TEXT NOT NULL COLLATE NOCASE,
  platform TEXT NOT NULL,
  profile_url TEXT NOT NULL,
  discovered_from TEXT NOT NULL DEFAULT 'website',
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  UNIQUE(competitor_set_id, platform, profile_url)
);

CREATE INDEX IF NOT EXISTS idx_comp_social_set_domain ON competitor_social_profiles(competitor_set_id, domain);

------------------------------------------------------------
-- 10) TASK SUMMARY VIEWS
------------------------------------------------------------

CREATE VIEW IF NOT EXISTS v_task_counts AS
SELECT
  site_id,
  status,
  priority,
  mode,
  category,
  COUNT(1) AS count
FROM tasks
GROUP BY site_id, status, priority, mode, category;

CREATE VIEW IF NOT EXISTS v_task_quick_wins AS
SELECT
  site_id,
  id AS task_id,
  title,
  priority,
  mode,
  updated_at
FROM tasks
WHERE status = 'READY'
  AND mode = 'AUTO'
  AND priority IN ('P0', 'P1')
ORDER BY updated_at DESC;
