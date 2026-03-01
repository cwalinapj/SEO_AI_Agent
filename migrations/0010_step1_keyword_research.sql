CREATE TABLE IF NOT EXISTS wp_ai_seo_sites (
  site_id TEXT PRIMARY KEY,
  site_url TEXT NOT NULL,
  site_name TEXT,
  business_address TEXT,
  primary_location_hint TEXT,
  site_type_hint TEXT,
  local_mode INTEGER NOT NULL DEFAULT 0 CHECK (local_mode IN (0, 1)),
  input_json TEXT NOT NULL,
  site_profile_json TEXT NOT NULL,
  last_analysis_at INTEGER NOT NULL,
  last_research_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wp_ai_seo_sites_url
  ON wp_ai_seo_sites (site_url);

CREATE TABLE IF NOT EXISTS wp_ai_seo_keywords (
  keyword_id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL,
  research_run_id TEXT NOT NULL,
  keyword TEXT NOT NULL,
  keyword_norm TEXT NOT NULL,
  cluster TEXT NOT NULL,
  intent TEXT NOT NULL CHECK (intent IN ('informational', 'commercial', 'transactional', 'navigational')),
  local_intent TEXT NOT NULL CHECK (local_intent IN ('yes', 'weak', 'no')),
  page_type TEXT NOT NULL,
  recommended_slug TEXT NOT NULL,
  opportunity_score REAL NOT NULL DEFAULT 0,
  selected_tier TEXT NOT NULL CHECK (selected_tier IN ('primary', 'secondary', 'none')),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (site_id) REFERENCES wp_ai_seo_sites(site_id)
);

CREATE INDEX IF NOT EXISTS idx_wp_ai_seo_keywords_site_tier
  ON wp_ai_seo_keywords (site_id, selected_tier, opportunity_score DESC);

CREATE INDEX IF NOT EXISTS idx_wp_ai_seo_keywords_site_norm
  ON wp_ai_seo_keywords (site_id, keyword_norm);

CREATE TABLE IF NOT EXISTS wp_ai_seo_keyword_metrics (
  metric_id TEXT PRIMARY KEY,
  keyword_id TEXT NOT NULL,
  volume_us INTEGER NOT NULL DEFAULT 0,
  kd REAL NOT NULL DEFAULT 0,
  cpc REAL NOT NULL DEFAULT 0,
  competitive_density REAL NOT NULL DEFAULT 0,
  serp_features_json TEXT NOT NULL DEFAULT '[]',
  data_source TEXT NOT NULL DEFAULT 'semrush_or_heuristic',
  created_at INTEGER NOT NULL,
  FOREIGN KEY (keyword_id) REFERENCES wp_ai_seo_keywords(keyword_id)
);

CREATE INDEX IF NOT EXISTS idx_wp_ai_seo_keyword_metrics_keyword
  ON wp_ai_seo_keyword_metrics (keyword_id, created_at DESC);

CREATE TABLE IF NOT EXISTS wp_ai_seo_selections (
  selection_id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL,
  research_run_id TEXT NOT NULL,
  selection_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (site_id) REFERENCES wp_ai_seo_sites(site_id),
  UNIQUE (site_id, research_run_id, selection_type)
);

CREATE INDEX IF NOT EXISTS idx_wp_ai_seo_selections_site_type
  ON wp_ai_seo_selections (site_id, selection_type, created_at DESC);
