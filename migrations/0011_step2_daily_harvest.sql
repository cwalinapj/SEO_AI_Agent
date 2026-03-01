CREATE TABLE IF NOT EXISTS step2_serp_snapshots (
  serp_id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL,
  keyword TEXT NOT NULL,
  cluster TEXT NOT NULL,
  intent TEXT NOT NULL,
  geo TEXT NOT NULL,
  date_yyyymmdd TEXT NOT NULL,
  serp_features_json TEXT NOT NULL DEFAULT '[]',
  scraped_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_step2_serp_snapshots_site_date
  ON step2_serp_snapshots (site_id, date_yyyymmdd DESC);

CREATE INDEX IF NOT EXISTS idx_step2_serp_snapshots_keyword_date
  ON step2_serp_snapshots (keyword, date_yyyymmdd DESC);

CREATE TABLE IF NOT EXISTS step2_serp_results (
  result_id TEXT PRIMARY KEY,
  serp_id TEXT NOT NULL,
  rank INTEGER NOT NULL,
  url TEXT NOT NULL,
  url_hash TEXT NOT NULL,
  domain TEXT NOT NULL,
  page_type TEXT NOT NULL,
  title_snippet TEXT,
  desc_snippet TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (serp_id) REFERENCES step2_serp_snapshots(serp_id)
);

CREATE INDEX IF NOT EXISTS idx_step2_serp_results_serp_rank
  ON step2_serp_results (serp_id, rank ASC);

CREATE INDEX IF NOT EXISTS idx_step2_serp_results_urlhash
  ON step2_serp_results (url_hash, created_at DESC);

CREATE TABLE IF NOT EXISTS step2_page_extracts (
  extract_id TEXT PRIMARY KEY,
  url_hash TEXT NOT NULL,
  url TEXT NOT NULL,
  domain TEXT NOT NULL,
  date_yyyymmdd TEXT NOT NULL,
  title TEXT,
  meta_description TEXT,
  robots_meta TEXT,
  canonical_url TEXT,
  hreflang_count INTEGER NOT NULL DEFAULT 0,
  h1_text TEXT,
  h2_json TEXT NOT NULL DEFAULT '[]',
  h3_json TEXT NOT NULL DEFAULT '[]',
  word_count INTEGER NOT NULL DEFAULT 0,
  schema_types_json TEXT NOT NULL DEFAULT '[]',
  internal_links_out_count INTEGER NOT NULL DEFAULT 0,
  internal_anchors_json TEXT NOT NULL DEFAULT '[]',
  external_links_out_count INTEGER NOT NULL DEFAULT 0,
  external_anchors_json TEXT NOT NULL DEFAULT '[]',
  image_count INTEGER NOT NULL DEFAULT 0,
  alt_coverage_rate REAL NOT NULL DEFAULT 1,
  keyword_placement_flags_json TEXT NOT NULL DEFAULT '{}',
  faq_section_present INTEGER NOT NULL DEFAULT 0,
  pricing_section_present INTEGER NOT NULL DEFAULT 0,
  testimonials_present INTEGER NOT NULL DEFAULT 0,
  location_refs_present INTEGER NOT NULL DEFAULT 0,
  how_it_works_present INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE (url_hash, date_yyyymmdd)
);

CREATE INDEX IF NOT EXISTS idx_step2_page_extracts_domain_date
  ON step2_page_extracts (domain, date_yyyymmdd DESC);

CREATE TABLE IF NOT EXISTS step2_url_backlinks (
  backlink_id TEXT PRIMARY KEY,
  url_hash TEXT NOT NULL,
  url TEXT NOT NULL,
  domain TEXT NOT NULL,
  date_yyyymmdd TEXT NOT NULL,
  backlinks INTEGER NOT NULL DEFAULT 0,
  ref_domains INTEGER NOT NULL DEFAULT 0,
  follow_nofollow_json TEXT NOT NULL DEFAULT '{}',
  link_types_json TEXT NOT NULL DEFAULT '{}',
  top_anchors_json TEXT NOT NULL DEFAULT '[]',
  authority_metric REAL NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  UNIQUE (url_hash, date_yyyymmdd)
);

CREATE INDEX IF NOT EXISTS idx_step2_url_backlinks_domain_date
  ON step2_url_backlinks (domain, date_yyyymmdd DESC);

CREATE TABLE IF NOT EXISTS step2_domain_backlinks (
  domain_backlink_id TEXT PRIMARY KEY,
  domain TEXT NOT NULL,
  date_yyyymmdd TEXT NOT NULL,
  authority_metric REAL NOT NULL DEFAULT 0,
  ref_domains INTEGER NOT NULL DEFAULT 0,
  topical_categories_json TEXT NOT NULL DEFAULT '[]',
  created_at INTEGER NOT NULL,
  UNIQUE (domain, date_yyyymmdd)
);

CREATE INDEX IF NOT EXISTS idx_step2_domain_backlinks_domain_date
  ON step2_domain_backlinks (domain, date_yyyymmdd DESC);

CREATE TABLE IF NOT EXISTS step2_internal_graph_edges (
  edge_id TEXT PRIMARY KEY,
  domain TEXT NOT NULL,
  date_yyyymmdd TEXT NOT NULL,
  from_url TEXT NOT NULL,
  to_url TEXT NOT NULL,
  anchor TEXT,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_step2_internal_graph_edges_domain_date
  ON step2_internal_graph_edges (domain, date_yyyymmdd DESC);

CREATE INDEX IF NOT EXISTS idx_step2_internal_graph_edges_to
  ON step2_internal_graph_edges (to_url, date_yyyymmdd DESC);

CREATE TABLE IF NOT EXISTS step2_ai_analyses (
  analysis_id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL,
  date_yyyymmdd TEXT NOT NULL,
  scope TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  rank_band TEXT NOT NULL,
  findings_json TEXT NOT NULL,
  recommendations_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_step2_ai_analyses_site_date
  ON step2_ai_analyses (site_id, date_yyyymmdd DESC, scope);
