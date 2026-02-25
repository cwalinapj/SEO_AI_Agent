ALTER TABLE keywords ADD COLUMN kw TEXT;
ALTER TABLE keywords ADD COLUMN geo_bucket TEXT;
ALTER TABLE keywords ADD COLUMN serp_top_domains_json TEXT;

ALTER TABLE kw_metrics ADD COLUMN volume INTEGER;
ALTER TABLE kw_metrics ADD COLUMN cpc REAL;

CREATE TABLE IF NOT EXISTS kw_intent (
  kw_id TEXT PRIMARY KEY,
  intent_bucket TEXT NOT NULL,
  confidence REAL NOT NULL,
  scores_json TEXT NOT NULL,
  explanation TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cohort_stats (
  cohort_id TEXT PRIMARY KEY,
  vertical TEXT NOT NULL,
  geo_bucket TEXT NOT NULL,
  intent_bucket TEXT NOT NULL,
  device TEXT NOT NULL,
  impressions INTEGER NOT NULL DEFAULT 0,
  conversions INTEGER NOT NULL DEFAULT 0,
  conversion_rate REAL NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL
);
