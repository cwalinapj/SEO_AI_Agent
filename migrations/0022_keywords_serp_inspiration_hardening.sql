PRAGMA foreign_keys = OFF;

-- serp_runs/serp_results rebuilds temporarily invalidate this view.
DROP VIEW IF EXISTS onboarding_next_links;
DROP VIEW IF EXISTS keyword_device_selection;
DROP VIEW IF EXISTS keyword_device_expected_value;

-- Keyword legacy table is intentionally left in-place to avoid breaking
-- existing keyword-device views/triggers in older environments.
CREATE INDEX IF NOT EXISTS idx_keywords_user ON keywords(user_id);
CREATE INDEX IF NOT EXISTS idx_keywords_user_norm_expr
  ON keywords(user_id, lower(trim(phrase)));

-- ---------------------------
-- kw_metrics (provider-aware + FK + defaults)
-- ---------------------------
CREATE TABLE kw_metrics_new (
  kw_id TEXT PRIMARY KEY REFERENCES keywords(kw_id) ON DELETE CASCADE,
  avg_cpc_micros INTEGER,
  monthly_volume INTEGER,
  difficulty INTEGER,
  volume INTEGER,
  cpc REAL,
  source TEXT NOT NULL DEFAULT 'unknown',
  metrics_json TEXT,
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

INSERT INTO kw_metrics_new (
  kw_id, avg_cpc_micros, monthly_volume, difficulty, volume, cpc, source, metrics_json, updated_at
)
SELECT
  kw_id,
  avg_cpc_micros,
  monthly_volume,
  difficulty,
  NULL,
  NULL,
  'unknown',
  NULL,
  COALESCE(updated_at, CAST(strftime('%s','now') AS INTEGER))
FROM kw_metrics;

DROP TABLE kw_metrics;
ALTER TABLE kw_metrics_new RENAME TO kw_metrics;

CREATE INDEX IF NOT EXISTS idx_kw_metrics_updated ON kw_metrics(updated_at DESC);

-- ---------------------------
-- serp_runs (provider-neutral + geo/proxy metadata)
-- ---------------------------
CREATE TABLE serp_runs_new (
  serp_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  phrase TEXT NOT NULL,
  region_json TEXT NOT NULL,
  device TEXT NOT NULL,
  engine TEXT NOT NULL,
  provider TEXT NOT NULL,
  actor TEXT,
  run_id TEXT,
  status TEXT NOT NULL,
  error TEXT,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  dataset_id TEXT,
  mode TEXT NOT NULL DEFAULT 'serp',
  fallback_reason TEXT,
  keyword_norm TEXT NOT NULL,
  region_key TEXT NOT NULL,
  device_key TEXT NOT NULL,
  serp_key TEXT,
  parser_version TEXT,
  raw_payload_sha256 TEXT,
  extractor_mode TEXT,
  geo_key TEXT NOT NULL DEFAULT 'us',
  proxy_lease_id TEXT,
  proxy_geo_json TEXT
);

INSERT INTO serp_runs_new (
  serp_id, user_id, phrase, region_json, device, engine, provider, actor, run_id, status, error,
  created_at, dataset_id, mode, fallback_reason, keyword_norm, region_key, device_key, serp_key,
  parser_version, raw_payload_sha256, extractor_mode, geo_key, proxy_lease_id, proxy_geo_json
)
SELECT
  serp_id,
  user_id,
  phrase,
  region_json,
  device,
  engine,
  provider,
  actor,
  run_id,
  status,
  error,
  COALESCE(created_at, CAST(strftime('%s','now') AS INTEGER)),
  dataset_id,
  COALESCE(NULLIF(mode, ''), 'serp'),
  fallback_reason,
  COALESCE(keyword_norm, LOWER(TRIM(phrase))),
  COALESCE(NULLIF(region_key, ''), LOWER(TRIM(region_json))),
  COALESCE(NULLIF(device_key, ''), LOWER(TRIM(device))),
  serp_key,
  parser_version,
  raw_payload_sha256,
  extractor_mode,
  'us',
  NULL,
  NULL
FROM serp_runs;

DROP TABLE serp_runs;
ALTER TABLE serp_runs_new RENAME TO serp_runs;

CREATE INDEX IF NOT EXISTS idx_serp_runs_user_created_at
  ON serp_runs(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_serp_runs_serp_key
  ON serp_runs(serp_key);
CREATE INDEX IF NOT EXISTS idx_serp_runs_key_created
  ON serp_runs(serp_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_serp_runs_phrase_region
  ON serp_runs(user_id, phrase, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_serp_runs_phrase_region_device_created
  ON serp_runs(keyword_norm, region_key, device_key, created_at DESC);

-- ---------------------------
-- serp_results (FK + URL normalization helpers)
-- ---------------------------
CREATE TABLE serp_results_new (
  serp_id TEXT NOT NULL REFERENCES serp_runs(serp_id) ON DELETE CASCADE,
  rank INTEGER NOT NULL,
  url TEXT NOT NULL,
  url_hash TEXT,
  domain TEXT NOT NULL,
  root_domain TEXT,
  title TEXT,
  snippet TEXT,
  PRIMARY KEY (serp_id, rank)
);

INSERT INTO serp_results_new (
  serp_id, rank, url, url_hash, domain, root_domain, title, snippet
)
SELECT
  serp_id,
  rank,
  url,
  LOWER(TRIM(url)),
  domain,
  LOWER(TRIM(domain)),
  title,
  snippet
FROM serp_results;

DROP TABLE serp_results;
ALTER TABLE serp_results_new RENAME TO serp_results;

CREATE INDEX IF NOT EXISTS idx_serp_results_domain ON serp_results(domain);
CREATE INDEX IF NOT EXISTS idx_serp_results_url ON serp_results(url);
CREATE INDEX IF NOT EXISTS idx_serp_results_url_hash ON serp_results(url_hash);
CREATE INDEX IF NOT EXISTS idx_serp_results_root_domain ON serp_results(root_domain);

-- ---------------------------
-- inspiration_links (hash dedupe + defaults)
-- ---------------------------
CREATE TABLE inspiration_links_new (
  insp_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  source TEXT NOT NULL,
  url TEXT NOT NULL,
  url_hash TEXT NOT NULL,
  meta_json TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

INSERT OR REPLACE INTO inspiration_links_new (
  insp_id, user_id, source, url, url_hash, meta_json, created_at
)
SELECT
  insp_id,
  user_id,
  source,
  url,
  LOWER(TRIM(url)),
  meta_json,
  COALESCE(created_at, CAST(strftime('%s','now') AS INTEGER))
FROM inspiration_links;

DROP TABLE inspiration_links;
ALTER TABLE inspiration_links_new RENAME TO inspiration_links;

CREATE UNIQUE INDEX IF NOT EXISTS idx_inspiration_links_user_url_hash
  ON inspiration_links(user_id, url_hash);
CREATE INDEX IF NOT EXISTS idx_inspiration_links_user_created
  ON inspiration_links(user_id, created_at DESC);

CREATE VIEW IF NOT EXISTS keyword_device_expected_value AS
WITH base AS (
  SELECT
    k.kw_id,
    k.phrase,
    k.vertical,
    k.geo_bucket,
    k.cta_type,
    k.intent_bucket,
    (1.0 + COALESCE(km.monthly_volume, 0) / 1000.0 + COALESCE(km.avg_cpc_micros, 0) / 1000000.0) AS kw_score
  FROM keywords k
  LEFT JOIN kw_metrics km ON km.kw_id = k.kw_id
),
devices AS (
  SELECT 'mobile' AS device
  UNION ALL
  SELECT 'desktop' AS device
)
SELECT
  b.kw_id,
  b.phrase,
  d.device,
  b.kw_score,
  COALESCE(cdp.conversion_rate, 0.0) AS cohort_conversion_rate,
  COALESCE(cdp.confidence, 0.0) AS confidence_weight,
  b.kw_score * COALESCE(cdp.conversion_rate, 0.0) * COALESCE(cdp.confidence, 0.0) AS expected_value
FROM base b
CROSS JOIN devices d
LEFT JOIN cohort_device_priors cdp
  ON cdp.vertical = b.vertical
 AND cdp.geo_bucket = b.geo_bucket
 AND cdp.cta_type = b.cta_type
 AND cdp.intent_bucket = b.intent_bucket
 AND cdp.device = d.device;

CREATE VIEW IF NOT EXISTS keyword_device_selection AS
WITH ev AS (
  SELECT
    kw_id,
    MAX(CASE WHEN device = 'mobile' THEN expected_value END) AS mobile_ev,
    MAX(CASE WHEN device = 'desktop' THEN expected_value END) AS desktop_ev,
    MAX(CASE WHEN device = 'mobile' THEN confidence_weight END) AS mobile_confidence,
    MAX(CASE WHEN device = 'desktop' THEN confidence_weight END) AS desktop_confidence
  FROM keyword_device_expected_value
  GROUP BY kw_id
)
SELECT
  kw_id,
  CASE
    WHEN COALESCE(CASE WHEN mobile_confidence >= desktop_confidence THEN mobile_confidence ELSE desktop_confidence END, 0.0) < 0.2 THEN 'both'
    WHEN COALESCE(mobile_ev, 0.0) >= COALESCE(desktop_ev, 0.0) * 1.2 THEN 'mobile_only'
    WHEN COALESCE(desktop_ev, 0.0) >= COALESCE(mobile_ev, 0.0) * 1.2 THEN 'desktop_only'
    ELSE 'both'
  END AS mode,
  CASE
    WHEN COALESCE(CASE WHEN mobile_confidence >= desktop_confidence THEN mobile_confidence ELSE desktop_confidence END, 0.0) < 0.2 THEN 0.7
    WHEN COALESCE(mobile_ev, 0.0) >= COALESCE(desktop_ev, 0.0) * 1.2 THEN 1.0
    WHEN COALESCE(desktop_ev, 0.0) >= COALESCE(mobile_ev, 0.0) * 1.2 THEN 0.0
    ELSE 0.7
  END AS mobile_weight,
  CASE
    WHEN COALESCE(CASE WHEN mobile_confidence >= desktop_confidence THEN mobile_confidence ELSE desktop_confidence END, 0.0) < 0.2 THEN 0.3
    WHEN COALESCE(mobile_ev, 0.0) >= COALESCE(desktop_ev, 0.0) * 1.2 THEN 0.0
    WHEN COALESCE(desktop_ev, 0.0) >= COALESCE(mobile_ev, 0.0) * 1.2 THEN 1.0
    ELSE 0.3
  END AS desktop_weight,
  CASE
    WHEN COALESCE(CASE WHEN mobile_confidence >= desktop_confidence THEN mobile_confidence ELSE desktop_confidence END, 0.0) < 0.2 THEN 'confidence_gated_default_both'
    WHEN COALESCE(mobile_ev, 0.0) >= COALESCE(desktop_ev, 0.0) * 1.2 THEN 'mobile_ev_dominates'
    WHEN COALESCE(desktop_ev, 0.0) >= COALESCE(mobile_ev, 0.0) * 1.2 THEN 'desktop_ev_dominates'
    ELSE 'ev_close_default_both'
  END AS reason
FROM ev;

-- Restore onboarding helper view.
CREATE VIEW IF NOT EXISTS onboarding_next_links AS
WITH joined AS (
  SELECT
    k.user_id,
    k.phrase AS keyword,
    COALESCE(m.monthly_volume, 0) AS volume,
    COALESCE(m.avg_cpc_micros, 0) AS cpc,
    r.serp_id,
    r.rank,
    r.url,
    r.domain,
    r.title,
    r.snippet,
    CASE
      WHEN r.rank <= 3 THEN 1.0
      WHEN r.rank <= 10 THEN 0.6
      ELSE 0.3
    END AS rank_weight
  FROM keywords k
  JOIN serp_runs sr
    ON sr.user_id = k.user_id
   AND sr.phrase = k.phrase
   AND sr.status = 'ok'
  JOIN serp_results r ON r.serp_id = sr.serp_id
  LEFT JOIN kw_metrics m ON m.kw_id = k.kw_id
),
normalized AS (
  SELECT
    *,
    CASE
      WHEN MAX(volume) OVER (PARTITION BY user_id) = MIN(volume) OVER (PARTITION BY user_id)
        THEN CASE WHEN volume > 0 THEN 1.0 ELSE 0.0 END
      ELSE 1.0 * (volume - MIN(volume) OVER (PARTITION BY user_id))
           / NULLIF(MAX(volume) OVER (PARTITION BY user_id) - MIN(volume) OVER (PARTITION BY user_id), 0)
    END AS volume_norm,
    CASE
      WHEN MAX(cpc) OVER (PARTITION BY user_id) = MIN(cpc) OVER (PARTITION BY user_id)
        THEN CASE WHEN cpc > 0 THEN 1.0 ELSE 0.0 END
      ELSE 1.0 * (cpc - MIN(cpc) OVER (PARTITION BY user_id))
           / NULLIF(MAX(cpc) OVER (PARTITION BY user_id) - MIN(cpc) OVER (PARTITION BY user_id), 0)
    END AS cpc_norm
  FROM joined
),
scored AS (
  SELECT
    *,
    CASE
      WHEN volume > 0 AND cpc > 0 THEN volume_norm * cpc_norm * rank_weight
      ELSE rank_weight
    END AS kw_score
  FROM normalized
),
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY user_id, domain ORDER BY kw_score DESC, rank ASC) AS domain_pick
  FROM scored
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY kw_score DESC, rank ASC) AS link_rank
  FROM deduped
  WHERE domain_pick = 1
)
SELECT
  user_id,
  keyword,
  serp_id,
  rank,
  url,
  domain,
  title,
  snippet,
  kw_score,
  link_rank
FROM ranked
WHERE link_rank <= 5;

PRAGMA foreign_keys = ON;
