ALTER TABLE serp_runs ADD COLUMN dataset_id TEXT;
ALTER TABLE serp_runs ADD COLUMN mode TEXT NOT NULL DEFAULT 'serp'; -- serp | inspiration_only
ALTER TABLE serp_runs ADD COLUMN fallback_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_serp_runs_user_created_at ON serp_runs (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_serp_results_domain ON serp_results (domain);

CREATE TABLE IF NOT EXISTS user_events (
  event_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN ('like', 'dislike')),
  entity_type TEXT NOT NULL CHECK (entity_type IN ('serp_result', 'inspiration_link')),
  entity_id TEXT NOT NULL,
  payload_json TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_events_user_created_at ON user_events (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS onboarding_question_templates (
  question_id TEXT PRIMARY KEY,
  prompt TEXT NOT NULL,
  options_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

INSERT OR IGNORE INTO onboarding_question_templates (question_id, prompt, options_json, created_at)
VALUES (
  'style_direction_v1',
  'What should we explore next: font, palette, or layout?',
  '["font","palette","layout","show me different styles"]',
  1735689600
);

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
