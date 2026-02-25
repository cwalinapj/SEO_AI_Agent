CREATE TABLE IF NOT EXISTS sites (
  site_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  production_url TEXT NOT NULL,
  default_strategy TEXT NOT NULL CHECK (default_strategy IN ('mobile', 'desktop')) DEFAULT 'mobile',
  last_deploy_hash TEXT
);

CREATE TABLE IF NOT EXISTS speed_snapshots (
  snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id TEXT NOT NULL,
  date INTEGER NOT NULL,
  strategy TEXT NOT NULL CHECK (strategy IN ('mobile', 'desktop')),
  lcp_ms INTEGER,
  cls REAL,
  tbt_ms INTEGER,
  fcp_ms INTEGER,
  field_lcp_pctl INTEGER,
  field_cls_pctl REAL,
  field_inp_pctl INTEGER,
  performance_score REAL,
  psi_fetch_time TEXT NOT NULL,
  trigger_reason TEXT NOT NULL CHECK (trigger_reason IN ('deploy', 'manual', 'failsafe')),
  deploy_hash TEXT,
  FOREIGN KEY (site_id) REFERENCES sites(site_id)
);

CREATE INDEX IF NOT EXISTS idx_speed_snapshots_site_strategy_date
  ON speed_snapshots (site_id, strategy, date DESC);

CREATE TRIGGER IF NOT EXISTS speed_snapshots_cooldown_bi
BEFORE INSERT ON speed_snapshots
WHEN EXISTS (
  SELECT 1
  FROM speed_snapshots s
  WHERE s.site_id = NEW.site_id
    AND s.strategy = NEW.strategy
    AND s.date > NEW.date - 43200
)
BEGIN
  SELECT RAISE(ABORT, 'speed snapshot cooldown active');
END;

CREATE VIEW IF NOT EXISTS speed_snapshot_deltas AS
WITH paired AS (
  SELECT
    curr.snapshot_id,
    curr.site_id,
    curr.date,
    curr.strategy,
    curr.deploy_hash,
    curr.trigger_reason,
    curr.lcp_ms,
    curr.cls,
    curr.tbt_ms,
    curr.field_lcp_pctl,
    curr.field_inp_pctl,
    prev.lcp_ms AS prev_lcp_ms,
    prev.cls AS prev_cls,
    prev.tbt_ms AS prev_tbt_ms,
    prev.field_lcp_pctl AS prev_field_lcp_pctl,
    prev.field_inp_pctl AS prev_field_inp_pctl
  FROM speed_snapshots curr
  LEFT JOIN speed_snapshots prev
    ON prev.snapshot_id = (
      SELECT p.snapshot_id
      FROM speed_snapshots p
      WHERE p.site_id = curr.site_id
        AND p.strategy = curr.strategy
        AND p.date < curr.date
      ORDER BY p.date DESC
      LIMIT 1
    )
)
SELECT
  snapshot_id,
  site_id,
  date,
  strategy,
  trigger_reason,
  deploy_hash,
  lcp_ms - prev_lcp_ms AS delta_lcp_ms,
  cls - prev_cls AS delta_cls,
  tbt_ms - prev_tbt_ms AS delta_tbt_ms,
  field_lcp_pctl - prev_field_lcp_pctl AS delta_field_lcp_pctl,
  field_inp_pctl - prev_field_inp_pctl AS delta_field_inp_pctl,
  CASE
    WHEN (lcp_ms - prev_lcp_ms) > 700 OR (cls - prev_cls) > 0.1 THEN 'critical'
    WHEN (lcp_ms - prev_lcp_ms) >= 300 OR (cls - prev_cls) >= 0.03 THEN 'warn'
    ELSE NULL
  END AS severity,
  CASE
    WHEN (lcp_ms - prev_lcp_ms) >= 300
      OR (cls - prev_cls) >= 0.03
      OR (tbt_ms - prev_tbt_ms) >= 150
      OR (prev_field_lcp_pctl IS NOT NULL AND field_lcp_pctl IS NOT NULL AND (field_lcp_pctl - prev_field_lcp_pctl) >= 200)
      OR (prev_field_inp_pctl IS NOT NULL AND field_inp_pctl IS NOT NULL AND (field_inp_pctl - prev_field_inp_pctl) >= 100)
      THEN 1
    ELSE 0
  END AS should_create_note,
  'LCP regressed +' || CAST((lcp_ms - prev_lcp_ms) AS TEXT) || 'ms after deploy ' || COALESCE(deploy_hash, 'unknown') || ' (' || strategy || ').' AS suggested_note
FROM paired;
