-- Sites table (production-only)
CREATE TABLE IF NOT EXISTS sites (
  site_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  production_url TEXT NOT NULL,
  default_strategy TEXT NOT NULL,      -- "mobile" or "desktop"
  last_deploy_hash TEXT,
  last_speed_check_at INTEGER          -- epoch ms
);

-- Speed snapshots (append-only)
CREATE TABLE IF NOT EXISTS speed_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,         -- epoch ms
  strategy TEXT NOT NULL,              -- "mobile" or "desktop"
  trigger_reason TEXT NOT NULL,        -- "deploy" | "manual" | "failsafe"
  deploy_hash TEXT,

  performance_score REAL,              -- 0..1
  fcp_ms REAL,
  lcp_ms REAL,
  cls REAL,
  tbt_ms REAL,

  field_lcp_pctl REAL,
  field_cls_pctl REAL,
  field_inp_pctl REAL,

  psi_fetch_time TEXT
);

-- Notes (reuse if you already have this table)
CREATE TABLE IF NOT EXISTS seo_notes (
  note_id TEXT PRIMARY KEY,
  site_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  date TEXT NOT NULL,                  -- YYYY-MM-DD
  note_type TEXT NOT NULL,             -- "auto" | "manual" | "ai"
  category TEXT NOT NULL,              -- "speed" | "serp" | "links" | ...
  message TEXT NOT NULL,
  tags_json TEXT NOT NULL              -- JSON array
);

APPLY:

npx wrangler d1 migrations apply sitebuilder
