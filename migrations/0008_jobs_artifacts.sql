CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  user_id TEXT,
  site_id TEXT,
  type TEXT NOT NULL,
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 1,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  cost_units INTEGER NOT NULL DEFAULT 0,
  error_code TEXT,
  error_json TEXT,
  request_json TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  started_at INTEGER,
  finished_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_jobs_user_created_at
  ON jobs (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_jobs_site_created_at
  ON jobs (site_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_jobs_type_status
  ON jobs (type, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS artifacts (
  artifact_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  r2_key TEXT,
  checksum TEXT NOT NULL,
  payload_json TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);

CREATE INDEX IF NOT EXISTS idx_artifacts_job_created_at
  ON artifacts (job_id, created_at DESC);
