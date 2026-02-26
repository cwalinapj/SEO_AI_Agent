ALTER TABLE serp_runs ADD COLUMN keyword_norm TEXT;
ALTER TABLE serp_runs ADD COLUMN region_key TEXT;
ALTER TABLE serp_runs ADD COLUMN device_key TEXT;
ALTER TABLE serp_runs ADD COLUMN serp_key TEXT;
ALTER TABLE serp_runs ADD COLUMN parser_version TEXT;
ALTER TABLE serp_runs ADD COLUMN raw_payload_sha256 TEXT;
ALTER TABLE serp_runs ADD COLUMN extractor_mode TEXT;

CREATE INDEX IF NOT EXISTS idx_serp_runs_serp_key ON serp_runs (serp_key);
CREATE INDEX IF NOT EXISTS idx_serp_runs_phrase_region_device_created
  ON serp_runs (keyword_norm, region_key, device_key, created_at DESC);
