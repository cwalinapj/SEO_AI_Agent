PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS task_market_tasks (
  task_id TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'open', -- open|claimed|expired|closed
  platform TEXT NOT NULL,
  issuer_wallet TEXT NOT NULL,
  payout_amount TEXT NOT NULL,
  payout_token TEXT NOT NULL,
  payout_chain TEXT,
  deadline_at INTEGER NOT NULL,
  task_spec_json TEXT NOT NULL,
  task_spec_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_market_tasks_status_deadline
  ON task_market_tasks(status, deadline_at, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_task_market_tasks_platform
  ON task_market_tasks(platform, created_at DESC);

CREATE TABLE IF NOT EXISTS task_market_claims (
  claim_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES task_market_tasks(task_id) ON DELETE CASCADE,
  worker_wallet TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active', -- active|released|expired
  claimed_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  released_at INTEGER,
  release_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_task_market_claims_task_status
  ON task_market_claims(task_id, status, claimed_at DESC);
CREATE INDEX IF NOT EXISTS idx_task_market_claims_wallet_status
  ON task_market_claims(worker_wallet, status, claimed_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_task_market_claims_one_active_per_task
  ON task_market_claims(task_id)
  WHERE status = 'active';

CREATE TABLE IF NOT EXISTS task_market_evidence (
  evidence_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES task_market_tasks(task_id) ON DELETE CASCADE,
  claim_id TEXT NOT NULL REFERENCES task_market_claims(claim_id) ON DELETE CASCADE,
  worker_wallet TEXT NOT NULL,
  evidence_json TEXT NOT NULL,
  evidence_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_market_evidence_task_time
  ON task_market_evidence(task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_task_market_evidence_claim_time
  ON task_market_evidence(claim_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_task_market_evidence_hash
  ON task_market_evidence(evidence_hash);

CREATE TABLE IF NOT EXISTS task_market_verifications (
  verification_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES task_market_tasks(task_id) ON DELETE CASCADE,
  evidence_id TEXT NOT NULL REFERENCES task_market_evidence(evidence_id) ON DELETE CASCADE,
  passed INTEGER NOT NULL CHECK (passed IN (0,1)),
  result_json TEXT NOT NULL,
  verification_result_hash TEXT NOT NULL,
  server_signature TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_market_verifications_task_time
  ON task_market_verifications(task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_task_market_verifications_passed
  ON task_market_verifications(task_id, passed, created_at DESC);

CREATE TABLE IF NOT EXISTS task_market_payout_authorizations (
  authorization_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES task_market_tasks(task_id) ON DELETE CASCADE,
  claim_id TEXT NOT NULL REFERENCES task_market_claims(claim_id) ON DELETE CASCADE,
  worker_wallet TEXT NOT NULL,
  payout_payload_json TEXT NOT NULL,
  payout_payload_hash TEXT NOT NULL,
  server_signature TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'authorized', -- authorized|executed|expired|revoked
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_market_payout_auth_task_time
  ON task_market_payout_authorizations(task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_task_market_payout_auth_wallet
  ON task_market_payout_authorizations(worker_wallet, created_at DESC);

CREATE TABLE IF NOT EXISTS task_idempotency_keys (
  idempotency_key TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  response_status INTEGER NOT NULL,
  response_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (endpoint, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_task_idempotency_created
  ON task_idempotency_keys(created_at DESC);

CREATE TABLE IF NOT EXISTS task_audit_log (
  event_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES task_market_tasks(task_id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  actor_wallet TEXT,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_audit_log_task_time
  ON task_audit_log(task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_task_audit_log_event_type
  ON task_audit_log(event_type, created_at DESC);
