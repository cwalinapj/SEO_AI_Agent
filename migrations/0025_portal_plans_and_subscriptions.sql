PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS billing_site_subscriptions (
  subscription_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
  site_id TEXT REFERENCES wp_ai_seo_sites(site_id) ON DELETE SET NULL,
  plan_key TEXT NOT NULL CHECK (plan_key IN ('starter', 'growth', 'agency')),
  company_name TEXT,
  contact_email TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'portal',
  status TEXT NOT NULL DEFAULT 'lead' CHECK (status IN ('lead', 'active', 'paused', 'canceled')),
  notes_json TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_billing_subscriptions_site_time
  ON billing_site_subscriptions(site_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_billing_subscriptions_plan_time
  ON billing_site_subscriptions(plan_key, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_billing_subscriptions_email_time
  ON billing_site_subscriptions(contact_email, created_at DESC);
