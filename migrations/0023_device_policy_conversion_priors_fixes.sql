PRAGMA foreign_keys = ON;

DROP VIEW IF EXISTS keyword_device_selection;
DROP VIEW IF EXISTS keyword_device_expected_value;
DROP VIEW IF EXISTS cohort_device_priors;

DROP TRIGGER IF EXISTS keywords_device_policy_ai;
DROP TRIGGER IF EXISTS keywords_device_policy_au;

-- Ensure table exists, then rebuild to enforce DEFAULT(updated_at).
CREATE TABLE IF NOT EXISTS kw_device_policy (
  kw_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL CHECK (mode IN ('mobile_only', 'desktop_only', 'both')),
  mobile_weight REAL NOT NULL CHECK (mobile_weight >= 0.0 AND mobile_weight <= 1.0),
  desktop_weight REAL NOT NULL CHECK (desktop_weight >= 0.0 AND desktop_weight <= 1.0),
  reason TEXT NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE TABLE kw_device_policy_new (
  kw_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL CHECK (mode IN ('mobile_only', 'desktop_only', 'both')),
  mobile_weight REAL NOT NULL CHECK (mobile_weight >= 0.0 AND mobile_weight <= 1.0),
  desktop_weight REAL NOT NULL CHECK (desktop_weight >= 0.0 AND desktop_weight <= 1.0),
  reason TEXT NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

INSERT INTO kw_device_policy_new (kw_id, mode, mobile_weight, desktop_weight, reason, updated_at)
SELECT
  kw_id,
  mode,
  mobile_weight,
  desktop_weight,
  reason,
  COALESCE(updated_at, CAST(strftime('%s','now') AS INTEGER))
FROM kw_device_policy;

DROP TABLE kw_device_policy;
ALTER TABLE kw_device_policy_new RENAME TO kw_device_policy;

CREATE INDEX IF NOT EXISTS idx_kw_device_policy_updated
  ON kw_device_policy(updated_at DESC);

-- Ensure table exists, then rebuild to enforce DEFAULT(created_at).
CREATE TABLE IF NOT EXISTS conversion_events (
  event_id TEXT PRIMARY KEY,
  vertical TEXT NOT NULL,
  geo_bucket TEXT NOT NULL,
  cta_type TEXT NOT NULL CHECK (cta_type IN ('call_now', 'book_now', 'get_quote')),
  device TEXT NOT NULL CHECK (device IN ('mobile', 'desktop')),
  intent_bucket TEXT NOT NULL CHECK (intent_bucket IN ('emergency', 'research', 'purchase')),
  event_type TEXT NOT NULL CHECK (event_type IN ('impression', 'click_to_call', 'booking_link_click', 'form_submit_success', 'request_quote_submission', 'chat_lead_captured')),
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE TABLE conversion_events_new (
  event_id TEXT PRIMARY KEY,
  vertical TEXT NOT NULL,
  geo_bucket TEXT NOT NULL,
  cta_type TEXT NOT NULL CHECK (cta_type IN ('call_now', 'book_now', 'get_quote')),
  device TEXT NOT NULL CHECK (device IN ('mobile', 'desktop')),
  intent_bucket TEXT NOT NULL CHECK (intent_bucket IN ('emergency', 'research', 'purchase')),
  event_type TEXT NOT NULL CHECK (event_type IN ('impression', 'click_to_call', 'booking_link_click', 'form_submit_success', 'request_quote_submission', 'chat_lead_captured')),
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

INSERT INTO conversion_events_new (
  event_id, vertical, geo_bucket, cta_type, device, intent_bucket, event_type, created_at
)
SELECT
  event_id,
  vertical,
  geo_bucket,
  cta_type,
  device,
  intent_bucket,
  event_type,
  COALESCE(created_at, CAST(strftime('%s','now') AS INTEGER))
FROM conversion_events;

DROP TABLE conversion_events;
ALTER TABLE conversion_events_new RENAME TO conversion_events;

CREATE INDEX IF NOT EXISTS idx_conversion_events_cohort
  ON conversion_events(vertical, geo_bucket, cta_type, device, intent_bucket, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_conversion_events_type_time
  ON conversion_events(event_type, created_at DESC);

CREATE TRIGGER IF NOT EXISTS keywords_device_policy_ai
AFTER INSERT ON keywords
BEGIN
  INSERT INTO kw_device_policy (kw_id, mode, mobile_weight, desktop_weight, reason, updated_at)
  VALUES (
    NEW.kw_id,
    CASE
      WHEN NEW.intent_bucket = 'research' AND NEW.cta_type = 'get_quote' AND NEW.price_point = 'high' THEN 'desktop_only'
      WHEN NEW.vertical = 'plumbing' AND NEW.service_model = 'emergency' AND NEW.cta_type = 'call_now' THEN 'both'
      WHEN NEW.service_model = 'emergency' OR NEW.cta_type = 'call_now' OR NEW.business_hours_profile = '24_7' THEN 'mobile_only'
      WHEN NEW.vertical IS NULL OR NEW.service_model IS NULL OR NEW.cta_type IS NULL
        OR NEW.intent_bucket IS NULL OR NEW.price_point IS NULL OR NEW.business_hours_profile IS NULL
      THEN 'both'
      ELSE 'both'
    END,
    CASE
      WHEN NEW.intent_bucket = 'research' AND NEW.cta_type = 'get_quote' AND NEW.price_point = 'high' THEN 0.0
      WHEN NEW.vertical = 'plumbing' AND NEW.service_model = 'emergency' AND NEW.cta_type = 'call_now' THEN 0.75
      WHEN NEW.service_model = 'emergency' OR NEW.cta_type = 'call_now' OR NEW.business_hours_profile = '24_7' THEN 1.0
      ELSE 0.7
    END,
    CASE
      WHEN NEW.intent_bucket = 'research' AND NEW.cta_type = 'get_quote' AND NEW.price_point = 'high' THEN 1.0
      WHEN NEW.vertical = 'plumbing' AND NEW.service_model = 'emergency' AND NEW.cta_type = 'call_now' THEN 0.25
      WHEN NEW.service_model = 'emergency' OR NEW.cta_type = 'call_now' OR NEW.business_hours_profile = '24_7' THEN 0.0
      ELSE 0.3
    END,
    CASE
      WHEN NEW.intent_bucket = 'research' AND NEW.cta_type = 'get_quote' AND NEW.price_point = 'high' THEN 'research+get_quote+high_price'
      WHEN NEW.vertical = 'plumbing' AND NEW.service_model = 'emergency' AND NEW.cta_type = 'call_now' THEN 'plumbing+emergency+call_now'
      WHEN NEW.service_model = 'emergency' OR NEW.cta_type = 'call_now' OR NEW.business_hours_profile = '24_7' THEN 'mobile_first_urgent_or_call'
      WHEN NEW.vertical IS NULL OR NEW.service_model IS NULL OR NEW.cta_type IS NULL
        OR NEW.intent_bucket IS NULL OR NEW.price_point IS NULL OR NEW.business_hours_profile IS NULL
      THEN 'unknown_safe_fallback'
      ELSE 'v1_mobile_first_default'
    END,
    strftime('%s', 'now')
  )
  ON CONFLICT(kw_id) DO UPDATE SET
    mode = excluded.mode,
    mobile_weight = excluded.mobile_weight,
    desktop_weight = excluded.desktop_weight,
    reason = excluded.reason,
    updated_at = excluded.updated_at;
END;

CREATE TRIGGER IF NOT EXISTS keywords_device_policy_au
AFTER UPDATE OF vertical, service_model, cta_type, business_hours_profile, price_point, intent_bucket ON keywords
BEGIN
  INSERT INTO kw_device_policy (kw_id, mode, mobile_weight, desktop_weight, reason, updated_at)
  VALUES (
    NEW.kw_id,
    CASE
      WHEN NEW.intent_bucket = 'research' AND NEW.cta_type = 'get_quote' AND NEW.price_point = 'high' THEN 'desktop_only'
      WHEN NEW.vertical = 'plumbing' AND NEW.service_model = 'emergency' AND NEW.cta_type = 'call_now' THEN 'both'
      WHEN NEW.service_model = 'emergency' OR NEW.cta_type = 'call_now' OR NEW.business_hours_profile = '24_7' THEN 'mobile_only'
      WHEN NEW.vertical IS NULL OR NEW.service_model IS NULL OR NEW.cta_type IS NULL
        OR NEW.intent_bucket IS NULL OR NEW.price_point IS NULL OR NEW.business_hours_profile IS NULL
      THEN 'both'
      ELSE 'both'
    END,
    CASE
      WHEN NEW.intent_bucket = 'research' AND NEW.cta_type = 'get_quote' AND NEW.price_point = 'high' THEN 0.0
      WHEN NEW.vertical = 'plumbing' AND NEW.service_model = 'emergency' AND NEW.cta_type = 'call_now' THEN 0.75
      WHEN NEW.service_model = 'emergency' OR NEW.cta_type = 'call_now' OR NEW.business_hours_profile = '24_7' THEN 1.0
      ELSE 0.7
    END,
    CASE
      WHEN NEW.intent_bucket = 'research' AND NEW.cta_type = 'get_quote' AND NEW.price_point = 'high' THEN 1.0
      WHEN NEW.vertical = 'plumbing' AND NEW.service_model = 'emergency' AND NEW.cta_type = 'call_now' THEN 0.25
      WHEN NEW.service_model = 'emergency' OR NEW.cta_type = 'call_now' OR NEW.business_hours_profile = '24_7' THEN 0.0
      ELSE 0.3
    END,
    CASE
      WHEN NEW.intent_bucket = 'research' AND NEW.cta_type = 'get_quote' AND NEW.price_point = 'high' THEN 'research+get_quote+high_price'
      WHEN NEW.vertical = 'plumbing' AND NEW.service_model = 'emergency' AND NEW.cta_type = 'call_now' THEN 'plumbing+emergency+call_now'
      WHEN NEW.service_model = 'emergency' OR NEW.cta_type = 'call_now' OR NEW.business_hours_profile = '24_7' THEN 'mobile_first_urgent_or_call'
      WHEN NEW.vertical IS NULL OR NEW.service_model IS NULL OR NEW.cta_type IS NULL
        OR NEW.intent_bucket IS NULL OR NEW.price_point IS NULL OR NEW.business_hours_profile IS NULL
      THEN 'unknown_safe_fallback'
      ELSE 'v1_mobile_first_default'
    END,
    strftime('%s', 'now')
  )
  ON CONFLICT(kw_id) DO UPDATE SET
    mode = excluded.mode,
    mobile_weight = excluded.mobile_weight,
    desktop_weight = excluded.desktop_weight,
    reason = excluded.reason,
    updated_at = excluded.updated_at;
END;

CREATE VIEW IF NOT EXISTS cohort_device_priors AS
SELECT
  (lower(coalesce(vertical, 'unknown')) || '|' ||
   lower(coalesce(geo_bucket, 'unknown')) || '|' ||
   lower(coalesce(cta_type, 'unknown')) || '|' ||
   lower(coalesce(device, 'unknown')) || '|' ||
   lower(coalesce(intent_bucket, 'unknown'))) AS cohort_key,
  vertical,
  geo_bucket,
  cta_type,
  device,
  intent_bucket,
  SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) AS impressions,
  SUM(CASE WHEN event_type IN ('click_to_call', 'booking_link_click', 'form_submit_success', 'request_quote_submission', 'chat_lead_captured') THEN 1 ELSE 0 END) AS conversions,
  CASE
    WHEN SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) > 0
      THEN 1.0 * SUM(CASE WHEN event_type IN ('click_to_call', 'booking_link_click', 'form_submit_success', 'request_quote_submission', 'chat_lead_captured') THEN 1 ELSE 0 END)
           / SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END)
    ELSE 0.0
  END AS conversion_rate,
  min(1.0, 1.0 * SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) / 50.0) AS confidence,
  MAX(created_at) AS last_updated
FROM conversion_events
GROUP BY vertical, geo_bucket, cta_type, device, intent_bucket;

CREATE VIEW IF NOT EXISTS keyword_device_expected_value AS
WITH base AS (
  SELECT
    k.kw_id,
    k.phrase,
    k.vertical,
    k.cta_type,
    k.intent_bucket,
    (1.0 + COALESCE(km.monthly_volume, 0) / 1000.0 + COALESCE(km.avg_cpc_micros, 0) / 1000000.0) AS kw_score,
    LOWER(TRIM(COALESCE(k.region_json, 'unknown'))) AS region_bucket
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
 AND cdp.cta_type = b.cta_type
 AND cdp.intent_bucket = b.intent_bucket
 AND cdp.device = d.device
 AND cdp.geo_bucket = b.region_bucket;

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
