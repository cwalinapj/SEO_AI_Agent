CREATE TABLE IF NOT EXISTS residential_proxies (
  proxy_id TEXT PRIMARY KEY,
  label TEXT,
  proxy_url TEXT NOT NULL,
  country TEXT,
  region TEXT,
  metro_area TEXT,
  max_concurrent_leases INTEGER NOT NULL DEFAULT 1 CHECK (max_concurrent_leases >= 1),
  hourly_rate_usd REAL NOT NULL DEFAULT 0 CHECK (hourly_rate_usd >= 0),
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_residential_proxies_geo_status
  ON residential_proxies (status, country, region, metro_area, updated_at DESC);

CREATE TABLE IF NOT EXISTS proxy_leases (
  lease_id TEXT PRIMARY KEY,
  proxy_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  keyword TEXT,
  metro_area TEXT,
  status TEXT NOT NULL CHECK (status IN ('active', 'released', 'expired')),
  leased_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  released_at INTEGER,
  hourly_rate_usd REAL NOT NULL DEFAULT 0 CHECK (hourly_rate_usd >= 0),
  FOREIGN KEY (proxy_id) REFERENCES residential_proxies(proxy_id)
);

CREATE INDEX IF NOT EXISTS idx_proxy_leases_proxy_active
  ON proxy_leases (proxy_id, status, expires_at);

CREATE INDEX IF NOT EXISTS idx_proxy_leases_user_active
  ON proxy_leases (user_id, status, expires_at);

CREATE VIEW IF NOT EXISTS proxy_inventory_status AS
SELECT
  rp.proxy_id,
  rp.label,
  rp.country,
  rp.region,
  rp.metro_area,
  rp.status,
  rp.max_concurrent_leases,
  rp.hourly_rate_usd,
  COALESCE(
    SUM(
      CASE
        WHEN pl.status = 'active' AND pl.expires_at > (CAST(strftime('%s', 'now') AS INTEGER) * 1000)
          THEN 1
        ELSE 0
      END
    ),
    0
  ) AS active_leases,
  MAX(
    0,
    rp.max_concurrent_leases - COALESCE(
      SUM(
        CASE
          WHEN pl.status = 'active' AND pl.expires_at > (CAST(strftime('%s', 'now') AS INTEGER) * 1000)
            THEN 1
          ELSE 0
        END
      ),
      0
    )
  ) AS available_slots,
  rp.updated_at
FROM residential_proxies rp
LEFT JOIN proxy_leases pl ON pl.proxy_id = rp.proxy_id
GROUP BY
  rp.proxy_id,
  rp.label,
  rp.country,
  rp.region,
  rp.metro_area,
  rp.status,
  rp.max_concurrent_leases,
  rp.hourly_rate_usd,
  rp.updated_at;
