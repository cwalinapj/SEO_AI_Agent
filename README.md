## SEO_AI_Agent

### Database migrations include support for

#### SERP sampling & persistence
- **`POST /serp/sample`** persistence:
  - Tables: `serp_runs`, `serp_results`
  - Fallback tracking: `mode`, `fallback_reason`

#### Onboarding link ranking
- **`POST /onboarding/next-links`** ranking inputs via view: `onboarding_next_links`
  - Rank weight from SERP position
  - `kw_score = normalize(volume) × normalize(cpc) × normalize(rank_weight)` when metrics are available
  - Rank-only fallback when CPC/volume are missing
  - Domain diversity: one result per domain

#### Style preference capture
- **`POST /event`** stores style preferences in `user_events` (`like` / `dislike`) for downstream semantic-memory updates

#### Default next-question template
- The default next-question template offers:
  - Font
  - Palette
  - Layout
  - “Show me different styles”

---

### Production PSI monitoring schema

- **`POST /deploy/notify`** updates `sites.last_deploy_hash`
- **`POST /speed/check`** writes `speed_snapshots` with triggers:
  - `deploy` / `manual` / `failsafe`
- 12-hour per-site + strategy cooldown
- Delta view: `speed_snapshot_deltas`
  - LCP/CLS/TBT/field regressions
  - Severity: `warn` / `critical`

### Headless Google top-20 SERP capture

Worker endpoints:

- `POST /serp/google/top20`
- `POST /serp/sample` (alias)
- `GET /serp/results?serp_id=...`
- `GET /serp/graph?keyword=...&days=30`
- `GET /serp/domain-averages?keyword=...&keyword=...`
- `POST /serp/seo-suggestion`
- `POST /serp/watchlist/save`
- `GET /serp/watchlist?user_id=...`
- `POST /serp/watchlist/remove`
- `POST /serp/watchlist/run`
- `GET /jobs/status?job_id=...`
- `GET /jobs/artifacts?job_id=...`

`POST /serp/google/top20` request:

```json
{
  "user_id": "user-123",
  "keyword": "best plumber in san jose",
  "region": { "country": "US", "language": "en" },
  "device": "desktop",
  "max_results": 20
}
```

Response returns up to 20 rows with columns:

- `rank`
- `url`
- `domain`
- `title`
- `snippet`

Response also includes:

- `serp_url` (the Google query URL used for this run)
- optional `proxy_lease_id` + `proxy_geo` when a rented metro proxy is attached
- `job_id` (durable job status/evidence ID)

Rows are persisted in D1 tables:

- `serp_runs` (run metadata)
- `serp_results` (one row per result)

`POST /serp/google/top20` supports optional proxy attachment:

```json
{
  "user_id": "user-123",
  "keyword": "best plumber in san jose",
  "region": { "country": "US", "language": "en" },
  "proxy_lease_id": "lease_abc123"
}
```

Daily rank graph endpoint:

`GET /serp/graph?keyword=Plumbers%20in%20Minden,%20NV&days=30`

Response includes:

- `day_axis` (x-axis days)
- `graph[]` rows where each row has:
  - `url`
  - `root_domain`
  - `ranks` (y-axis positions, aligned to `day_axis`)

Cross-keyword root-domain averages:

`GET /serp/domain-averages?keyword=plumber%20near%20me&keyword=Plumbers%20in%20Minden,%20NV&require_all_keywords=1`

Returns root domains sorted by best average rank (lower is better), including per-keyword best rank.

Title/meta suggestion from selected competitor domains:

`POST /serp/seo-suggestion`

```json
{
  "target_keyword": "plumbers in minden, nv",
  "keywords": ["plumber near me", "Plumbers in Minden, NV"],
  "domains": ["callbighorn.com", "hoffmanplumbing.com", "rotorooter.com"],
  "max_points": 6
}
```

Response includes:

- `suggestion.title`
- `suggestion.meta_description`
- `points_used` (the exact SERP data points merged to produce the copy)

Daily SERP tracking for graph updates:

- Save tracked keywords with `POST /serp/watchlist/save`
- Worker cron runs daily (`0 9 * * *` UTC) and captures top-20 snapshots for active watchlist rows
- Trend charts are then read from `GET /serp/graph` using those stored daily snapshots

Watchlist save payload:

```json
{
  "user_id": "user-123",
  "keyword": "plumbers in minden, nv",
  "region": { "country": "US", "language": "en" },
  "device": "desktop",
  "max_results": 20,
  "active": true
}
```

Run watchlist immediately (without waiting for cron):

```json
{
  "user_id": "user-123",
  "force": true
}
```

### Explicit jobs + evidence model

New schema:

- `jobs`: durable status/cost/error tracking for unit work (`serp_top20`, `speed_check`, `proxy_lease`)
- `artifacts`: proof/evidence rows linked to `job_id` (checksum + optional `r2_key` + payload snapshot)

Behavior now:

- `POST /serp/google/top20` creates job + artifacts (parsed rows, raw checksum)
- `POST /speed/check` creates job + artifacts (PSI raw checksum + metrics/delta)
- `POST /proxy/lease` creates job + artifacts (lease receipt/error)
- `POST /serp/watchlist/run` creates per-keyword jobs for each attempted SERP run

Inspect evidence:

- `GET /jobs/status?job_id=job_...`
- `GET /jobs/artifacts?job_id=job_...`

SERP canonicalization + drift metadata (`serp_runs`) now includes:

- `keyword_norm`
- `region_key`
- `device_key`
- `serp_key` (hash of normalized key + UTC day)
- `parser_version`
- `raw_payload_sha256`
- `extractor_mode`

### Residential proxy pool (metro SERPs)

Endpoints:

- `POST /proxy/admin/register` (admin-only; register/update a residential proxy)
- `GET /proxy/admin/inventory` (admin-only; includes active lease counts)
- `GET /proxy/availability?country=us&region=ca&metro_area=san%20jose`
- `POST /proxy/lease` (rent a proxy temporarily for a user/keyword/metro)
- `GET /proxy/lease?lease_id=...&user_id=...` (fetch active lease details)
- `POST /proxy/release` (release active lease)

Admin routes require header:

- `x-proxy-control-secret: <PROXY_CONTROL_SECRET>`

Lease payload example:

```json
{
  "user_id": "user-123",
  "country": "us",
  "region": "ca",
  "metro_area": "san jose",
  "keyword": "best plumber in san jose",
  "duration_minutes": 30
}
```

#### Required secrets
- `PAGESPEED_API_KEY`
- `APIFY_TOKEN`

#### Optional WordPress plugin secret
- `WP_PLUGIN_SHARED_SECRET`

When set, the worker supports signed WordPress plugin SEO profile endpoints:

- `POST /plugin/wp/schema/save` (store schema status/profile/JSON-LD)
- `POST /plugin/wp/schema/profile` (fetch stored schema profile)
- `POST /plugin/wp/redirects/save` (store audited broken-link redirect paths)
- `POST /plugin/wp/redirects/profile` (fetch redirect profile)

Request signing requirements:

- `X-Plugin-Timestamp`
- `X-Plugin-Signature` HMAC-SHA256 over `${timestamp}.${rawBody}`
- 5-minute replay window enforcement

```bash
npx wrangler secret put PAGESPEED_API_KEY
npx wrangler secret put APIFY_TOKEN
npx wrangler secret put WP_PLUGIN_SHARED_SECRET
npx wrangler secret put PROXY_CONTROL_SECRET
```

Optional var in `wrangler.toml`:

- `APIFY_GOOGLE_ACTOR` (default: `apify/google-search-scraper`)
