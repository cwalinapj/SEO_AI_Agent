## SEO_AI_Agent

### Cloudflare Agents + Bright Data MCP Worker

- A dedicated Agents SDK worker has been added at `cloudflare-agents-worker/`.
- Bright Data hosted MCP is wired in `cloudflare-agents-worker/src/server.ts` via `BRIGHT_DATA_API_TOKEN`.
- Setup guide: `cloudflare-agents-worker/README.md`

### Unified D1 Step2/Step3 migration

- Canonical D1 schema migration for site/run/keyword/page/link/task pipeline:
  - `migrations/0015_unified_d1_step2_step3.sql`
- Moz snapshot and budgeting migration:
  - `migrations/0017_moz_snapshots_and_budgeting.sql`
- Adds:
  - `site_signals`, `site_briefs`, `keyword_sets`, `keyword_metrics`
  - `site_runs`, `site_run_jobs`
  - `urls`, `serp_result_url_map`, `page_snapshots`, `page_diffs`
  - `backlink_url_metrics`, `backlink_domain_metrics`, `link_diffs`
  - `internal_graph_runs`, `internal_graph_url_stats`
  - `tasks`, `task_status_events`, `task_board_cache`
  - `competitor_sets`, `competitor_social_profiles`
  - `moz_url_metrics_snapshots`
  - `moz_anchor_text_snapshots`
  - `moz_linking_root_domains_snapshots`
  - `moz_link_intersect_snapshots`
  - `moz_usage_snapshots`, `moz_index_metadata_snapshots`

Backfill legacy Step 3 tasks into canonical `tasks` table:

```bash
./.venv/bin/python scripts/backfill_step3_tasks_to_tasks.py --db ./local.sqlite --dry-run
./.venv/bin/python scripts/backfill_step3_tasks_to_tasks.py --db ./local.sqlite
```

Optional filters:

- `--site-id <site_id>`
- `--run-id <run_id>`
- `--limit <n>`
- `--no-events` (skip `task_status_events` inserts)

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

- `POST /v1/sites/analyze`
- `POST /v1/sites/upsert`
- `POST /v1/sites/{id}/keyword-research`
- `GET /v1/jobs/{id}`
- `GET /v1/sites/{id}/keyword-research/results`
- `POST /v1/sites/{id}/step2/run`
- `POST /v1/sites/{id}/daily-run`
- `GET /v1/sites/{id}/step2/report?date=YYYY-MM-DD`
- `POST /v1/sites/{id}/step3/plan`
- `GET /v1/sites/{id}/step3/report?date=YYYY-MM-DD`
- `GET /v1/sites/{id}/step3/tasks?execution_mode=...&task_group=...&status=...`
- `GET /v1/sites/{id}/tasks/board`
- `GET /v1/sites/{id}/runs/{site_run_id}/tasks/board`
- `GET /v1/sites/{id}/tasks/{task_id}`
- `PATCH /v1/sites/{id}/tasks/{task_id}`
- `POST /v1/sites/{id}/tasks/bulk`
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
- `GET /v1/sites/{id}/moz/budget`
- `GET /v1/sites/{id}/providers`
- `POST /v1/sites/{id}/providers`
- `GET /v1/sites/{id}/moz/profile`
- `POST /v1/sites/{id}/moz/profile`
- `POST /v1/sites/{id}/moz/run`
- `POST /moz/url-metrics`
- `POST /moz/anchor-text`
- `POST /moz/linking-root-domains`
- `POST /moz/link-intersect`
- `POST /moz/usage-data`
- `POST /moz/index-metadata`
- `POST /page/fetch`
- `GET /jobs/status?job_id=...`
- `GET /jobs/artifacts?job_id=...`

Signed WP plugin endpoint namespace (`x-plugin-timestamp` + `x-plugin-signature`):

- `POST /plugin/wp/v1/sites/upsert`
- `POST /plugin/wp/v1/sites/{id}/keyword-research`
- `POST /plugin/wp/v1/sites/{id}/step2/run`
- `POST /plugin/wp/v1/sites/{id}/step3/plan`
- `POST /plugin/wp/v1/sites/{id}/tasks/board`
- `POST /plugin/wp/v1/sites/{id}/step3/tasks`
- `POST /plugin/wp/v1/sites/{id}/tasks/{task_id}`
- `POST /plugin/wp/v1/sites/{id}/tasks/bulk`

API and integration docs:

- OpenAPI: `docs/openapi.yaml`
- WP integration guide: `docs/wp-plugin-integration-guide.md`
- Web Secrets Wallet marketplace contract: `docs/web-secrets-task-marketplace-contract.md`
- Signed WP client helper: `scripts/wp_signed_client.py`

Human-verified task marketplace endpoints:

- `POST /v1/tasks`
- `GET /v1/tasks/open`
- `GET /v1/tasks/{task_id}`
- `POST /v1/tasks/{task_id}/claim`
- `POST /v1/tasks/{task_id}/release`
- `POST /v1/tasks/{task_id}/evidence`
- `POST /v1/tasks/{task_id}/verify`
- `POST /v1/tasks/{task_id}/payout-authorize`

`POST /v1/sites/upsert` payload (WP plugin contract):

```json
{
  "site_url": "https://example.com",
  "wp_site_id": "abc123",
  "plan": { "metro_proxy": true, "metro": "Los Angeles, CA" },
  "signals": {
    "site_name": "Example Co",
    "detected_address": "123 Main St, Los Angeles, CA",
    "detected_phone": "+1-555-000-0000",
    "industry_hint": null,
    "is_woocommerce": true,
    "sitemap_urls": ["https://example.com/sitemap.xml"],
    "top_pages": [
      {
        "url": "https://example.com/water-heater-repair",
        "title": "Water Heater Repair",
        "h1": "Water Heater Repair",
        "meta": "Fast local service",
        "text_extract": "Same-day water heater repair in Los Angeles."
      }
    ]
  }
}
```

`POST /v1/sites/{id}/daily-run` optional body:

```json
{
  "run_type": "auto",
  "max_keywords": 20,
  "max_results": 20,
  "geo": "US"
}
```

`run_type` can be `auto` (default), `baseline`, or `delta`.

`POST /v1/sites/{id}/step3/plan` optional body:

```json
{
  "step2_date": "2026-02-28"
}
```

Step 3 output includes:

- Local-service execution tasks split by mode: `auto_safe`, `assisted`, `team_only`
- Competitor set derived from Step 2 top-5 SERP frequency
- Competitor-reactive social plan briefs (strategy signals only)
- Footprint/spam risk flags (doorway and repetitive-pattern guardrails)
- `task.v1` payload format with taxonomy categories:
  `ON_PAGE`, `TECHNICAL_SEO`, `LOCAL_SEO`, `CONTENT`, `AUTHORITY`, `SOCIAL`, `MEASUREMENT`
- `task_board.v1` payload for WP Kanban rendering:
  - summary counters (`by_status`, `by_priority`, `by_mode`, `by_category`)
  - Kanban columns (`NEW`, `READY`, `BLOCKED`, `IN_PROGRESS`, `DONE`)
  - filter sets (status/priority/mode/category/cluster/requires_access)
  - `task_details.by_id` embedded full task objects

Deterministic Step2 → Step3 triggers currently implemented:

- FAQ schema prevalence threshold (`top3 faq_schema_rate >= 0.5`) → `FAQ_SCHEMA_ADD`
- Internal-link gap (`target inbound < 50% of top3 median`) → `INTERNAL_LINKING_BOOST_MONEY_PAGE`
- New top-3 entrant + pricing-module prevalence → `CONTENT_MODULE_ADD`
- Directory-heavy SERP composition → `LOCAL_PARTNERSHIP_OPPORTUNITIES`
- Ref-domain gap vs top3 median → `OUTREACH_TARGET_LIST`
- Competitor social cadence increase vs prior baseline → `SOCIAL_PLAN_WEEKLY`

Task status transition endpoint:

`PATCH /v1/sites/{id}/tasks/{task_id}`

```json
{
  "status": "IN_PROGRESS"
}
```

Supported transitions:

- `READY -> IN_PROGRESS -> DONE`
- blocking/unblocking:
  - `READY|NEW|IN_PROGRESS -> BLOCKED`
  - `BLOCKED -> READY`

You may pass blockers when setting `BLOCKED`:

```json
{
  "status": "BLOCKED",
  "blockers": [
    { "code": "MISSING_ACCESS", "message": "Connect Google Business Profile" }
  ]
}
```

Bulk task actions:

`POST /v1/sites/{id}/tasks/bulk`

`AUTO_APPLY_READY` (all READY `AUTO` tasks in a run, marks as `DONE`):

```json
{
  "action": "AUTO_APPLY_READY",
  "site_run_id": "s3run_xxx"
}
```

`MARK_DONE_SELECTED` (requires task IDs):

```json
{
  "action": "MARK_DONE_SELECTED",
  "site_run_id": "s3run_xxx",
  "task_ids": ["task_1", "task_2"]
}
```

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

`POST /serp/watchlist/run` supports optional proxy lease and now reports graph-path readiness:

```json
{
  "user_id": "user-123",
  "force": true,
  "proxy_lease_id": "lease_abc123"
}
```

Summary fields include:

- `proxy_lease_id`
- `graph_ready_rows`
- per-row `graph_rows_last_30d`

Daily rank graph endpoint:

`GET /serp/graph?keyword=Plumbers%20in%20Minden,%20NV&days=30`

Response includes:

- `day_axis` (x-axis days)
- `graph[]` rows where each row has:
  - `url`
  - `root_domain`
  - `ranks` (y-axis positions, aligned to `day_axis`)

Local scripts:

- `scripts/dev_pipeline_run.sh` runs `upsert -> step1 -> step2 -> step3 -> board`.
- `scripts/task_state_machine_harness.py` validates task transitions (`READY -> IN_PROGRESS -> DONE`, `BLOCKED -> READY`).

Moz row-budget rules implemented (estimator endpoint):

- Daily delta baseline: top-5 URL metrics across tracked keywords + entrants/movers.
- Weekly refresh model: site-level `competitor_focus_urls` (default 20 URLs) for anchor/link-domain refresh and link-intersect.
- Profiles:
  - `single_site_max` (default): richer daily collection under a per-site monthly row budget (default `15000`).
  - `scalable_delta`: same guardrails, tuned for tighter weekly focus.
- Guardrails degrade automatically when projected rows exceed monthly remaining budget (`depth 20->10->5`, RD rows `50->25`, anchors `20->10`, then intersect skip).
- Monthly baseline model: `light=400 rows` or `fuller=1100 rows`.
- Cost model defaults to `$5 / 1,000 rows` and is configurable via query param.

Provider profile support (per site):

- `serp_provider`: `decodo_serp_api` or `headless_google`
- `page_provider`: `decodo_web_api` or `direct_fetch`
- `geo_provider`: `decodo_geo` or `proxy_lease_pool`
- Configure with `POST /v1/sites/{id}/providers`, inspect with `GET /v1/sites/{id}/providers`.
- Step2 and `/serp/google/top20` can use Decodo with automatic fallback to headless/direct modes.
- Every provider call is logged through `jobs` and `artifacts` with `extractor_mode` and `fallback_reason`.

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
