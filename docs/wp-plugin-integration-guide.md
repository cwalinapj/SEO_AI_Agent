# WP Plugin Integration Guide

This guide defines the signed request contract for WordPress plugin integration with the SEO AI Agent worker.

## Base URL

- Local: `http://127.0.0.1:8787`
- Prod: your deployed Cloudflare Worker URL

## Signed request contract

All `/plugin/wp/v1/*` endpoints require HMAC request signing.

Required headers:

- `x-plugin-timestamp`: epoch milliseconds
- `x-plugin-signature`: hex(HMAC_SHA256(secret, `${timestamp}.${rawBody}`))

CLI helper for signed calls:

- Script: `/Users/root1/loc-count/_repos/SEO_AI_Agent/scripts/wp_signed_client.py`
- Example:

```bash
python3 scripts/wp_signed_client.py \
  --base-url "https://<your-worker-domain>" \
  --endpoint "/plugin/wp/v1/sites/upsert" \
  --secret "<WP_PLUGIN_SHARED_SECRET>" \
  --payload @/Users/root1/loc-count/_repos/SEO_AI_Agent/docs/examples/plugin-site-upsert.json
```

Rules:

- Signature is calculated from the exact raw JSON body string.
- Requests fail if timestamp skew is outside the worker window.
- Server secret is `WP_PLUGIN_SHARED_SECRET`.

## Signed WP endpoints

## Site + pipeline

- `POST /plugin/wp/v1/sites/upsert`
- `POST /plugin/wp/v1/sites/{site_id}/keyword-research`
- `POST /plugin/wp/v1/sites/{site_id}/step2/run`
- `POST /plugin/wp/v1/sites/{site_id}/step3/plan`

## Tasks and board

- `POST /plugin/wp/v1/sites/{site_id}/tasks/board`
- `POST /plugin/wp/v1/sites/{site_id}/step3/tasks`
- `POST /plugin/wp/v1/sites/{site_id}/tasks/{task_id}`
- `POST /plugin/wp/v1/sites/{site_id}/tasks/bulk`

## Semantic memory

- `POST /plugin/wp/v1/sites/{site_id}/memory/upsert`
- `POST /plugin/wp/v1/sites/{site_id}/memory/search`

## Endpoint payloads

`POST /plugin/wp/v1/sites/upsert` (same payload as `/v1/sites/upsert`):

```json
{
  "site_url": "https://example.com",
  "wp_site_id": "wp_123",
  "plan": { "metro_proxy": true, "metro": "Los Angeles, CA" },
  "signals": {
    "site_name": "Example Co",
    "detected_address": "123 Main St, Los Angeles, CA",
    "top_pages": [
      {
        "url": "https://example.com/",
        "title": "Water Heater Repair Los Angeles",
        "h1": "Same-Day Water Heater Repair",
        "meta": "Fast local service",
        "text_extract": "Same-day plumbing service in Los Angeles."
      }
    ]
  }
}
```

`POST /plugin/wp/v1/sites/{site_id}/step2/run`:

```json
{
  "run_type": "auto",
  "max_keywords": 20,
  "max_results": 20,
  "geo": "US"
}
```

`POST /plugin/wp/v1/sites/{site_id}/tasks/board` (optional run pin):

```json
{
  "site_run_id": "optional_run_id"
}
```

`POST /plugin/wp/v1/sites/{site_id}/step3/tasks` (filters):

```json
{
  "site_run_id": "optional_run_id",
  "execution_mode": ["AUTO", "DIY"],
  "task_group": ["ON_PAGE", "LOCAL_SEO"],
  "status": ["READY", "BLOCKED"]
}
```

`POST /plugin/wp/v1/sites/{site_id}/tasks/{task_id}` (status transitions):

```json
{
  "status": "BLOCKED",
  "blockers": [
    { "code": "MISSING_ACCESS", "message": "Connect Google Business Profile" }
  ]
}
```

Allowed transitions:

- `READY -> IN_PROGRESS -> DONE`
- `NEW -> READY`
- `NEW|READY|IN_PROGRESS -> BLOCKED`
- `BLOCKED -> READY`

`POST /plugin/wp/v1/sites/{site_id}/tasks/bulk`:

```json
{
  "action": "AUTO_APPLY_READY",
  "site_run_id": "s3run_xxx"
}
```

`POST /plugin/wp/v1/sites/{site_id}/memory/upsert`:

```json
{
  "type": "serp_keyword_summary",
  "scope_key": "kw_water_heater_repair_los_angeles",
  "collected_day": "2026-03-01",
  "geo_key": "metro:los-angeles-ca",
  "title": "Water heater cluster summary",
  "text_summary": "Top 3 results favor service pages with pricing and FAQ schema. Median ref domains is 38.",
  "tags": {
    "cluster": "Water Heater Repair",
    "intent": "transactional",
    "vertical": "plumbing"
  },
  "raw_payload": {
    "top3_median_ref_domains": 38,
    "faq_schema_prevalence": 0.67
  }
}
```

`POST /plugin/wp/v1/sites/{site_id}/memory/search`:

```json
{
  "q": "local plumbing emergency keyword pattern with FAQ schema",
  "k": 8,
  "type": "serp_keyword_summary",
  "days": 60,
  "geo_key": "metro:los-angeles-ca"
}
```

## Operational checklist

1. Cloudflare config
- Set D1 binding `DB`.
- Set Vectorize binding `USER_MEM`.
- Set R2 binding `MEMORY_R2`.
- Set secrets: `WP_PLUGIN_SHARED_SECRET`, `OPENAI_API_KEY`.

2. Request signing implementation (plugin)
- Build raw JSON body string first.
- Set `x-plugin-timestamp` to epoch milliseconds.
- Compute `x-plugin-signature` with:
  - `hex(HMAC_SHA256(WP_PLUGIN_SHARED_SECRET, "${timestamp}.${rawBody}"))`
- Send the same raw body bytes used for signature generation.

3. Per-endpoint signed examples
- Upsert site:
```bash
python3 scripts/wp_signed_client.py --base-url "https://<worker>" --endpoint "/plugin/wp/v1/sites/upsert" --secret "<WP_PLUGIN_SHARED_SECRET>" --payload @docs/examples/plugin-site-upsert.json
```
- Step2 run:
```bash
python3 scripts/wp_signed_client.py --base-url "https://<worker>" --endpoint "/plugin/wp/v1/sites/<site_id>/step2/run" --secret "<WP_PLUGIN_SHARED_SECRET>" --payload '{"run_type":"auto","max_keywords":20,"max_results":20,"geo":"US"}'
```
- Step3 plan:
```bash
python3 scripts/wp_signed_client.py --base-url "https://<worker>" --endpoint "/plugin/wp/v1/sites/<site_id>/step3/plan" --secret "<WP_PLUGIN_SHARED_SECRET>" --payload '{}'
```
- Task board:
```bash
python3 scripts/wp_signed_client.py --base-url "https://<worker>" --endpoint "/plugin/wp/v1/sites/<site_id>/tasks/board" --secret "<WP_PLUGIN_SHARED_SECRET>" --payload '{}'
```
- Filtered tasks:
```bash
python3 scripts/wp_signed_client.py --base-url "https://<worker>" --endpoint "/plugin/wp/v1/sites/<site_id>/step3/tasks" --secret "<WP_PLUGIN_SHARED_SECRET>" --payload '{"status":["READY"],"execution_mode":["AUTO"]}'
```
- Task transition:
```bash
python3 scripts/wp_signed_client.py --base-url "https://<worker>" --endpoint "/plugin/wp/v1/sites/<site_id>/tasks/<task_id>" --secret "<WP_PLUGIN_SHARED_SECRET>" --payload '{"status":"IN_PROGRESS"}'
```
- Bulk task action:
```bash
python3 scripts/wp_signed_client.py --base-url "https://<worker>" --endpoint "/plugin/wp/v1/sites/<site_id>/tasks/bulk" --secret "<WP_PLUGIN_SHARED_SECRET>" --payload '{"action":"AUTO_APPLY_READY","site_run_id":"s3run_xxx"}'
```
- Memory upsert:
```bash
python3 scripts/wp_signed_client.py --base-url "https://<worker>" --endpoint "/plugin/wp/v1/sites/<site_id>/memory/upsert" --secret "<WP_PLUGIN_SHARED_SECRET>" --payload '{"type":"cluster_summary","scope_key":"water_heater","collected_day":"2026-03-01","geo_key":"us","text_summary":"Top 3 pattern summary","tags":{"cluster":"Water Heater"},"raw_payload":{"source":"step2"}}'
```
- Memory search:
```bash
python3 scripts/wp_signed_client.py --base-url "https://<worker>" --endpoint "/plugin/wp/v1/sites/<site_id>/memory/search" --secret "<WP_PLUGIN_SHARED_SECRET>" --payload '{"q":"similar local emergency plumbing patterns","k":5,"days":30}'
```

```json
{
  "action": "MARK_DONE_SELECTED",
  "site_run_id": "s3run_xxx",
  "task_ids": ["task_1", "task_2"]
}
```

## Watchlist + graph path notes

- `/serp/watchlist/run` supports `proxy_lease_id`.
- Watchlist run summary now reports per-row graph readiness (`graph_rows_last_30d`) and aggregate `graph_ready_rows`.
- This verifies watchlist writes are visible through the same read path that powers `/serp/graph`.

## Local dev validation scripts

- Full pipeline: `scripts/dev_pipeline_run.sh`
- Task transition harness: `scripts/task_state_machine_harness.py`

Example:

```bash
BASE_URL=http://127.0.0.1:8787 ./scripts/dev_pipeline_run.sh
python scripts/task_state_machine_harness.py --base-url http://127.0.0.1:8787
```

## API reference

- OpenAPI file: [openapi.yaml](/Users/root1/loc-count/_repos/SEO_AI_Agent/docs/openapi.yaml)
