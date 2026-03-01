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
