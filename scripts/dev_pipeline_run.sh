#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8787}"
SITE_URL="${SITE_URL:-https://example.local}"
WP_SITE_ID="${WP_SITE_ID:-wp-$(date +%s)}"
RUN_TYPE="${RUN_TYPE:-auto}"
GEO="${GEO:-US}"

request() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -sS -X "$method" "$BASE_URL$path" -H "content-type: application/json" --data "$body"
  else
    curl -sS -X "$method" "$BASE_URL$path"
  fi
}

json_get() {
  local key="$1"
  python3 -c 'import json,sys; key=sys.argv[1]; data=json.load(sys.stdin); cur=data
for part in key.split("."):
    cur = cur.get(part) if isinstance(cur, dict) else None
print("" if cur is None else cur)' "$key"
}

pretty() {
  python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin), indent=2, sort_keys=True))'
}

echo "[1/6] Upsert site"
UPSERT_PAYLOAD=$(cat <<JSON
{
  "site_url": "$SITE_URL",
  "wp_site_id": "$WP_SITE_ID",
  "plan": {"metro_proxy": true, "metro": "Los Angeles, CA"},
  "signals": {
    "site_name": "Dev Pipeline Demo",
    "detected_address": "123 Main St, Los Angeles, CA",
    "detected_phone": "+1-555-000-0000",
    "is_woocommerce": false,
    "top_pages": [
      {
        "url": "$SITE_URL/",
        "title": "Water Heater Repair Los Angeles",
        "h1": "Same-Day Water Heater Repair",
        "meta": "Emergency local service",
        "text_extract": "Same-day water heater repair and drain cleaning in Los Angeles."
      }
    ]
  }
}
JSON
)
UPSERT_JSON=$(request POST /v1/sites/upsert "$UPSERT_PAYLOAD")
printf '%s\n' "$UPSERT_JSON" | pretty
SITE_ID=$(printf '%s\n' "$UPSERT_JSON" | json_get site_id)
if [[ -z "$SITE_ID" ]]; then
  echo "Failed to extract site_id from upsert response" >&2
  exit 1
fi

echo "[2/6] Run keyword research"
KR_JSON=$(request POST "/v1/sites/$SITE_ID/keyword-research" '{}')
printf '%s\n' "$KR_JSON" | pretty

echo "[3/6] Run step2 daily harvest"
STEP2_JSON=$(request POST "/v1/sites/$SITE_ID/step2/run" "{\"run_type\":\"$RUN_TYPE\",\"max_keywords\":20,\"max_results\":20,\"geo\":\"$GEO\"}")
printf '%s\n' "$STEP2_JSON" | pretty

echo "[4/6] Generate step3 plan"
STEP3_JSON=$(request POST "/v1/sites/$SITE_ID/step3/plan" '{}')
printf '%s\n' "$STEP3_JSON" | pretty
RUN_ID=$(printf '%s\n' "$STEP3_JSON" | json_get summary.run_id)

echo "[5/6] Fetch board payload"
BOARD_JSON=$(request GET "/v1/sites/$SITE_ID/tasks/board")
printf '%s\n' "$BOARD_JSON" | pretty

echo "[6/6] Fetch filtered step3 tasks"
TASKS_PATH="/v1/sites/$SITE_ID/step3/tasks"
if [[ -n "$RUN_ID" ]]; then
  TASKS_PATH+="?site_run_id=$RUN_ID&status=ready"
fi
TASKS_JSON=$(request GET "$TASKS_PATH")
printf '%s\n' "$TASKS_JSON" | pretty

echo
echo "Pipeline complete"
echo "site_id=$SITE_ID"
if [[ -n "$RUN_ID" ]]; then
  echo "site_run_id=$RUN_ID"
fi
