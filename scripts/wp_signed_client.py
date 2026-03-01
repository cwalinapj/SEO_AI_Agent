#!/usr/bin/env python3
import argparse
import hashlib
import hmac
import json
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def compute_signature(secret: str, timestamp_ms: int, raw_body: str) -> str:
    message = f"{timestamp_ms}.{raw_body}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def load_payload(payload_arg: str) -> dict:
    if payload_arg.startswith("@"):
        payload_path = Path(payload_arg[1:])
        return json.loads(payload_path.read_text())
    return json.loads(payload_arg)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Call signed WordPress plugin endpoints with x-plugin-timestamp/x-plugin-signature headers."
    )
    parser.add_argument("--base-url", required=True, help="Worker base URL, e.g. https://your-worker.workers.dev")
    parser.add_argument("--endpoint", required=True, help="Path like /plugin/wp/v1/sites/upsert")
    parser.add_argument("--secret", required=True, help="Shared secret (WP_PLUGIN_SHARED_SECRET)")
    parser.add_argument(
        "--payload",
        required=True,
        help="JSON string payload or @/absolute/path/to/payload.json",
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    args = parser.parse_args()

    payload = load_payload(args.payload)
    raw_body = json.dumps(payload, separators=(",", ":"), sort_keys=False)
    timestamp_ms = int(time.time() * 1000)
    signature = compute_signature(args.secret, timestamp_ms, raw_body)

    url = f"{args.base_url.rstrip('/')}{args.endpoint}"
    req = Request(url=url, method="POST", data=raw_body.encode("utf-8"))
    req.add_header("content-type", "application/json")
    req.add_header("x-plugin-timestamp", str(timestamp_ms))
    req.add_header("x-plugin-signature", signature)

    try:
      with urlopen(req, timeout=args.timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        print(f"HTTP {resp.status}")
        print(body)
        return 0
    except HTTPError as err:
      body = err.read().decode("utf-8", errors="replace")
      print(f"HTTP {err.code}", file=sys.stderr)
      print(body, file=sys.stderr)
      return 1
    except URLError as err:
      print(f"Request failed: {err}", file=sys.stderr)
      return 2


if __name__ == "__main__":
    raise SystemExit(main())
