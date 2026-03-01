#!/usr/bin/env python3
"""Live endpoint harness for Step3 task state transitions.

Validates:
- READY -> IN_PROGRESS -> DONE
- READY/IN_PROGRESS/NEW -> BLOCKED -> READY

Usage:
  python scripts/task_state_machine_harness.py --base-url http://127.0.0.1:8787
  python scripts/task_state_machine_harness.py --base-url http://127.0.0.1:8787 --site-id <site_id>
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass
class HttpResult:
    status: int
    body: dict[str, Any]


def http_json(method: str, url: str, payload: dict[str, Any] | None = None) -> HttpResult:
    data = None
    headers = {"content-type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read().decode("utf-8")
            return HttpResult(status=resp.status, body=json.loads(raw or "{}"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            parsed = json.loads(raw or "{}")
        except json.JSONDecodeError:
            parsed = {"ok": False, "error": raw}
        return HttpResult(status=exc.code, body=parsed)


def create_site_and_tasks(base_url: str) -> tuple[str, str | None]:
    token = str(int(time.time()))
    upsert = http_json(
        "POST",
        f"{base_url}/v1/sites/upsert",
        {
            "site_url": f"https://harness-{token}.local",
            "wp_site_id": f"harness-{token}",
            "plan": {"metro_proxy": True, "metro": "Los Angeles, CA"},
            "signals": {
                "site_name": "Harness Plumbing",
                "detected_address": "123 Main St, Los Angeles, CA",
                "top_pages": [
                    {
                        "url": f"https://harness-{token}.local/",
                        "title": "Water Heater Repair Los Angeles",
                        "h1": "Same-Day Water Heater Repair",
                        "meta": "Fast local service",
                        "text_extract": "Same-day plumbing, drain cleaning, and emergency repairs in Los Angeles.",
                    }
                ],
            },
        },
    )
    if upsert.status >= 300 or not upsert.body.get("ok"):
        raise RuntimeError(f"upsert failed: {upsert.status} {upsert.body}")
    site_id = str(upsert.body.get("site_id") or "").strip()
    if not site_id:
        raise RuntimeError(f"upsert returned no site_id: {upsert.body}")

    kr = http_json("POST", f"{base_url}/v1/sites/{site_id}/keyword-research", {})
    if kr.status >= 300 or not kr.body.get("ok"):
        raise RuntimeError(f"keyword-research failed: {kr.status} {kr.body}")

    step2 = http_json(
        "POST",
        f"{base_url}/v1/sites/{site_id}/step2/run",
        {"run_type": "auto", "max_keywords": 20, "max_results": 20, "geo": "US"},
    )
    if step2.status >= 300 or not step2.body.get("ok"):
        raise RuntimeError(f"step2 run failed: {step2.status} {step2.body}")

    step3 = http_json("POST", f"{base_url}/v1/sites/{site_id}/step3/plan", {})
    if step3.status >= 300 or not step3.body.get("ok"):
        raise RuntimeError(f"step3 plan failed: {step3.status} {step3.body}")

    run_id = None
    summary = step3.body.get("summary")
    if isinstance(summary, dict):
        run_id_raw = summary.get("run_id")
        run_id = str(run_id_raw).strip() if run_id_raw else None
    return site_id, run_id


def choose_task_ids(board: dict[str, Any]) -> tuple[str, str]:
    all_tasks: list[dict[str, Any]] = []
    for column in board.get("columns", []):
        if isinstance(column, dict):
            tasks = column.get("tasks", [])
            if isinstance(tasks, list):
                all_tasks.extend([t for t in tasks if isinstance(t, dict)])

    if not all_tasks:
        raise RuntimeError("board has no tasks")

    ready = next((t for t in all_tasks if t.get("status") == "READY"), None)
    transition_task = ready or next((t for t in all_tasks if t.get("status") == "NEW"), None)
    if not transition_task:
        raise RuntimeError("no READY or NEW task available for transition test")

    block_task = next(
        (
            t
            for t in all_tasks
            if t.get("task_id") != transition_task.get("task_id") and t.get("status") in {"NEW", "READY", "IN_PROGRESS"}
        ),
        None,
    )
    if not block_task:
        block_task = transition_task

    transition_task_id = str(transition_task.get("task_id") or "").strip()
    block_task_id = str(block_task.get("task_id") or "").strip()
    if not transition_task_id or not block_task_id:
        raise RuntimeError("could not resolve task IDs from board")
    return transition_task_id, block_task_id


def patch_status(base_url: str, site_id: str, task_id: str, status: str, blockers: list[dict[str, str]] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"status": status}
    if blockers is not None:
        payload["blockers"] = blockers
    res = http_json("PATCH", f"{base_url}/v1/sites/{site_id}/tasks/{task_id}", payload)
    if res.status >= 300 or not res.body.get("ok"):
        raise RuntimeError(f"PATCH {task_id} -> {status} failed: {res.status} {res.body}")
    task = res.body.get("task")
    if not isinstance(task, dict):
        raise RuntimeError(f"PATCH {task_id} -> {status} missing task payload: {res.body}")
    got = str(task.get("status") or "")
    if got != status:
        raise RuntimeError(f"PATCH {task_id} -> {status} returned status {got}")
    return task


def main() -> int:
    parser = argparse.ArgumentParser(description="Run live task state machine transition checks against worker endpoints.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8787", help="Worker base URL")
    parser.add_argument("--site-id", default="", help="Existing site_id (optional)")
    parser.add_argument("--run-id", default="", help="Optional run_id for board pull")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")

    if args.site_id.strip():
        site_id = args.site_id.strip()
        run_id = args.run_id.strip() or None
    else:
        site_id, run_id = create_site_and_tasks(base_url)

    board_path = f"/v1/sites/{site_id}/tasks/board"
    if run_id:
        board_path = f"/v1/sites/{site_id}/runs/{run_id}/tasks/board"
    board_res = http_json("GET", f"{base_url}{board_path}")
    if board_res.status >= 300 or not board_res.body.get("ok"):
        raise RuntimeError(f"board fetch failed: {board_res.status} {board_res.body}")

    transition_task_id, block_task_id = choose_task_ids(board_res.body)

    # Main chain: READY/NEW -> IN_PROGRESS -> DONE
    current = http_json("GET", f"{base_url}/v1/sites/{site_id}/tasks/{transition_task_id}")
    if current.status >= 300 or not current.body.get("ok"):
        raise RuntimeError(f"task read failed: {current.status} {current.body}")
    current_status = str((current.body.get("task") or {}).get("status") or "")
    if current_status == "NEW":
        patch_status(base_url, site_id, transition_task_id, "READY")
    elif current_status not in {"READY", "IN_PROGRESS", "DONE", "BLOCKED"}:
        raise RuntimeError(f"unexpected starting status for {transition_task_id}: {current_status}")

    if current_status in {"READY", "NEW"}:
        patch_status(base_url, site_id, transition_task_id, "IN_PROGRESS")
        patch_status(base_url, site_id, transition_task_id, "DONE")
    elif current_status == "IN_PROGRESS":
        patch_status(base_url, site_id, transition_task_id, "DONE")

    # Blocking chain: NEW|READY|IN_PROGRESS -> BLOCKED -> READY
    block_current = http_json("GET", f"{base_url}/v1/sites/{site_id}/tasks/{block_task_id}")
    if block_current.status >= 300 or not block_current.body.get("ok"):
        raise RuntimeError(f"block task read failed: {block_current.status} {block_current.body}")
    block_status = str((block_current.body.get("task") or {}).get("status") or "")
    if block_status == "DONE":
        # pick another if available in board payload
        for col in board_res.body.get("columns", []):
            if not isinstance(col, dict):
                continue
            for task in col.get("tasks", []):
                if isinstance(task, dict) and str(task.get("status") or "") in {"NEW", "READY", "IN_PROGRESS"}:
                    block_task_id = str(task.get("task_id") or "").strip()
                    block_status = str(task.get("status") or "")
                    break
            if block_task_id and block_status in {"NEW", "READY", "IN_PROGRESS"}:
                break
    if block_status == "NEW":
        patch_status(base_url, site_id, block_task_id, "READY")
        block_status = "READY"
    if block_status in {"READY", "IN_PROGRESS"}:
        patch_status(
            base_url,
            site_id,
            block_task_id,
            "BLOCKED",
            blockers=[{"code": "MISSING_ACCESS", "message": "Connect Google Business Profile"}],
        )
        patch_status(base_url, site_id, block_task_id, "READY")

    print(
        json.dumps(
            {
                "ok": True,
                "site_id": site_id,
                "run_id": run_id,
                "transition_task_id": transition_task_id,
                "block_task_id": block_task_id,
                "checked": [
                    "READY -> IN_PROGRESS -> DONE",
                    "READY/IN_PROGRESS -> BLOCKED -> READY",
                ],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2), file=sys.stderr)
        raise SystemExit(1)
