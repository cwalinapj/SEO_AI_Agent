#!/usr/bin/env python3
"""Backfill legacy step3_tasks rows into canonical tasks table.

Usage:
  python scripts/backfill_step3_tasks_to_tasks.py --db ./local.sqlite
  python scripts/backfill_step3_tasks_to_tasks.py --db ./local.sqlite --dry-run
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _clean(value: Any, max_len: int = 4000) -> str:
    if value is None:
        return ""
    out = str(value).strip()
    return out[:max_len]


def _iso_from_epoch_seconds(value: Any) -> str:
    try:
        ts = int(value)
    except (TypeError, ValueError):
        ts = int(datetime.now(tz=timezone.utc).timestamp())
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_json_loads(raw: str) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _map_mode(raw: str) -> str:
    value = _clean(raw, 40).lower()
    if value in {"auto", "auto_safe"}:
        return "AUTO"
    if value in {"diy", "assisted"}:
        return "DIY"
    if value in {"team", "team_only"}:
        return "TEAM"
    return "AUTO"


def _map_priority(raw: Any) -> str:
    if isinstance(raw, str):
        value = _clean(raw, 8).upper()
        if value in {"P0", "P1", "P2", "P3"}:
            return value
        try:
            raw = int(value)
        except ValueError:
            return "P2"
    try:
        num = int(raw)
    except (TypeError, ValueError):
        return "P2"
    if num <= 1:
        return "P0"
    if num == 2:
        return "P1"
    if num == 3:
        return "P2"
    return "P3"


def _map_status(raw: str) -> str:
    value = _clean(raw, 40).upper()
    if value in {"NEW", "READY", "BLOCKED", "IN_PROGRESS", "DONE", "SKIPPED", "FAILED"}:
        return value
    mapped = _clean(raw, 40).lower()
    if mapped == "planned":
        return "READY"
    if mapped == "applied":
        return "DONE"
    if mapped == "draft":
        return "IN_PROGRESS"
    if mapped == "blocked":
        return "BLOCKED"
    return "NEW"


def _map_category(raw: str) -> str:
    value = _clean(raw, 80).upper()
    if value in {
        "ON_PAGE",
        "TECHNICAL_SEO",
        "LOCAL_SEO",
        "CONTENT",
        "AUTHORITY",
        "SOCIAL",
        "MEASUREMENT",
    }:
        return value
    lowered = _clean(raw, 80).lower()
    mapping = {
        "on_site": "ON_PAGE",
        "local_ops": "LOCAL_SEO",
        "authority": "AUTHORITY",
        "social": "SOCIAL",
        "technical": "TECHNICAL_SEO",
        "content": "CONTENT",
        "measurement": "MEASUREMENT",
    }
    return mapping.get(lowered, "ON_PAGE")


@dataclass
class BackfillResult:
    scanned: int = 0
    inserted_or_updated: int = 0
    skipped_invalid: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "scanned": self.scanned,
            "inserted_or_updated": self.inserted_or_updated,
            "skipped_invalid": self.skipped_invalid,
        }


def _build_task_v1(row: sqlite3.Row) -> dict[str, Any]:
    details = _safe_json_loads(_clean(row["details_json"], 128000))

    task_id = _clean(details.get("task_id") or row["task_id"], 120)
    created_iso = _clean(details.get("created_at"), 80) or _iso_from_epoch_seconds(row["created_at"])
    updated_iso = _clean(details.get("updated_at"), 80) or created_iso

    scope_obj = details.get("scope") if isinstance(details.get("scope"), dict) else {}
    keyword = _clean(scope_obj.get("keyword"), 300) or None
    cluster = _clean(scope_obj.get("cluster"), 120) or None
    target_slug = _clean(scope_obj.get("target_slug") or row["target_slug"], 300) or None
    target_url = _clean(scope_obj.get("target_url") or row["target_url"], 2000) or None
    geo = _clean(scope_obj.get("geo"), 120) or None

    mode = _map_mode(_clean(details.get("mode") or row["execution_mode"], 40))
    status = _map_status(_clean(details.get("status") or row["status"], 40))
    priority = _map_priority(details.get("priority") or row["priority"])
    category = _map_category(_clean(details.get("category") or row["task_group"], 80))
    task_type = _clean(details.get("type") or row["task_type"], 120).upper() or "CONTENT_REFRESH"
    title = _clean(details.get("title") or row["title"], 400) or "Untitled task"
    summary = _clean(details.get("summary") or row["why_text"], 2000)

    requires = details.get("requires") if isinstance(details.get("requires"), dict) else {}
    access = requires.get("access") if isinstance(requires.get("access"), list) else []
    inputs = requires.get("inputs") if isinstance(requires.get("inputs"), list) else []
    approvals = requires.get("approvals") if isinstance(requires.get("approvals"), list) else []

    blockers = details.get("blockers") if isinstance(details.get("blockers"), list) else []
    if status == "BLOCKED" and len(blockers) == 0:
        blockers = [{"code": "MISSING_INPUT", "message": "Legacy blocked task imported without explicit blocker details."}]

    task = {
        "schema_version": "task.v1",
        "task_id": task_id,
        "site_id": _clean(details.get("site_id") or row["site_id"], 120),
        "site_run_id": _clean(details.get("site_run_id") or row["run_id"], 120) or None,
        "created_at": created_iso,
        "updated_at": updated_iso,
        "category": category,
        "type": task_type,
        "title": title,
        "summary": summary,
        "priority": priority,
        "effort": _clean(details.get("effort"), 1).upper() or "M",
        "confidence": float(details.get("confidence") or 0.7),
        "estimated_impact": details.get("estimated_impact")
        if isinstance(details.get("estimated_impact"), dict)
        else {"seo": "med", "leads": "med", "time_to_effect_days": 14},
        "mode": mode,
        "requires": {
            "access": access or ["NONE"],
            "inputs": inputs or ["NONE"],
            "approvals": approvals or ["NONE"],
        },
        "status": status,
        "blockers": blockers,
        "scope": {
            "keyword_id": _clean(scope_obj.get("keyword_id"), 120) or None,
            "keyword": keyword,
            "cluster": cluster,
            "target_url": target_url,
            "target_slug": target_slug,
            "geo": geo,
        },
        "evidence": details.get("evidence")
        if isinstance(details.get("evidence"), dict)
        else {"based_on": ["BEST_PRACTICE"], "citations": []},
        "instructions": details.get("instructions")
        if isinstance(details.get("instructions"), dict)
        else {"steps": [], "acceptance_criteria": [], "guardrails": []},
        "automation": details.get("automation")
        if isinstance(details.get("automation"), dict)
        else {"can_auto_apply": mode == "AUTO", "auto_apply_default": False, "actions": []},
        "outputs": details.get("outputs")
        if isinstance(details.get("outputs"), dict)
        else {"artifacts": []},
        "dependencies": details.get("dependencies")
        if isinstance(details.get("dependencies"), dict)
        else {"depends_on_task_ids": [], "supersedes_task_ids": []},
    }
    return task


def backfill_step3_tasks_to_tasks(
    conn: sqlite3.Connection,
    *,
    site_id: str | None = None,
    run_id: str | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    with_events: bool = True,
) -> BackfillResult:
    conn.row_factory = sqlite3.Row
    clauses = ["1=1"]
    params: list[Any] = []
    if site_id:
        clauses.append("site_id = ?")
        params.append(site_id)
    if run_id:
        clauses.append("run_id = ?")
        params.append(run_id)
    query = f"""
      SELECT
        task_id, run_id, site_id, task_group, task_type, execution_mode, priority,
        title, why_text, details_json, target_slug, target_url, status, created_at
      FROM step3_tasks
      WHERE {' AND '.join(clauses)}
      ORDER BY created_at ASC
    """
    if limit and limit > 0:
        query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(query, params).fetchall()
    result = BackfillResult(scanned=len(rows))
    now_epoch = int(datetime.now(tz=timezone.utc).timestamp())
    valid_site_run_ids = {
        _clean(row[0], 120)
        for row in conn.execute("SELECT id FROM site_runs").fetchall()
    }

    for row in rows:
        task = _build_task_v1(row)
        task_id = _clean(task.get("task_id"), 120)
        if not task_id:
            result.skipped_invalid += 1
            continue

        blocker_codes = []
        for blocker in task.get("blockers", []):
            if isinstance(blocker, dict):
                code = _clean(blocker.get("code"), 60)
                if code:
                    blocker_codes.append(code)

        if not dry_run:
            canonical_site_run_id = _clean(task.get("site_run_id"), 120)
            if canonical_site_run_id and canonical_site_run_id not in valid_site_run_ids:
                canonical_site_run_id = ""
            conn.execute(
                """
                INSERT INTO tasks (
                  id, site_id, site_run_id, category, type, title, priority, mode, effort, status,
                  requires_access_json, blocker_codes_json, scope_json, task_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  site_id = excluded.site_id,
                  site_run_id = excluded.site_run_id,
                  category = excluded.category,
                  type = excluded.type,
                  title = excluded.title,
                  priority = excluded.priority,
                  mode = excluded.mode,
                  effort = excluded.effort,
                  status = excluded.status,
                  requires_access_json = excluded.requires_access_json,
                  blocker_codes_json = excluded.blocker_codes_json,
                  scope_json = excluded.scope_json,
                  task_json = excluded.task_json,
                  updated_at = excluded.updated_at
                """,
                (
                    task_id,
                    _clean(task["site_id"], 120),
                    canonical_site_run_id or None,
                    _clean(task["category"], 40),
                    _clean(task["type"], 120),
                    _clean(task["title"], 400),
                    _clean(task["priority"], 4),
                    _clean(task["mode"], 8),
                    _clean(task["effort"], 1),
                    _clean(task["status"], 20),
                    json.dumps(task["requires"]["access"], separators=(",", ":")),
                    json.dumps(blocker_codes, separators=(",", ":")),
                    json.dumps(task["scope"], separators=(",", ":")),
                    json.dumps(task, separators=(",", ":")),
                    int(row["created_at"]) if row["created_at"] is not None else now_epoch,
                    now_epoch,
                ),
            )
            if with_events:
                conn.execute(
                    """
                    INSERT INTO task_status_events (
                      id, task_id, site_id, event_type, from_status, to_status, actor, message, created_at
                    ) VALUES (
                      lower(hex(randomblob(16))), ?, ?, 'status_change', NULL, ?, 'system',
                      'Backfilled from step3_tasks into canonical tasks table.', ?
                    )
                    """,
                    (
                        task_id,
                        _clean(task["site_id"], 120),
                        _clean(task["status"], 20),
                        now_epoch,
                    ),
                )
        result.inserted_or_updated += 1

    if not dry_run:
        conn.commit()
    return result


def _validate_required_tables(conn: sqlite3.Connection) -> None:
    needed = {"step3_tasks", "tasks"}
    found = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('step3_tasks','tasks')"
        ).fetchall()
    }
    missing = sorted(needed - found)
    if missing:
        raise RuntimeError(f"Missing required table(s): {', '.join(missing)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill step3_tasks into canonical tasks table.")
    parser.add_argument("--db", required=True, help="SQLite DB path (e.g., local D1 export).")
    parser.add_argument("--site-id", default=None, help="Optional site filter.")
    parser.add_argument("--run-id", default=None, help="Optional run filter.")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit.")
    parser.add_argument("--dry-run", action="store_true", help="Do not write changes.")
    parser.add_argument("--no-events", action="store_true", help="Skip task_status_events insert.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        _validate_required_tables(conn)
        result = backfill_step3_tasks_to_tasks(
            conn,
            site_id=args.site_id,
            run_id=args.run_id,
            limit=args.limit,
            dry_run=args.dry_run,
            with_events=not args.no_events,
        )
        print(json.dumps({"ok": True, **result.to_dict(), "dry_run": args.dry_run}, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
