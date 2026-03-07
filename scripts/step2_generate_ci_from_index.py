#!/usr/bin/env python3
"""
step2_generate_ci_from_index.py

Step 2: Read the repo index database produced by step1 (file_index table),
extract risks, then generate:
  - CI_PLAN.md (human-readable plan)
  - .github/workflows/ci.yml (GitHub Actions workflow)
  - tests/ scaffolding files for "risk stress tests" (placeholders + harness)

This is intentionally conservative: it won't guess brittle tests; it will
create a structured scaffold and TODOs that a later "auto-fix + test" step can fill.

Usage:
  python3 scripts/step2_generate_ci_from_index.py --root . --db .repo_index.sqlite

Options:
  --ci-plan CI_PLAN.md
  --workflow .github/workflows/ci.yml
  --tests-dir tests
  --overwrite
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple


# ----------------------------
# Data models
# ----------------------------
@dataclass
class Risk:
    title: str
    severity: str  # low|med|high|critical
    test_idea: str
    notes: str | None
    file_path: str


# ----------------------------
# Helpers
# ----------------------------
def read_db_rows(conn: sqlite3.Connection) -> List[Tuple[str, str, str, str]]:
    """
    Returns list of (path, sha, summary_md, risks_json)
    """
    rows = conn.execute(
        "SELECT path, sha, summary_md, risks_json FROM file_index ORDER BY path ASC"
    ).fetchall()
    return [(r[0], r[1], r[2], r[3]) for r in rows]


def parse_risks(rows: List[Tuple[str, str, str, str]]) -> List[Risk]:
    out: List[Risk] = []
    for path, _sha, _summary, risks_json in rows:
        if not risks_json:
            continue
        try:
            risks = json.loads(risks_json)
        except json.JSONDecodeError:
            continue
        if not isinstance(risks, list):
            continue
        for r in risks:
            if not isinstance(r, dict):
                continue
            title = str(r.get("title", "")).strip()
            severity = str(r.get("severity", "med")).strip().lower()
            if severity not in {"low", "med", "high", "critical"}:
                severity = "med"
            test_idea = str(r.get("test_idea", "")).strip()
            notes = r.get("notes")
            notes_s = str(notes).strip() if isinstance(notes, str) and notes.strip() else None
            if title:
                out.append(Risk(title=title, severity=severity, test_idea=test_idea, notes=notes_s, file_path=path))
    return out


def detect_repo_stack(paths: List[str]) -> Dict[str, bool]:
    """
    Very simple heuristics based on files present in the indexed set.
    """
    s = set(paths)
    has_node = any(p.endswith("package.json") for p in s) or any(p.endswith("pnpm-lock.yaml") for p in s) or any(
        p.endswith("yarn.lock") for p in s
    )
    has_python = any(p in s for p in ("pyproject.toml", "requirements.txt", "setup.py")) or any(
        p.endswith(".py") for p in s
    )
    has_ts = any(p.endswith(".ts") or p.endswith(".tsx") for p in s)
    has_cf_worker = any("wrangler.toml" in p or p.endswith("wrangler.toml") for p in s)
    return {"node": has_node, "python": has_python, "typescript": has_ts, "cloudflare": has_cf_worker}


def group_risks(risks: List[Risk]) -> Dict[str, List[Risk]]:
    groups: Dict[str, List[Risk]] = {"critical": [], "high": [], "med": [], "low": []}
    for r in risks:
        groups[r.severity].append(r)
    return groups


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def safe_write(path: Path, content: str, overwrite: bool) -> None:
    ensure_parent(path)
    if path.exists() and not overwrite:
        raise RuntimeError(f"Refusing to overwrite existing file: {path} (use --overwrite)")
    path.write_text(content, encoding="utf-8")


def severity_sort_key(sev: str) -> int:
    order = {"critical": 0, "high": 1, "med": 2, "low": 3}
    return order.get(sev, 9)


def normalize_slug(text: str) -> str:
    out = []
    for ch in text.lower():
        if ch.isalnum():
            out.append(ch)
        elif ch in {" ", "-", "_", "/"}:
            out.append("_")
    slug = "".join(out)
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_")[:80] or "risk"


# ----------------------------
# Generators
# ----------------------------
def generate_ci_plan_md(root: Path, stack: Dict[str, bool], risks: List[Risk]) -> str:
    groups = group_risks(risks)
    total = len(risks)
    stack_lines = []
    if stack["node"]:
        stack_lines.append("- Node/JS detected")
    if stack["typescript"]:
        stack_lines.append("- TypeScript detected")
    if stack["python"]:
        stack_lines.append("- Python detected")
    if stack["cloudflare"]:
        stack_lines.append("- Cloudflare Worker / wrangler detected")
    if not stack_lines:
        stack_lines.append("- No clear stack detected from indexed files (still generating generic CI).")

    def format_risk(r: Risk) -> str:
        note = f"\n    - Notes: {r.notes}" if r.notes else ""
        idea = f"{r.test_idea}" if r.test_idea else "(no test idea provided)"
        return (
            f"- **{r.title}** (`{r.file_path}`)\n"
            f"  - Severity: `{r.severity}`\n"
            f"  - Test idea: {idea}{note}\n"
        )

    lines: List[str] = []
    lines.append("# CI Plan (Generated from Step 1 Repo Index)\n")
    lines.append("This plan was generated from `file_index` (path → summary → risks).\n")
    lines.append("## Detected stack\n")
    lines.extend([f"{x}\n" for x in stack_lines])
    lines.append("\n## Risk inventory\n")
    lines.append(f"Total risks found: **{total}**\n\n")
    for sev in ["critical", "high", "med", "low"]:
        rs = groups[sev]
        lines.append(f"### {sev.upper()} ({len(rs)})\n\n")
        if not rs:
            lines.append("_None._\n\n")
            continue
        for r in rs:
            lines.append(format_risk(r) + "\n")

    lines.append("## Generated test scaffolding\n")
    lines.append("- `tests/risk_tests/` contains placeholder tests derived from risk titles.\n")
    lines.append("- A later step should implement real stress tests and assertions.\n\n")

    lines.append("## Recommended CI stages\n")
    lines.append("1) Install dependencies (Node and/or Python)\n")
    lines.append("2) Lint/typecheck (if available)\n")
    lines.append("3) Unit tests\n")
    lines.append("4) Risk stress tests (from `tests/risk_tests/`)\n")
    lines.append("5) Optional: build/pack (wrangler/ts build)\n\n")

    lines.append("## Next automation step (suggested)\n")
    lines.append("A follow-on agent can:\n")
    lines.append("- Read `tests/risk_tests/manifest.json` and implement each test idea\n")
    lines.append("- Run tests in CI\n")
    lines.append("- If failures occur, apply fixes and re-run until green\n")

    return "".join(lines)


def generate_github_actions_yaml(stack: Dict[str, bool]) -> str:
    """
    Creates a reasonable, safe default workflow.
    - If Node: npm ci + npm test (if package.json exists)
    - If Python: pip install + pytest
    - Always runs our risk test scaffolds (pytest if python, node script if node)
    """
    # Keep workflow generic: only run commands if the files exist.
    # This avoids breaking repos that don't have those tools configured yet.
    return f"""name: CI

on:
  push:
  pull_request:

jobs:
  build-test:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Node
        if: ${{{{ hashFiles('package.json') != '' }}}}
        uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'

      - name: Node install
        if: ${{{{ hashFiles('package.json') != '' }}}}
        run: npm ci

      - name: Node tests (if defined)
        if: ${{{{ hashFiles('package.json') != '' }}}}
        run: |
          if npm run | grep -qE '^  test'; then
            npm test
          else
            echo "No npm test script found; skipping."
          fi

      - name: Setup Python
        if: ${{{{ hashFiles('pyproject.toml', 'requirements.txt', 'setup.py') != '' }}}}
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'
          cache: 'pip'

      - name: Python install
        if: ${{{{ hashFiles('pyproject.toml', 'requirements.txt', 'setup.py') != '' }}}}
        run: |
          python -m pip install --upgrade pip
          if [ -f requirements.txt ]; then
            pip install -r requirements.txt
          elif [ -f pyproject.toml ]; then
            pip install .
          else
            echo "No requirements.txt or pyproject.toml install target; skipping."
          fi
          # Ensure pytest exists for risk tests
          pip install pytest

      - name: Python tests (if any)
        if: ${{{{ hashFiles('pyproject.toml', 'requirements.txt', 'setup.py') != '' }}}}
        run: |
          if [ -d tests ]; then
            pytest -q || (echo "pytest failed"; exit 1)
          else
            echo "No tests/ directory; skipping."
          fi

      - name: Risk stress test scaffolds (Python)
        if: ${{{{ hashFiles('tests/risk_tests/*.py') != '' }}}}
        run: pytest -q tests/risk_tests || (echo "risk tests failed"; exit 1)

      - name: Risk stress test scaffolds (Node)
        if: ${{{{ hashFiles('tests/risk_tests/*.js') != '' }}}}
        run: node tests/risk_tests/run_node_risk_tests.js

      - name: Wrangler validate (optional)
        if: ${{{{ hashFiles('wrangler.toml') != '' }}}}
        run: |
          if [ -f package.json ]; then
            # Try wrangler if present
            npx wrangler --version || true
          fi
"""


def generate_tests_scaffold(risks: List[Risk], stack: Dict[str, bool]) -> Dict[str, str]:
    """
    Returns a dict of relative_path -> file_contents to write.
    Creates:
      - tests/risk_tests/manifest.json
      - tests/risk_tests/README.md
      - tests/risk_tests/test_risk_<slug>.py (pytest placeholders) if python present
      - tests/risk_tests/run_node_risk_tests.js + node placeholder tests if node present
    """
    groups = group_risks(risks)

    # Build manifest
    manifest = {
        "generated_at": int(time.time()),
        "counts": {k: len(v) for k, v in groups.items()},
        "risks": [
            {
                "title": r.title,
                "severity": r.severity,
                "file_path": r.file_path,
                "test_idea": r.test_idea,
                "notes": r.notes or "",
                "slug": normalize_slug(r.title),
            }
            for r in sorted(risks, key=lambda x: (severity_sort_key(x.severity), x.file_path, x.title))
        ],
    }

    files: Dict[str, str] = {}

    files["tests/risk_tests/manifest.json"] = json.dumps(manifest, indent=2)

    files["tests/risk_tests/README.md"] = """# Risk Stress Tests (Generated)

These tests are scaffolds generated from Step 1 risk extraction.

- `manifest.json` lists all detected risks and a suggested test idea.
- The placeholder tests are intentionally minimal.
- A later automation step should implement real stress tests + assertions.

## How to extend
- For each entry in `manifest.json`, implement a test file that:
  1) reproduces/validates the risk
  2) fails reliably if the risk exists
  3) passes after the fix is applied

## Notes
- Keep tests deterministic.
- Avoid external network calls unless mocked.
"""

    # Python scaffolds
    if stack["python"]:
        # A single pytest file that enumerates risks and marks TODOs
        files["tests/risk_tests/test_risk_scaffolds.py"] = """import json
from pathlib import Path

MANIFEST = Path(__file__).with_name("manifest.json")

def test_manifest_exists():
    assert MANIFEST.exists()

def test_risk_scaffolds_placeholder():
    data = json.loads(MANIFEST.read_text(encoding="utf-8"))
    # This test is a placeholder so the suite runs.
    # Replace with per-risk tests as you implement them.
    assert "risks" in data
"""

        # Additionally create per-severity placeholder grouping tests (minimal)
        for sev in ["critical", "high"]:
            files[f"tests/risk_tests/test_{sev}_risks_placeholder.py"] = f"""import json
from pathlib import Path

MANIFEST = Path(__file__).with_name("manifest.json")

def test_{sev}_risks_listed():
    data = json.loads(MANIFEST.read_text(encoding="utf-8"))
    risks = [r for r in data.get("risks", []) if r.get("severity") == "{sev}"]
    # Placeholder assertion: file generated properly
    assert isinstance(risks, list)
"""

    # Node scaffolds
    if stack["node"]:
        files["tests/risk_tests/run_node_risk_tests.js"] = """// Minimal node runner for risk test scaffolds.
// You can replace this with Jest/Vitest/etc. later.

const fs = require("fs");
const path = require("path");

const manifestPath = path.join(__dirname, "manifest.json");

function main() {
  if (!fs.existsSync(manifestPath)) {
    console.error("Missing manifest.json");
    process.exit(1);
  }
  const data = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  console.log("Risk scaffolds:", (data.risks || []).length);
  // Placeholder: always pass.
  // Later: execute per-risk scripts and fail on reproduction.
  process.exit(0);
}

main();
"""

    return files


# ----------------------------
# Main CLI
# ----------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Repo root")
    ap.add_argument("--db", default=".repo_index.sqlite", help="SQLite DB created by Step 1")
    ap.add_argument("--ci-plan", default="CI_PLAN.md", help="Output markdown plan")
    ap.add_argument("--workflow", default=".github/workflows/ci.yml", help="Output GitHub Actions workflow")
    ap.add_argument("--tests-dir", default="tests", help="Tests directory root")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite generated files if they exist")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    db_path = root / args.db
    ci_plan_path = root / args.ci_plan
    workflow_path = root / args.workflow

    if not db_path.exists():
        print(f"ERROR: DB not found: {db_path}\nRun Step 1 first to generate file_index.", file=sys.stderr)
        return 2

    conn = sqlite3.connect(db_path)
    rows = read_db_rows(conn)
    paths = [r[0] for r in rows]
    risks = parse_risks(rows)
    stack = detect_repo_stack(paths)

    # Generate outputs
    ci_plan_md = generate_ci_plan_md(root, stack, risks)
    workflow_yml = generate_github_actions_yaml(stack)
    test_files = generate_tests_scaffold(risks, stack)

    # Write outputs
    safe_write(ci_plan_path, ci_plan_md, overwrite=args.overwrite)
    safe_write(workflow_path, workflow_yml, overwrite=args.overwrite)

    for rel, content in test_files.items():
        outp = root / rel
        safe_write(outp, content + ("\n" if not content.endswith("\n") else ""), overwrite=args.overwrite)

    print("✅ Step 2 generated:")
    print(f"  - {ci_plan_path}")
    print(f"  - {workflow_path}")
    print(f"  - tests scaffolds under: {root / 'tests/risk_tests'}")
    print("\nNext:")
    print("  - Commit the workflow and plan (optional).")
    print("  - Implement real stress tests based on tests/risk_tests/manifest.json")
    print("  - Add a Step 3 agent to run CI, reproduce risks, fix code, and re-run tests.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
