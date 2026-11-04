#!/usr/bin/env python3
"""Turn dbt's target/run_results.json into a test-management summary.

Usage: python scripts/summarise_run_results.py [path/to/run_results.json] [--fail-on-warn]

Prints a markdown report (also appended to $GITHUB_STEP_SUMMARY in CI) and exits
non-zero if any node errored/failed. Gives pass-rate metrics by test type without
needing a dbt Cloud UI.
"""
import json
import os
import sys
from collections import Counter, defaultdict

BAD = {"error", "fail", "runtime error"}


def classify(unique_id: str) -> str:
    kind = unique_id.split(".")[0]
    if kind == "unit_test":
        return "unit test"
    if kind == "test":
        name = unique_id.split(".")[2]
        return "singular test" if name.startswith("assert_") else "generic test"
    return {"model": "model", "seed": "seed", "snapshot": "snapshot"}.get(kind, kind)


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    path = args[0] if args else "target/run_results.json"
    fail_on_warn = "--fail-on-warn" in sys.argv
    with open(path) as fh:
        results = json.load(fh)["results"]

    by_type = defaultdict(Counter)
    problems, warnings = [], []
    for r in results:
        status = r["status"].lower()
        by_type[classify(r["unique_id"])][status] += 1
        if status in BAD:
            problems.append((r["unique_id"], status, (r.get("message") or "")[:200]))
        elif status == "warn":
            warnings.append((r["unique_id"], r.get("failures"), (r.get("message") or "")[:200]))

    lines = ["## dbt run summary", "", "| Type | Total | Pass/Success | Warn | Fail/Error | Skipped |", "|---|---|---|---|---|---|"]
    for kind in sorted(by_type):
        c = by_type[kind]
        ok = c["pass"] + c["success"]
        bad = sum(c[s] for s in BAD)
        lines.append(f"| {kind} | {sum(c.values())} | {ok} | {c['warn']} | {bad} | {c['skipped']} |")

    tests = [r for r in results if r["unique_id"].startswith(("test.", "unit_test."))]
    if tests:
        passed = sum(1 for r in tests if r["status"].lower() in {"pass", "success"})
        lines += ["", f"**Test pass rate: {passed}/{len(tests)} ({passed / len(tests):.0%})**"]
    if warnings:
        lines += ["", "### Warnings (data-quality signals, not blockers)"]
        lines += [f"- `{u}` - {f} failing rows" for u, f, _ in warnings]
    if problems:
        lines += ["", "### Failures"]
        lines += [f"- `{u}` [{s}] {m}" for u, s, m in problems]

    report = "\n".join(lines)
    print(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as fh:
            fh.write(report + "\n")
    return 1 if problems or (fail_on_warn and warnings) else 0


if __name__ == "__main__":
    sys.exit(main())
