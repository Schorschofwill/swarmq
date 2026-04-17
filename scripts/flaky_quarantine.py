#!/usr/bin/env python3
"""U4.4 — Flaky-Quarantäne-Helper.

Liest pytest-JSON-Reports aus den letzten N Nightly-Läufen, identifiziert
Tests die ≥3× konsekutiv gescheitert sind, und printet einen
Issue-Body-Vorschlag für den Operator.

Phase-1-Version: kein Auto-Skip-Commit. Operator entscheidet manuell,
ob ein Test in pyproject.toml als skipped markiert oder gefixt wird.

Usage::

    python scripts/flaky_quarantine.py reports/*.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path


def parse_pytest_json(path: Path) -> list[tuple[str, str]]:
    """Return list of (nodeid, outcome) from a pytest --json-report file."""
    data = json.loads(path.read_text())
    out: list[tuple[str, str]] = []
    for test in data.get("tests", []):
        out.append((test["nodeid"], test.get("outcome", "unknown")))
    return out


def find_flakes(reports: list[Path], min_failures: int = 3) -> dict[str, int]:
    failures: dict[str, int] = defaultdict(int)
    for path in reports:
        try:
            for nodeid, outcome in parse_pytest_json(path):
                if outcome == "failed":
                    failures[nodeid] += 1
                elif outcome == "passed":
                    failures[nodeid] = 0  # streak broken
        except Exception as exc:
            print(f"WARN: failed to parse {path}: {exc}", file=sys.stderr)
    return {n: c for n, c in failures.items() if c >= min_failures}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("reports", nargs="+", type=Path)
    parser.add_argument("--min-failures", type=int, default=3)
    args = parser.parse_args()

    flakes = find_flakes(sorted(args.reports), args.min_failures)
    if not flakes:
        print("No quarantine candidates.")
        return 0

    print(f"\n=== {len(flakes)} quarantine candidate(s) ===\n")
    for node, count in sorted(flakes.items(), key=lambda kv: -kv[1]):
        print(f"  {count}x failed: {node}")
    print("\nSuggested action: skip with reason in pyproject.toml or fix.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
