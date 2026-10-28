"""Coverage-Gate nach dem Combine (Plan 2026-10-13-0902, KTD7/KTD8).

Laeuft im CI-Job ``coverage-combine`` nach ``coverage xml``:

- Tags: nichts zu pruefen.
- Sonst wird ``main`` ausdruecklich geholt; eine Branch-Pipeline laedt nur
  ihren eigenen Ref. Fehlt ``origin/main`` danach, scheitert das Gate, statt
  gegen 0 zu vergleichen.
- Auf ``main`` selbst: kein Vergleich (die Untergrenze prueft
  ``coverage report`` ueber ``fail_under``).
- Auf jedem anderen Branch, beides gegen den Merge-Base mit ``origin/main``:
  ``fail_under`` darf nicht sinken (Ratchet), und neue oder geaenderte Zeilen
  muessen zu 100 % abgedeckt sein (diff-cover).

Ref-Art und Branch kommen aus ``GITHUB_REF_TYPE``/``GITHUB_REF_NAME``. Bis
zur Umstellung laeuft daneben noch die GitLab-CI, deshalb gelten
``CI_COMMIT_TAG``/``CI_COMMIT_BRANCH`` ebenfalls (Plan 2026-10-28-1308, KTD8).
"""

from __future__ import annotations

import os
import subprocess
import sys
import tomllib
from pathlib import Path


def fail_under(pyproject: str) -> float:
    """``[tool.coverage.report] fail_under`` aus *pyproject*; ohne Eintrag 0."""
    report = tomllib.loads(pyproject).get("tool", {}).get("coverage", {}).get("report", {})
    return float(report.get("fail_under", 0))


def ratchet_error(base_pyproject: str, head_pyproject: str) -> str | None:
    """Fehlermeldung, wenn der Branch ``fail_under`` gegenüber dem Merge-Base senkt."""
    base, head = fail_under(base_pyproject), fail_under(head_pyproject)
    if head < base:
        return f"fail_under sinkt von {base:g} (Merge-Base mit main) auf {head:g}; die Untergrenze darf nur steigen"
    return None


def _git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], capture_output=True, text=True, check=False)


def main() -> int:
    ref_type = os.environ.get("GITHUB_REF_TYPE")
    if os.environ.get("CI_COMMIT_TAG") or ref_type == "tag":
        print("coverage-gate: Tag-Pipeline, kein Vergleich")
        return 0
    _git("fetch", "--quiet", "origin", "+refs/heads/main:refs/remotes/origin/main")
    if _git("rev-parse", "--verify", "--quiet", "refs/remotes/origin/main").returncode != 0:
        print("coverage-gate: origin/main fehlt nach dem Fetch; ohne main kein Vergleich moeglich", file=sys.stderr)
        return 1
    branch = (
        os.environ.get("CI_COMMIT_BRANCH")
        or (os.environ.get("GITHUB_REF_NAME") if ref_type == "branch" else None)
        or _git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    )
    if branch == "main":
        print("coverage-gate: main, kein Ratchet- und Diff-Vergleich")
        return 0
    base = _git("merge-base", "refs/remotes/origin/main", "HEAD").stdout.strip()
    if not base:
        print("coverage-gate: kein Merge-Base mit origin/main (volle Historie noetig, GIT_DEPTH 0)", file=sys.stderr)
        return 1
    base_pyproject = _git("show", f"{base}:pyproject.toml").stdout
    head_pyproject = Path("pyproject.toml").read_text(encoding="utf-8")
    error = ratchet_error(base_pyproject, head_pyproject)
    if error is not None:
        print(f"coverage-gate: {error}", file=sys.stderr)
        return 1
    print(f"coverage-gate: diff-cover gegen Merge-Base {base[:12]}")
    return subprocess.run(
        ["diff-cover", "coverage.xml", f"--compare-branch={base}", "--fail-under=100"],
        check=False,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
