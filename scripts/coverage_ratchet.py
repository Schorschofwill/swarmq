"""Coverage-Untergrenze anheben (Plan 2026-10-13-0902, U9, KTD7).

Aufruf: ``uv run just ratchet [wert wert wert]``. Ohne Werte holt das Skript
die Quoten der letzten drei erfolgreichen CI-Laeufe auf ``main`` ueber die
GitHub-API (``gh``): ``coverage-combine`` legt die Gesamtquote als Artefakt
``coverage-total`` ab (Plan 2026-10-28-1308, KTD7/KTD8). Neue Untergrenze: kleinster Wert
minus 0,5 Prozentpunkte (die in U6 gemessene Schwankung lag weit darunter),
auf ganze Prozent abgerundet. Lokale Laeufe liegen systematisch tiefer (kein
zweiter Broker, andere Suiten); deshalb zaehlen nur Werte aus der CI.

Das Skript hebt nur an, senkt nie, und weigert sich, wenn versionierte
Dateien geaendert sind (unversionierte stoeren nicht). Es aendert nur ``pyproject.toml``; committen ist Handarbeit.
"""

from __future__ import annotations

import io
import json
import math
import re
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

_PYPROJECT = Path("pyproject.toml")
_SPREAD = 0.5
_SECTION = "[tool.coverage.report]"


def derive_floor(values: list[float]) -> int:
    return math.floor(min(values) - _SPREAD)


def current_floor(text: str) -> float | None:
    report = tomllib.loads(text).get("tool", {}).get("coverage", {}).get("report", {})
    value = report.get("fail_under")
    return None if value is None else float(value)


def with_floor(text: str, floor: int) -> str:
    """``text`` mit ``fail_under = floor`` im Abschnitt ``[tool.coverage.report]``."""
    start = text.index(_SECTION) + len(_SECTION)
    end = text.find("\n[", start)
    end = len(text) if end == -1 else end
    section = text[start:end]
    line = f"fail_under = {floor}"
    if re.search(r"(?m)^fail_under\s*=", section):
        section = re.sub(r"(?m)^fail_under\s*=.*$", line, section)
    else:
        # Der folgende Abschnitt beginnt mit "\n[": genau eine Leerzeile davor.
        section = section.rstrip("\n") + f"\n{line}\n"
    return text[:start] + section + text[end:]


# ``gh`` setzt Besitzer und Repo aus dem Git-Remote ein.
_REPO = "repos/{owner}/{repo}"


def _gh_api(path: str) -> bytes:
    return subprocess.run(["gh", "api", path], capture_output=True, check=True).stdout


def _main_values() -> list[float]:
    runs = json.loads(_gh_api(f"{_REPO}/actions/workflows/ci.yml/runs?branch=main&event=push&status=success"))
    values: list[float] = []
    for run in runs["workflow_runs"]:
        # ``branch=main`` vergleicht nur den Branchnamen; ein Pull Request aus einem
        # Fork, dessen Branch main heisst, passt auch. Nur Pushes auf das eigene main zaehlen.
        if run.get("event") != "push" or run["head_repository"]["full_name"] != run["repository"]["full_name"]:
            continue
        artifacts = json.loads(_gh_api(f"{_REPO}/actions/runs/{run['id']}/artifacts?name=coverage-total"))
        if not artifacts["artifacts"]:
            continue
        archive = _gh_api(f"{_REPO}/actions/artifacts/{artifacts['artifacts'][0]['id']}/zip")
        with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
            values.append(float(zipped.read("coverage-total.txt").decode().strip()))
        if len(values) == 3:
            break
    return values


def main(argv: list[str]) -> int:
    if subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"], capture_output=True, text=True, check=True
    ).stdout.strip():
        print("ratchet: Arbeitsbaum veraendert; erst committen oder aufraeumen", file=sys.stderr)
        return 1
    values = [float(v.replace(",", ".")) for v in argv] if argv else _main_values()
    if len(values) < 3:
        print(f"ratchet: drei Messwerte noetig, gefunden {len(values)}", file=sys.stderr)
        return 1
    text = _PYPROJECT.read_text(encoding="utf-8")
    old = current_floor(text)
    new = derive_floor(values)
    print(f"ratchet: Werte {', '.join(f'{v:.2f}' for v in values)} -> abgeleitet {new}, bisher {old}")
    if old is not None and new <= old:
        print("ratchet: kein Anstieg, fail_under bleibt unveraendert")
        return 0
    _PYPROJECT.write_text(with_floor(text, new), encoding="utf-8")
    print(f"ratchet: fail_under = {new} in pyproject.toml eingetragen")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
