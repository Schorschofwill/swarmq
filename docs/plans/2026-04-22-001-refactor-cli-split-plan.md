---
title: "Refactor cli.py — split 1621-LoC monolith into cli/ package"
status: implemented-and-reviewed
execution_mode: big-bang
quantification:
  cli_py_touches_last_6mo: 18
  baseline_unit_tests: 1113 passed / 1 skipped in 16.4s
created: 2026-04-22
depends_on:
  - R5.3 Hot Reload (merged 2026-04-22) — last feature to land in cli.py
  - R4.6 ProcessManager (merged 2026-04-19) — `run_multi_process_worker`
    entry + circular import `cli.run_worker_async` ↔ `process_manager`
---

## Overview

`src/swarmq/cli.py` ist mit 1621 Zeilen das zweitgrößte Modul nach
`worker.py`. Es bündelt sechs disjunkte Verantwortlichkeiten (Parser,
Worker-Subcommand, Schedule-Subcommands, DLQ-Subcommands, Inspect,
Check/Main) in einer Datei, obwohl zwischen den Subcommand-Handlern
**kein geteilter Zustand** existiert. Die einzigen gemeinsamen
Dependencies sind pure Hilfsfunktionen (`_broker_from_env`,
`_backend_from_env`, argparse-Type-Validatoren, Tabellen-Renderer).

Ziel dieses Refactorings: Verhaltens-identische Umstellung von
`cli.py` auf ein `cli/`-Package mit einem Modul pro Subcommand und
einem neutralen `_shared/`-Bereich für die geteilten Helfer. **Keine
Verhaltensänderung, kein neues Flag, kein API-Break.**

**Nicht-Ziele:** Keine neue Subcommand-Logik, keine neue CLI-Option,
keine Änderung an Exit-Codes, Ausgabeformaten, JSON-Contract des
`--check`-Pfads, argparse-Messages, Log-Keys. Keine Konsolidierung
von Tests oder Fixture-Umbau.

## Problem Frame

1. **Monolith-Kosten:** Ein 1621-LoC-Modul mit 33 Top-Level-Funktionen
   verlängert Code-Navigation, Review-Zeit und Diff-Blast-Radius
   jedes neuen Subcommands. Jeder Hot-Reload-/DLQ-/Schedule-Change
   der letzten sechs Monate hat in dieser Datei eingegriffen — das
   macht Merges empfindlich.

2. **Mentale Kopplung ohne reale Kopplung:** Schedule-, DLQ- und
   Worker-Pfade haben **keinen geteilten Zustand**; sie werden nur
   durch das gemeinsame File zu einer Einheit. Ein Split macht die
   tatsächliche Unabhängigkeit sichtbar und erlaubt paralleles
   Arbeiten an verschiedenen Subcommands ohne Konflikt.

3. **Zirkuläre Import-Falle ist real:** `process_manager.py:754`
   importiert `from swarmq.cli import run_worker_async`. Ein naives
   Aufsplitten ohne `__init__.py`-Re-Exports bricht diesen Import.
   Die Refactoring-Kernregel ist deshalb: **alle bisher unter
   `swarmq.cli.<name>` erreichbaren Symbole bleiben unter exakt
   demselben dotted path**.

4. **Test-Patches hängen an dotted paths:** Tests verwenden
   `patch("swarmq.cli.run_worker_async")`,
   `patch("swarmq.cli.asyncio.run")`, und direkte Imports von
   `_autoscale_range`, `_build_parser`, `_watch_dir_path`, `main`.
   Jeder Symbolwechsel unter `swarmq.cli.*` bricht stumm einen Test
   — also: Namensstabilität ist Pflicht, nicht Kür.

5. **Kein geteilter Typ-Staat:** `cli.py` steht in `[tool.mypy]
   ignore_errors=true`-Legacy-Block. Das Refactoring **behält** das
   `ignore_errors=true` pro neuem Teil-Modul — Type-Debt-Abbau ist
   ein separater Pass, nicht Teil dieses Plans.

## Requirements Trace

| ID  | Anforderung | Quelle | Satisfied by |
|-----|-------------|--------|--------------|
| R-1 | Jedes öffentliche Symbol, das vor dem Refactoring unter `swarmq.cli.<name>` erreichbar war, ist danach unter exakt demselben Pfad erreichbar. | Problem Frame #3/#4 | Unit 1 (Audit-Test) + Unit 2 (Re-Export-Fassade) |
| R-2 | `swarmq worker`, `swarmq schedule *`, `swarmq dlq *`, `swarmq inspect`, `swarmq worker --check` verhalten sich byte-identisch (stdout, stderr, Exit-Code, JSON-Payload, argparse-Help-Text). | Scope Boundaries | Unit 8 (bestehende CLI-Tests bleiben unverändert grün) |
| R-3 | Circular Import `process_manager` ↔ `cli.run_worker_async` bleibt funktional. | Problem Frame #3 | Unit 2 + Unit 3 |
| R-4 | `cli/` Package-Struktur trennt Subcommands in **ein Modul pro Command-Family**, mit einem `_shared.py`-Modul für Helfer, die von mehr als einem Subcommand benutzt werden. | Design Decision #1 | Units 3–7 |
| R-5 | Kein neues Verhalten, kein neues Flag, kein entfernter Code, kein umbenanntes Symbol. Reine Move-Operation. | Overview | Code-Review-Gate + Unit 8 |
| R-6 | Jedes neue Modul in `cli/` bleibt unter mypy `ignore_errors=true` (Legacy-Bucket), um Type-Debt-Abbau orthogonal zu halten. | Problem Frame #5 | Unit 2 (`pyproject.toml`-Update) |
| R-7 | Test-Coverage bleibt unverändert: `tests/unit/test_cli.py`, `tests/unit/test_cli_multiprocess.py`, `tests/integration/test_cli_schedule.py`, `tests/integration/test_cli_worker.py`, `tests/e2e/test_dlq_cli.py` laufen ohne Änderung grün. | Testing-Anforderung | Unit 8 |
| R-8 | `swarmq.cli.*`-Attribut-Audit-Test verhindert stille Regressions bei zukünftigen Splits: eine Whitelist aller vor-Split öffentlich erreichbaren Symbole wird gegen `dir(swarmq.cli)` geprüft (Identity-Check pro Symbol, nicht nur Präsenz). | Problem Frame #4 | Unit 1 (kombiniert mit früherem Unit 9) |

## Scope Boundaries

**In Scope:**
- `src/swarmq/cli.py` → `src/swarmq/cli/` Package mit Sub-Modulen.
- `src/swarmq/cli/__init__.py` als Re-Export-Fassade (keine Logik).
- `pyproject.toml` — mypy-Override erweitert um neue Modulnamen (`swarmq.cli.*`).
- Neuer Audit-Test (Unit 1) gegen Symbol-Drift, permanent ab dem ersten Commit.
- Keine Änderungen an existierenden Test-Dateien (das ist das Gate).

**Out of Scope (bewusst):**
- Typ-Debt-Abbau für Teil-Module (separater Plan nach diesem Merge).
- `cli.py`-interne Refactorings **innerhalb** einer Subcommand-Family
  (z.B. `_run_dlq_retry` in kleinere Helfer zerlegen) — separate Unit
  später, sobald die Struktur steht.
- Import-Kreis `process_manager` ↔ `cli` auflösen (der lokale Import
  in `process_manager.py:754` bleibt; Auflösung wäre eigene
  Architektur-Entscheidung).
- `cli.py`-Docstring-Header-Rewrite (historischer Kontext; bleibt).

## Target Structure

```
src/swarmq/cli/
  __init__.py              # Re-Export-Fassade + main() entry point.
                           # main() bleibt hier, damit
                           # `patch("swarmq.cli.run_worker_async")` die
                           # tatsächliche Bindung patcht, die main() auflöst.
                           # Sonst würde main() in einem anderen Modul sein
                           # eigenes `from …worker_cmd import …` halten
                           # und die Fassaden-Patches gingen ins Leere.
  _shared.py               # Cross-command Helfer (Broker/Backend-Env,
                           # Tabellen-Renderer, JSON-Normalisierung,
                           # argparse-Type-Validatoren).
  parser.py                # _build_parser, _swarmq_version.
  worker_cmd.py            # run_worker_async.
  schedule_cmd.py          # _run_schedule_add/list/remove/status/_async,
                           # _format_schedule_table.
  dlq_cmd.py               # _run_dlq_list/inspect/retry/purge/_async,
                           # _format_dlq_list_table, _DLQNotFound,
                           # _DLQ_NOT_FOUND_EXIT_CODE, _normalise_headers_for_json.
  inspect_cmd.py           # _run_inspect_async, _format_inspect_table.
  check_cmd.py             # _run_worker_check, _seed_check_payload,
                           # _is_check_invocation, _emit_argparse_check_error,
                           # _WORKER_CHECK_PAYLOAD_KEYS.
```

> **Kein separates `main.py`:** Review-Finding (feasibility + adversarial,
> hohe Konfidenz) — wenn `main()` nach `cli/main.py` zieht und per
> `from swarmq.cli.worker_cmd import run_worker_async` einbindet, patcht
> `patch("swarmq.cli.run_worker_async")` nur die Fassade, nicht die
> Bindung in `main.py`. Tests blieben grün und patchen nichts Echtes.
> `main()` in `__init__.py` zu lassen hält die Patch-Semantik intakt.
> `asyncio` bleibt als Top-Level-Import der Fassade erhalten (Singleton-
> Modul-Objekt — Patches durchdringen das Modul).

### Symbol-zu-Modul-Mapping (vollständig)

| Quell-Symbol (Zeile in alter cli.py) | Zielmodul |
|---|---|
| `_VALID_LOG_LEVELS`, `_VALID_LOG_FORMATS` (45/46) | `_shared.py` |
| `_positive_int` (49) | `_shared.py` |
| `_positive_float` (62) | `_shared.py` |
| `_DOTTED_IDENTIFIER_RE` (84), `_dotted_module_name` (90) | `_shared.py` |
| `_autoscale_range` (106) | `_shared.py` |
| `_FORBIDDEN_WATCH_PREFIXES` (137), `_watch_dir_path` (140) | `_shared.py` |
| `_queue_list` (168) | `_shared.py` |
| `_swarmq_version` (184) | `parser.py` |
| `_build_parser` (197) | `parser.py` |
| `run_worker_async` (682) | `worker_cmd.py` |
| `_backend_from_env` (733) | `_shared.py` |
| `_render_table` (751) | `_shared.py` |
| `_format_schedule_table` (772) | `schedule_cmd.py` |
| `_run_schedule_list` (805) | `schedule_cmd.py` |
| `_run_schedule_add` (820) | `schedule_cmd.py` |
| `_run_schedule_remove` (944) | `schedule_cmd.py` |
| `_run_schedule_status` (974) | `schedule_cmd.py` |
| `_format_inspect_table` (1027) | `inspect_cmd.py` |
| `_broker_from_env` (1048) | `_shared.py` |
| `_format_dlq_list_table` (1067) | `dlq_cmd.py` |
| `_normalise_headers_for_json` (1087) | `dlq_cmd.py` |
| `_DLQ_NOT_FOUND_EXIT_CODE` (1108) | `dlq_cmd.py` |
| `_DLQNotFound` (1111) | `dlq_cmd.py` |
| `_run_dlq_list` (1120) | `dlq_cmd.py` |
| `_run_dlq_inspect` (1152) | `dlq_cmd.py` |
| `_run_dlq_retry` (1211) | `dlq_cmd.py` |
| `_run_dlq_purge` (1289) | `dlq_cmd.py` |
| `_run_dlq_async` (1305) | `dlq_cmd.py` |
| `_run_inspect_async` (1321) | `inspect_cmd.py` |
| `_run_schedule_async` (1365) | `schedule_cmd.py` |
| `_WORKER_CHECK_PAYLOAD_KEYS` (1385) | `check_cmd.py` |
| `_seed_check_payload` (1397) | `check_cmd.py` |
| `_run_worker_check` (1416) | `check_cmd.py` |
| `_is_check_invocation` (1468) | `check_cmd.py` |
| `_emit_argparse_check_error` (1489) | `check_cmd.py` |
| `main` (1510) | `__init__.py` (bewusst nicht extrahiert — siehe Hinweis oben) |

### Re-Export-Fassade (`cli/__init__.py`)

Die Fassade re-exportiert **jedes** oben aufgeführte Symbol, sodass
`from swarmq.cli import X` und `patch("swarmq.cli.X")` unverändert
funktionieren. Zusätzlich bleibt `asyncio` als Modul-Attribut
erreichbar (für `patch("swarmq.cli.asyncio.run")`) — Fassade
importiert `asyncio` auf Top-Level.

```python
# src/swarmq/cli/__init__.py — Skeleton
# noqa: F401 auf `asyncio` ist load-bearing:
# tests/unit/test_cli_multiprocess.py:144 verwendet
# patch("swarmq.cli.asyncio.run"). Entfernen bricht diesen Test.
# Kein ruff-Autoremove, kein isort-Cleanup erlaubt.
import asyncio  # noqa: F401 — load-bearing for patch("swarmq.cli.asyncio.run")

from swarmq.cli._shared import (
    _VALID_LOG_LEVELS,
    _VALID_LOG_FORMATS,
    _positive_int,
    _positive_float,
    _DOTTED_IDENTIFIER_RE,
    _dotted_module_name,
    _autoscale_range,
    _FORBIDDEN_WATCH_PREFIXES,
    _watch_dir_path,
    _queue_list,
    _backend_from_env,
    _broker_from_env,
    _render_table,
)
from swarmq.cli.parser import _build_parser, _swarmq_version
from swarmq.cli.worker_cmd import run_worker_async
from swarmq.cli.schedule_cmd import (
    _format_schedule_table,
    _run_schedule_list,
    _run_schedule_add,
    _run_schedule_remove,
    _run_schedule_status,
    _run_schedule_async,
)
from swarmq.cli.dlq_cmd import (
    _format_dlq_list_table,
    _normalise_headers_for_json,
    _DLQ_NOT_FOUND_EXIT_CODE,
    _DLQNotFound,
    _run_dlq_list,
    _run_dlq_inspect,
    _run_dlq_retry,
    _run_dlq_purge,
    _run_dlq_async,
)
from swarmq.cli.inspect_cmd import _format_inspect_table, _run_inspect_async
from swarmq.cli.check_cmd import (
    _WORKER_CHECK_PAYLOAD_KEYS,
    _seed_check_payload,
    _run_worker_check,
    _is_check_invocation,
    _emit_argparse_check_error,
)

# main() lebt hier. Kein Import aus einem main.py-Submodul — die
# Patch-Semantik verlangt, dass `main()` die Symbole unter exakt
# diesen Namen (`run_worker_async`, `_run_dlq_async`, etc.) auflöst.
def main(argv: Sequence[str] | None = None) -> int:
    ...  # body from old cli.py:1510-1629

__all__ = [...]  # full whitelist, also consumed by Unit 1 audit test
```

## Design Decisions

**#1 — Ein Modul pro Command-Family, nicht pro Funktion.**
Schedule hat fünf Handler, DLQ hat fünf — sie in Einzel-Module pro
Handler zu zerlegen würde das Navigations-Problem umkehren (statt
einer großen Datei: zwanzig kleine). Eine Datei pro Subcommand-
Family ist die richtige Granularität: Schedule-Entwickler öffnen
`schedule_cmd.py`, DLQ-Entwickler öffnen `dlq_cmd.py`. Die
Format-Helfer (`_format_schedule_table`, `_format_dlq_list_table`)
ziehen mit ihrem Command mit, weil sie nur dort genutzt werden.

**#2 — `_shared.py` statt `_shared/`-Unterpaket.**
Zehn Helfer sind kein Paket wert. Ein einzelnes `_shared.py`-Modul
hält die cross-cutting Validatoren (`_positive_int` &co.), die
Connection-Factories (`_backend_from_env`, `_broker_from_env`) und
den generischen Tabellen-Renderer. Die command-spezifischen
Renderer bleiben im jeweiligen Command-Modul.

**#3 — Re-Export-Fassade statt Imports fixen.**
Wir fassen `swarmq.cli` als öffentliche API auf (jedes bisher
erreichbare Symbol zählt als öffentlich — Tests beweisen das). Die
Alternative — alle Test-Imports und `process_manager`-Imports auf
die neuen Pfade umziehen — ist ein größerer Diff, höheres Risiko,
und koppelt uns in Zukunft an die interne Struktur. Die Fassade
entkoppelt und ist billig zu warten (reiner Re-Export).

**#4 — Keine `__all__`-basierten Re-Exports aus Unter-Modulen.**
Die Sub-Module exportieren explizit via normaler `def`/Konstant-
Definition; nur `__init__.py` verwendet `__all__`. Das hält die
Semantik simpel und macht den Audit-Test (Unit 1) deterministisch.

**#5 — `check_cmd.py` ist eigenes Modul, nicht Teil von
`worker_cmd.py`.**
`--check` ist ein dry-run Validator mit eigenem JSON-Contract und
eigenem Exit-Code-Vertrag; der Code überschneidet sich nicht mit
`run_worker_async`. Eigenes Modul macht den Contract-Perimeter
sichtbar und hält `worker_cmd.py` klein.

**#6 — Keine Test-Änderungen im Happy Path.**
Der Beweis, dass das Refactoring verhaltens-identisch ist, ist die
Tatsache, dass **keine** bestehende Test-Zeile angefasst wird. Der
Audit-Test (Unit 1) ist neu, aber additiv. Das ist das härteste
verfügbare Feature-Parity-Gate.

## Units

TDD-konform: Jede Unit beginnt mit einem fehlschlagenden Test
(oder Audit), dann minimaler Code, dann `pytest tests/ -x`.

**Gate-Standard für jede Unit ab Unit 2:**
- `pytest tests/unit -q` grün
- `pytest tests/unit/test_cli_module_surface.py -v` grün
  (Audit + Identity-Check)
- `uv run swarmq --version` liefert Exit-Code 0

### Unit 1 — Audit-Test (Baseline + permanenter Regression-Guard)

**Ziel:** Ein einziger Test-File, der (a) die vor-Refactor-Surface
festhält und (b) dauerhaft gegen Symbol-Drift schützt. Ersetzt das
ursprüngliche "Baseline + Audit in zwei Units"-Schema.

**Test-First:** `tests/unit/test_cli_module_surface.py` mit:
1. Whitelist der aktuellen öffentlich erreichbaren Symbole (hart
   kodiert, aus `[n for n in dir(swarmq.cli) if not n.startswith('__')]`
   des heutigen `cli.py` extrahiert).
2. Assertion `set(n for n in dir(cli) if not n.startswith('__')) ==
   WHITELIST` — **Equality**, nicht Superset (fängt auch unbeabsichtigte
   API-Erweiterungen ab).
3. Identity-Assertion: für jedes callable Symbol
   `obj = getattr(cli, name); assert obj is not None and callable(obj)`
   (fängt kaputte Re-Exports, die `dir()` passieren aber `getattr()`
   schlagen lassen).
4. Live-Patch-Through-Assertion: mindestens ein
   `with patch("swarmq.cli.run_worker_async") as m: cli.main(["worker",
   "--module=x.y"]); m.assert_called_once()` — beweist, dass die
   Fassaden-Patches die echten Call-Sites in `main()` treffen
   (ce-review adversarial #4).
5. Subprocess-Import-Assertion (fresh interpreter):
   `subprocess.run([sys.executable, "-c", "import swarmq.cli; [getattr(
   swarmq.cli, n) for n in WHITELIST]"])` returncode 0 — catcht
   Zirkular-Init-Probleme, die der warm geladene pytest-Prozess
   versteckt (ce-review feasibility #5).

**Green:** Keine Produktionsänderung. Test läuft gegen unverändertes
`cli.py` grün.

**Gate:** `pytest tests/unit/test_cli_module_surface.py -v` grün.
Whitelist wird gegen `git show HEAD:src/swarmq/cli.py` regeneriert,
wenn sich die Surface legitim ändert.

### Unit 2 — `cli/` Package-Gerüst anlegen

**Ziel:** Datei-Struktur mit Re-Export-Fassade. Inklusive `main()`
in `__init__.py` (nicht extrahieren — siehe Target Structure Hinweis).

**Red:** Test aus Unit 1 bleibt grün (keine Surface-Änderung).

**Green:**
- `src/swarmq/cli.py` in `src/swarmq/cli/_legacy.py` verschieben
  (ein einziger Commit — nicht zwei):
  ```
  mkdir src/swarmq/cli
  git mv src/swarmq/cli.py src/swarmq/cli/_legacy.py
  find src -name "cli.cpython-*.pyc" -delete   # stale bytecode
  find src -name "__pycache__" -path "*/swarmq/*" -exec rm -rf {} +
  ```
- `src/swarmq/cli/__init__.py` anlegen mit:
  - `import asyncio  # noqa: F401` (load-bearing, siehe Skeleton)
  - `from swarmq.cli._legacy import *`
  - Expliziten `from swarmq.cli._legacy import <name>` für **jedes**
    Leading-Underscore-Symbol der Whitelist (Star-Import lädt die nicht).
  - **`main` auch explizit re-importieren**: `from swarmq.cli._legacy
    import main` — die console-script-Entry `swarmq = "swarmq.cli:main"`
    muss ab Commit 1 funktionieren.
- `pyproject.toml` — mypy-Override: den bestehenden Eintrag
  `"swarmq.cli"` in **zwei Einträge** aufteilen, `"swarmq.cli"` UND
  `"swarmq.cli.*"`. Ohne beide: Fassade (`__init__.py` = Modul
  `swarmq.cli`) oder Submodule (`swarmq.cli.worker_cmd` etc.) landen
  nicht im `ignore_errors=true`-Bucket und Unit-Gates brechen auf
  frischen Typ-Fehlern (ce-review feasibility #3 + adversarial #3).

**Risiken explizit adressiert:**
- `from swarmq.cli._legacy import *` exportiert keine `_`-Präfixe:
  explizite Re-Exports sind Pflicht.
- Stale `__pycache__` aus Vor-Rename: obige Cleanup-Kommandos.
- Editable-Install-RECORD: bei `uv sync`-Neubauen ggf. auch
  `.venv/lib/python*/site-packages/swarmq-*.dist-info` neu generieren.

**Gate:**
- Standard-Gate (siehe oben).
- `uv run python -c "import swarmq.cli; assert swarmq.cli.__file__.endswith('cli/__init__.py')"`
- `uv run python -c "from swarmq.cli import main, run_worker_async"`

### Unit 3 — `_shared.py` extrahieren

**Ziel:** Reine Utility-Funktionen in `cli/_shared.py` ziehen.

**Red:** Test für `_shared.py` direkter Import:
`from swarmq.cli._shared import _positive_int` — schlägt fehl bis
das Modul existiert.

**Green:** Jede Extraktion ist eine **atomare Drei-Schritt-Operation
pro Symbol** — nicht vergessen, beide Seiten zu aktualisieren
(ce-review feasibility #6):
1. **Cut** aus `_legacy.py` (Definition vollständig entfernen).
2. **Paste** in `_shared.py`.
3. **Import** in `_legacy.py` via `from swarmq.cli._shared import
   <name1>, <name2>, …` — am Top, nach stdlib/3rd-party.

Nach jeder Unit ausführen:
```bash
python -c "
import ast
t = ast.parse(open('src/swarmq/cli/_legacy.py').read())
names = [n.name for n in t.body
         if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef,
                           ast.ClassDef))]
assert len(names) == len(set(names)), f'duplicate top-level defs: {names}'
"
```
Verhindert, dass `_legacy.py` sowohl die alte Definition UND einen
Re-Import hält (stumme Doppeldefinition).

**Zielmenge:**
- `_VALID_LOG_LEVELS`, `_VALID_LOG_FORMATS`
- `_positive_int`, `_positive_float`
- `_DOTTED_IDENTIFIER_RE`, `_dotted_module_name`
- `_autoscale_range`
- `_FORBIDDEN_WATCH_PREFIXES`, `_watch_dir_path`
- `_queue_list`
- `_backend_from_env`, `_broker_from_env`
- `_render_table`

`__init__.py`-Fassade wechselt die Re-Export-Quelle für diese Namen
von `_legacy` zu `_shared`.

**Gate:** Standard + `pytest tests/unit/test_cli_multiprocess.py -v`
grün (nutzt `_autoscale_range`, `_watch_dir_path`).

### Unit 4 — `parser.py` extrahieren

**Ziel:** `_build_parser` + `_swarmq_version` in `cli/parser.py`.

**Red:** Import-Test `from swarmq.cli.parser import _build_parser` +
neuer Golden-File-Test `tests/unit/test_cli_help_output.py`:
- Captured `swarmq --help`, `swarmq worker --help`, `swarmq dlq --help`,
  `swarmq schedule --help`, `swarmq dlq inspect --help`, `swarmq dlq
  retry --help`, `swarmq schedule add --help` etc.
- Vergleich gegen `tests/unit/fixtures/cli_help/*.txt` Golden-Files.
- **Snapshot vor Unit 4 erzeugen** (via `pytest
  tests/unit/test_cli_help_output.py --snapshot-create` oder einfach
  Write der Fixtures vor dem Move).

Ersetzt den ursprünglichen "manuellen diff"-Gate — automatisiert
und CI-fähig (ce-review scope #5).

**Green:** Atomare Drei-Schritt-Operation (siehe Unit 3). Zielmenge:
`_build_parser`, `_swarmq_version`.

**Gate:** Standard + Golden-File-Test + `pytest tests/unit/test_cli.py
-v` grün.

### Unit 5 — `worker_cmd.py` + `check_cmd.py` extrahieren

**Ziel:** `run_worker_async` nach `worker_cmd.py`; `_run_worker_check`
+ 4 Helfer nach `check_cmd.py`.

**Red:** Import-Test für beide Module. Zusätzlich: Der Live-Patch-
Through-Assertion aus Unit 1 läuft weiter grün (die Fassade muss
nach dem Move `run_worker_async` so re-exportieren, dass `main()` in
`__init__.py` die gepatchte Referenz sieht).

**Green:** Atomare Drei-Schritt-Operation. **Kritisch:** Da `main()`
in `cli/__init__.py` bleibt (Design Decision), muss die Re-Import-
Reihenfolge in `__init__.py` so sein, dass
`from swarmq.cli.worker_cmd import run_worker_async` **vor** der
`def main(...)`-Definition steht. Dann bindet `main()` den Namen
`run_worker_async` an das Modulattribut — und `patch("swarmq.cli.
run_worker_async", …)` ersetzt dieses Attribut, bevor `main()` es
liest. `monkeypatch`-Tests in `test_process_manager.py` funktionieren
dadurch unverändert.

**Gate:**
- Standard-Gate.
- `pytest tests/unit/test_process_manager.py -v` grün (die drei
  `monkeypatch.setattr("swarmq.cli.run_worker_async", …)`-Tests
  müssen unverändert passieren — Beweis via
  `monkeypatch.setattr` mit `side_effect`-Assertion, die aufschlägt,
  wenn die echte Funktion doch läuft).
- `pytest tests/unit/test_cli.py tests/unit/test_cli_multiprocess.py
  -v` grün.
- Smoke: `swarmq worker --check --module nonexistent.module` liefert
  identisches JSON wie pre-Unit-5 (Byte-Vergleich).

### Unit 6 — `schedule_cmd.py` extrahieren

**Ziel:** Alle `_run_schedule_*` + `_format_schedule_table` in
`cli/schedule_cmd.py`.

**Red:** Import-Test.

**Green:** Atomare Drei-Schritt-Operation (siehe Unit 3).

**Gate:** Standard + `pytest tests/integration/test_cli_schedule.py
-v` grün.

### Unit 7 — `dlq_cmd.py` + `inspect_cmd.py` extrahieren

**Ziel:** DLQ- und Inspect-Handler in ihre Module.

**Red:** Import-Test.

**Green:** Atomare Drei-Schritt-Operation. `_DLQNotFound`,
`_DLQ_NOT_FOUND_EXIT_CODE`, `_normalise_headers_for_json` ziehen mit
`dlq_cmd.py` mit.

**Gate:** Standard + `pytest tests/e2e/test_dlq_cli.py -v` grün.

### Unit 8 — `_legacy.py` entfernen + Endzustand

**Ziel:** `_legacy.py` ist nach Units 3–7 nur noch eine Re-Import-
Hülle. Diese Unit löscht sie und konsolidiert die Fassade.

**Red:** `pytest tests/ -x` komplett grün **vor** der Löschung.
Anschließend:
```bash
python -c "import ast; t = ast.parse(open('src/swarmq/cli/_legacy.py').read()); assert not [n for n in t.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))], '_legacy.py must contain only re-imports before deletion'"
```
Beweis, dass `_legacy.py` vor der Löschung tatsächlich leer an
eigener Logik ist.

**Green:**
- `_legacy.py` löschen.
- `__init__.py` neu aufbauen: alle Re-Exports direkt aus den neuen
  Modulen (`_shared`, `parser`, `worker_cmd`, `schedule_cmd`,
  `dlq_cmd`, `inspect_cmd`, `check_cmd`). `main()` bleibt in
  `__init__.py` — nicht bewegen.
- Full sweep: `grep -r "swarmq.cli._legacy" src tests` muss leer
  sein.

**Gate:**
- `pytest tests/ -x` komplett grün (Unit- + Integration- + E2E-Tier).
- `pytest tests/unit/test_cli_module_surface.py -v` — **Equality +
  Identity + Live-Patch + Subprocess-Import** aus Unit 1, alle grün.
- `ruff check src/swarmq/cli/` grün.
- `mypy src/swarmq/` kein neuer Fehler (Legacy-Modules bleiben in
  `ignore_errors=true`).
- Manuelle CLI-Smoke-Tests: `swarmq --version`, `swarmq worker
  --check --module swarmq`, `swarmq dlq list`, `swarmq schedule
  list` — alle identisches Verhalten.
- Dokumentations-Build: `uv run mkdocs build --strict` grün.

### Unit 9 — Whitelist-Wartung dokumentieren

**Ziel:** Wartungs-Runbook für künftige Surface-Änderungen festhalten.

**Red:** Kein Test. Diese Unit ist Dokumentations-Only.

**Green:** In `tests/unit/test_cli_module_surface.py` einen
Docstring-Header einfügen, der erklärt:
- Warum die Whitelist eingefroren ist (Link zu
  `process_manager.py:754` und den `patch(…)`-Test-Sites).
- Wann die Whitelist aktualisiert werden darf (neuer öffentlicher
  Export MIT passendem Test-Konsumenten — nicht "weil ruff
  `F401`-Fehler meldet").
- Wie man die Whitelist regeneriert:
  `python -c "import swarmq.cli; print(sorted(n for n in dir(swarmq.cli) if not n.startswith('__')))"`

**Gate:** `pytest tests/unit/test_cli_module_surface.py -v` grün;
Docstring ist vorhanden (`grep -q "Whitelist-Wartung" tests/unit/
test_cli_module_surface.py`).

## Risk / Rollback

| Risiko | Wahrscheinlichkeit | Schaden | Mitigation |
|---|---|---|---|
| Zirkulärer Import bricht (`process_manager` ↔ `cli`) | Mittel | Worker startet nicht | Unit 5 Gate: `test_process_manager.py` grün. Fassade hält Symbol-Pfad stabil. |
| Test-Patches brechen stumm (`patch("swarmq.cli.X")` geht ins Leere) | Hoch → niedrig | Tests grün, patchen aber nichts Echtes | **main() bleibt in `cli/__init__.py`** — damit liegen alle Patch-Ziele und alle Call-Sites im selben Modul-Namespace. Unit-1-Live-Patch-Through-Test beweist es pro Commit. (Ursprünglich P0-Finding im Review.) |
| argparse-Help-Text ändert sich byte-subtil (Modul-Docstrings im Help) | Niedrig | User-sichtbare UX-Regression | Unit 4 Gate: Golden-File-Test `test_cli_help_output.py` (automatisiert, nicht manuell). |
| Ruff/mypy findet Probleme die in `cli.py` maskiert waren | Niedrig | Delay | Legacy-`ignore_errors=true`-Bucket wandert mit: sowohl `"swarmq.cli"` als auch `"swarmq.cli.*"` in pyproject.toml (Unit 2). |
| Test-Reihenfolge-Abhängigkeit durch `sys.modules`-Caching beim Reimport | Niedrig | Flaky Tests | Unit-1-Subprocess-Import-Assertion fängt Zirkular-Init-Probleme, die im warm geladenen pytest-Prozess versteckt sind. Stale bytecode per Cleanup-Skript in Unit 2 entfernt. |
| Editable-Install-Cache (`.dist-info/RECORD`) zeigt auf alte `cli.py` | Niedrig | `swarmq`-Konsolen-Skript startet kaputte Version | Nach Unit 2: `uv sync --reinstall-package swarmq` oder `rm -rf .venv && uv sync`. |

**Rollback:** Jede Unit ist ein eigener Commit. `git revert <sha>`
pro Unit; die Struktur ist so gewählt, dass Unit N-1 eigenständig
funktioniert. Worst-Case-Revert: bis Unit 1 zurück, `cli.py`
unberührt.

## Verification

**Pre-Merge-Gate (alle grün):**
1. `uv run --extra test pytest tests/unit -q` (< 30s, lokaler Dev-Loop)
2. `uv run --extra test pytest tests/integration -q` (mit docker compose up)
3. `uv run --extra test pytest tests/e2e -q`
4. `uv run ruff check src/swarmq/cli/`
5. `uv run mypy src/swarmq/` (kein neuer Fehler)
6. Manuelle Smoke-Test-Matrix:
   - `swarmq --version`
   - `swarmq --help`
   - `swarmq worker --help`
   - `swarmq worker --module <existierendes Testmodul> --check`
   - `swarmq worker --module nonexistent.module --check` (erwartet JSON error)
   - `swarmq dlq list`
   - `swarmq schedule list`
   - `swarmq schedule status`
   - `swarmq inspect`
7. Diff-Size-Check: Sum of moves approximately gleicht aus
   (`git diff --stat` zeigt Move-Pattern).

**Post-Merge:**
- Ein Follow-Up-Issue für den orthogonalen Type-Debt-Abbau
  (`swarmq.cli.*` Module schrittweise in mypy-strict überführen).
- Ein Follow-Up-Issue für die echte Entkopplung
  `process_manager` ↔ `cli` (Event-Dispatcher-Pattern statt
  direkten Import).

## Ausgesparte Entscheidungen (explizit offen gelassen)

- **`cli/` oder `_cli/`?** → `cli/` (öffentlicher Namespace; Tests
  importieren `swarmq.cli`).
- **Subcommand-Dispatcher-Table?** → Nein. `main()` bleibt prozedural
  wie heute. Ein Dispatcher-Dict wäre eine zweite Änderung; dieser
  Plan ist pure Move-Operation.
- **Ein Modul pro `_run_dlq_*`?** → Nein. Granularität ist
  Command-Family, nicht einzelner Handler (Design Decision #1).
- **Type-Hints aufrüsten?** → Nein, separater Pass. Jede neue Zeile
  erbt den heutigen Typ-Status.

## Offene Entscheidungen aus dem CE-Review

Diese Punkte sind nicht durch Auto-Fixes geklärt — Georg entscheidet
vor Implementierungsstart:

- **Big-Bang vs. `_legacy.py`-Shim über 6 Units.** scope-guardian
  argumentiert, ein einziger Move-Commit sei bei Test-Gesamtlaufzeit
  <30s risiko-äquivalent und spare 6 Einheiten Parallel-Maintenance.
  Der aktuelle Plan behält den Shim — tragbar, wenn jede Unit ein
  eigener PR werden soll. Für einen einzigen lokalen Commit-Stream ist
  Big-Bang plausibler.
- **`check_cmd.py` als eigenes Modul vs. in `__init__.py` neben
  `main()`.** scope-guardian notiert: alle fünf Check-Symbole haben
  nur einen Aufrufer (`main()`). Eigenes Modul hält den JSON-Contract-
  Perimeter sichtbar (Design Decision #5). In `__init__.py` zu
  integrieren spart eine Datei, koppelt aber Check-Logik und Haupt-
  Dispatch im gleichen Namespace.
- **Public-API-Freeze für Underscore-Symbole.** adversarial weist
  darauf hin, dass der Audit-Test (Unit 1) 30+ privat-präfigierte
  Symbole als de-facto öffentliche API einfriert, sobald er grün ist.
  Alternativ: Whitelist-Einträge tagen (`# internal, may be moved —
  mirror in submodule`) und in künftigen Plans bewusst deprecaten.
- **Problem-Frame-Quantifizierung.** Der Plan nennt "die letzten
  sechs Monate" als Rechtfertigung. `git log --since="6 months ago"
  --oneline -- src/swarmq/cli.py | wc -l` einmal vor Start messen,
  damit die Motivation über Zeit nachvollziehbar bleibt (nur wenn
  gewünscht).
