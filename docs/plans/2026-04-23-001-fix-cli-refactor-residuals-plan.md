---
title: "Fix cli-refactor residuals — ce-review Findings aus b961f75 / 52e96c8"
status: ready
execution_mode: incremental
quantification:
  baseline_unit_tests: 1204 passed / 1 skipped (post 52e96c8)
  open_findings: 7 (zu fixen) + 6 (won't-fix, explizit dokumentiert)
  swarmq_help_latency_baseline_ms: ~155 (CLI-01 Eager-Imports)
created: 2026-04-23
reviewed: 2026-04-23 (document-review v1 — 11 auto-fixes + 8 decisions)
depends_on:
  - refactor/cli-split (Commits b961f75 + 52e96c8) — Zielstruktur steht
  - ce-review Run 20260422-192644-bba2a351 — Finding-Quelle
---

## Overview

Der CLI-Split auf `refactor/cli-split` ist test-grün abgeschlossen, aber
der ce-review (Run `20260422-192644-bba2a351`) hat 13 offene Findings
zurückgestellt — teils pre-existing Alt-Bugs, die erst durch die neue
Seam **gut fixbar** werden, teils echte Quality-Polish-Kandidaten, teils
bewusste Design-Entscheidungen. Dieser Plan adressiert die sieben
fixbaren Findings in **sieben unabhängigen Units** nach TDD-Disziplin
und dokumentiert die sechs explizit verworfenen Findings als
Non-Goals mit Begründung.

**Ziel:** `refactor/cli-split` → Merge-readiness ohne Alt-Bugs mehr als
"pre-existing" durchzureichen. Jede Unit ist ein eigener Commit mit
RED-GREEN-REFACTOR-Spur im Git-Log (kein Big-Bang).

**Nicht-Ziele:** Keine neuen CLI-Flags oder Subcommands, kein
Rewrite der Scheduler-Leader-Semantik, keine Typ-Debt-Abbau-Runden.
**User-visible Default-Changes sind zulässig, wenn sie per Env-Var
reversibel sind und im Changelog als Behaviour-Change markiert werden**
(trifft auf Unit 5 / CLI-04 zu).

## Problem Frame

1. **Alt-Bugs, die der Refactor sichtbar gemacht hat:** `CLI-01` (Eager-
   Imports in `_shared.py` → 155 ms `--help`-Regression seit Längerem),
   `CLI-02` (`--check` in `dlq retry worker --check` false-positive),
   `CLI-03` (ANSI-Farben ohne TTY-Guard), `KP-5` (`schedule status`
   greift auf `backend._client` privat zu) sind pre-existing, aber
   **orthogonal** zum Refactor selbst. Sie jetzt zu fixen verhindert,
   dass sie beim nächsten Refactor erneut als "pre-existing" dokumentiert
   werden und kompoundieren.

2. **Refactor-induziertes Rest-Risiko:** `TestLivePatchThrough` prüft
   nur den Worker-Dispatch. Für `schedule`/`dlq`/`inspect` gibt es
   kein Live-Patch-Pendant — ein zukünftiger Split, der diese Namen
   in Submodul-Namespaces verschiebt, würde erst bei Integrations-
   tests kracht. Diese Lücke ist **durch** den Refactor entstanden
   (vorher gab es keinen Live-Patch-Test überhaupt) und muss
   geschlossen werden.

3. **Quality-Polish ohne Verhaltensänderung:** `CLI-04` (TTY-aware
   `--format`-Default) und `M-04` (`cli/__init__.py`-Docstring kürzen)
   sind kleine Verbesserungen, die agent-readability bzw.
   Navigierbarkeit erhöhen. Kein funktionaler Eingriff.

4. **Bewusste Non-Goals:** Sechs Findings bleiben **explizit
   unadressiert** — `PS-01` (big-bang TDD-Abweichung, akzeptiert),
   `PS-02` (`main()` im Facade, by design für Patch-Semantik),
   `KP-2` (Rename `_DLQNotFound` wäre API-Break), `KP-7` (`match`
   braucht 3.14+), `CLI-05` (Burst-Exit-Code-Ambiguität, dokumentierte
   Limitierung), `M-02`/`M-03` (Surface-Whitelist bleibt gefroren
   bis externer Consumer ausgeschlossen). Dieser Plan dokumentiert
   die Begründungen, damit der nächste ce-review sie nicht erneut
   als offen flaggt.

## Requirements Trace

| ID  | Anforderung | Quelle | Satisfied by |
|-----|-------------|--------|--------------|
| R-1a | Die CLI-Module selbst triggern keine direkten Heavy-Imports mehr: `src/swarmq/cli/_shared.py` hat keine Top-Level-`import valkey\|aio_pika` mehr (Imports kapseln sich in `_backend_from_env` / `_broker_from_env`); `src/swarmq/cli/__init__.py` stellt `ValkeyBackend` / `RabbitMQBroker` via `__getattr__`-Lazy-Pattern bereit. Identity-Check bleibt erhalten (`cli.ValkeyBackend is swarmq.backend.valkey.ValkeyBackend`). | CLI-01 | Unit 1 |
| R-1b | Vollständige Import-Latency-Reduktion (≥ 60 % Median-Reduktion, kein `valkey`/`aio_pika` in `sys.modules` nach `import swarmq.cli`). **Nicht in diesem Plan** — Pre-Impl-Audit (2026-04-23) zeigte, dass `swarmq/app.py` und `swarmq/client.py` Type-Annotations via eager Imports halten. Delegiert an Follow-up-Plan `2026-04-23-002-fix-import-laziness-plan.md`. | CLI-01 | **Follow-up-Plan** |
| R-2 | Erkennung "Worker-Check-Invocation" passiert **nach** `argparse.parse_args()` auf dem geparsten `Namespace` — nicht mehr per argv-String-Scan. `argv` `swarmq dlq retry worker --check` führt zu `args.subcommand == "dlq"` und erreicht den Check-Pfad nicht. | CLI-02 | Unit 2 |
| R-3 | `structlog.dev.ConsoleRenderer` wird mit `colors=sys.stderr.isatty()` konfiguriert, Wert zum `setup_logging`-Aufrufzeitpunkt gebunden. Kein ANSI-Output mehr bei gepipter stderr. **Statischer Check ist akzeptiert** (siehe Decision E5); pytest-capsys-Regressionstest schützt gegen versehentliches ANSI in Test-Capture. | CLI-03 | Unit 3 |
| R-4 | `ValkeyBackend` exponiert `async def get_scheduler_leader() -> str \| None` als öffentliche Methode, die `ValkeyConnectionError`/`ValkeyTimeoutError` **propagiert** (siehe Decision E4). `schedule_cmd._run_schedule_status` nutzt sie; `backend._client.get(SCHEDULER_LOCK_KEY)` verschwindet aus `src/swarmq/cli/`. | KP-5 + Rest-Risiko #1 | Unit 4 |
| R-5 | `--format` defaultet auf `json` wenn `not sys.stdout.isatty()`, sonst auf `table`. Escape-Hatch: Env-Var `SWARMQ_DEFAULT_FORMAT=table\|json` überschreibt den TTY-basierten Default (siehe Decision E2). Explizites `--format=…` gewinnt immer. | CLI-04 | Unit 5 |
| R-6 | Live-Patch-Tests existieren zusätzlich für `schedule`, `dlq`, `inspect` — analog zur bestehenden Worker-Variante. Jeder testet, dass `patch("swarmq.cli.<symbol>")` die Bindung erreicht, die `main()` tatsächlich aufruft. | Rest-Risiko #2 | Unit 6 |
| R-7 | `cli/__init__.py`-Docstring auf den load-bearing "warum `main()` hier lebt"-Block reduziert. Keine eigenen Tests — Code-Review ist das Gate (siehe Decision E8). | M-04 | Unit 7 |
| R-8 | Keine Regression: `tests/unit/test_cli_module_surface.py` und `tests/unit/test_cli_help_output.py` bleiben unverändert grün. Surface-Whitelist wird **nicht** angetastet. | ce-review Gate | Alle Units |
| R-9 | Jede Unit ist ein eigener Commit im Format `fix(cli-residuals): <ID> — <kurzbeschreibung>` mit vorangehendem RED-Commit `test(cli-residuals): <ID> — reproduce`. | AGENTS.md TDD | Alle Units |

## Scope Boundaries

**In Scope:**
- `src/swarmq/cli/_shared.py` — Lazy-Importe für `ValkeyBackend`/`RabbitMQBroker`.
- `src/swarmq/cli/__init__.py` — (a) ambient re-exports von `ValkeyBackend`/`RabbitMQBroker` auf `__getattr__`-lazy umstellen (Unit 1, siehe Decision E1); (b) Docstring-Trim (Unit 7).
- `src/swarmq/cli/check_cmd.py` — `_is_check_invocation` wird durch Post-argparse-Check ersetzt (Decision E3).
- `src/swarmq/cli/__init__.py::main` — Dispatch-Logik ruft den Check-Pfad auf Basis des geparsten `Namespace` auf statt per Pre-Parse-argv-Scan.
- `src/swarmq/logging.py::setup_logging` — TTY-Guard für `ConsoleRenderer`.
- `src/swarmq/backend/valkey.py` — neue `async get_scheduler_leader`-Methode.
- `src/swarmq/cli/schedule_cmd.py` — Nutzung der neuen API.
- `src/swarmq/cli/parser.py` — `--format default=None` an allen ~10 `add_argument`-Sites; Help-Text dokumentiert TTY-Default.
- `src/swarmq/cli/_shared.py` — `resolve_format`-Helper mit Env-Var-Escape (`SWARMQ_DEFAULT_FORMAT`).
- `tests/unit/test_cli_residuals.py` (neu) — alle neuen Tests gebündelt.
- Erweiterung `tests/unit/test_cli_module_surface.py::TestLivePatchThrough` (Unit 6) — inklusive der ge-skippten Mutation-Demo.
- `tests/unit/test_cli_module_surface.py::_CLI_PUBLIC_SURFACE` Whitelist — `ValkeyBackend`/`RabbitMQBroker` bleiben als Surface-Einträge, aber mit Identity-Check via `cli.__getattr__` statt Top-Level-Binding.
- `tests/unit/test_cli_help_output.py` — Golden-File-Snapshots regeneriert in Unit 5.
- Changelog-Datei(en): `website/reference/changelog.md` (plus lokalisierte Varianten) — CLI-04-Behaviour-Change-Eintrag in Unit 5.

**Out of Scope (bewusst):**
- Rename `_DLQNotFound` → `_DLQNotFoundError` (KP-2, API-Break — mit Sunset-Datum 2026-Q4; siehe Decision E6).
- `if/elif` → `match` (KP-7, Stil-Präferenz; siehe Auto-Fix im Won't-Fix-Register).
- `main()` aus `__init__.py` extrahieren (PS-02, by design — validiert durch Unit 6).
- **Vollständiger** Surface-Whitelist-Trim (M-02/M-03, bis auf die beiden Heavy-Import-Einträge, die in Unit 1 auf lazy umgestellt werden; Sunset 2026-Q4).
- Burst-Exit-Code-Disambiguation (CLI-05, dokumentierte Limitation).
- PS-01 retroaktiv — wir haben uns gegen Git-Rewrite entschieden, weil der Branch test-validiert ist und Split-Rebase-Konflikt-Risiko den Nutzen nicht rechtfertigt. Nicht "unmöglich", sondern **bewusst akzeptierte Einmal-Ausnahme** (siehe Auto-Fix im Won't-Fix-Register).
- Breite mypy-Cleanups in `cli.*` (bleibt Legacy-Bucket).
- Änderungen an `process_manager.py` / Hot-Reload.
- Performance-Benchmarks jenseits der R-1-Messung.

## Target Changes Summary

```
src/swarmq/
  cli/
    __init__.py               # Docstring-Trim (R-7)
    _shared.py                # Lazy-Imports (R-1), --format-Resolver (R-5)
    check_cmd.py              # _is_check_invocation strict-positional (R-2)
    schedule_cmd.py           # get_scheduler_leader() call (R-4)
    parser.py                 # --format default=None an ALLEN add_argument-Sites (R-5)
  backend/valkey.py           # get_scheduler_leader() public async (R-4)
  logging.py                  # setup_logging: TTY-Guard für ConsoleRenderer (R-3)

tests/unit/
  test_cli_residuals.py       # neu — alle Finding-spezifischen Tests
  test_cli_module_surface.py  # TestLivePatchThrough erweitert (R-6)
```

## Units

> **Reihenfolge-Strategie:** Units sind unabhängig außer **Unit 4**
> (braucht `ValkeyBackend.get_scheduler_leader` → Unit 4a vor 4b).
> Empfohlene Abarbeitung: 1 → 2 → 3 → 4 → 5 → 6 → 7.
> Jede Unit = 1 RED-Commit + 1 GREEN-Commit (AGENTS.md TDD).

---

### Unit 1 — CLI-01: Lazy-Imports in den CLI-Modulen (R-1a)

**Problem (CLI-lokal):** Zwei Stellen in `src/swarmq/cli/*` ziehen
`valkey`/`aio_pika` eager:
1. `_shared.py:16-17` — Top-Level `from swarmq.backend.valkey import
   ValkeyBackend` / `from swarmq.broker.rabbitmq import RabbitMQBroker`,
   die nur in `_backend_from_env` / `_broker_from_env` gebraucht werden.
2. `cli/__init__.py:63-64` — ambient re-exports derselben Symbole für
   die Surface-Whitelist (M-02/M-03).

**Audit-Erkenntnis (2026-04-23):** `swarmq/app.py:8-9` und
`swarmq/client.py:15-16` halten dieselben Symbole als **Type-
Annotations** top-level. Solange diese bleiben, lädt `import swarmq.cli`
transitiv (über `cli/__init__.py:62 from swarmq.app import SwarmQ`)
immer noch `valkey`/`aio_pika`. **Diese globale Laziness ist Scope
des Follow-up-Plans** `2026-04-23-002-fix-import-laziness-plan.md`.
Unit 1 hier erledigt **nur den CLI-lokalen Teil** — notwendig, aber
nicht hinreichend für die volle Latency-Reduktion. R-1a ist die
Teil-Zielgröße dieses Plans.

**Pre-Implementation-Audit:** Ausgeführt 2026-04-23. Artefakt:
`.context/cli-residuals/pre-impl-import-audit.txt` (gitignored).
Baseline: 152 ms Median, `valkey` + `aio_pika` in `sys.modules`.

**RED (scope-korrigiert):**
```
tests/unit/test_cli_residuals.py::TestLazyImports
  - test_cli_shared_module_has_no_top_level_valkey_import
  - test_cli_shared_module_has_no_top_level_aio_pika_import
  - test_cli_init_module_has_no_top_level_heavy_imports
  - test_cli_getattr_valkey_backend_preserves_identity
  - test_cli_getattr_rabbitmq_broker_preserves_identity
  - test_cli_getattr_unknown_attribute_raises
```
Assertion-Muster für R-1a: Per AST-Scan von `src/swarmq/cli/_shared.py`
und `src/swarmq/cli/__init__.py` prüfen, dass keine Top-Level-
`ImportFrom`-Nodes mehr `swarmq.backend.valkey` oder
`swarmq.broker.rabbitmq` referenzieren. Für die Identity-Checks:
`cli.ValkeyBackend is swarmq.backend.valkey.ValkeyBackend`.

**Bewusst nicht in diesem Unit:** `sys.modules`-basierte Assertions
(`"valkey" not in sys.modules`) — die würden erst nach
`fix/import-laziness` grün; sie landen im Follow-up-Plan.

**GREEN:**
- In `_shared.py`: Top-Level-`from swarmq.backend.valkey import ValkeyBackend`
  und `from swarmq.broker.rabbitmq import RabbitMQBroker` streichen;
  `_broker_from_env` / `_backend_from_env` kapseln die Imports
  **innerhalb** ihres Funktionsbodies (Pattern analog
  `dlq_cmd._get_broker_factory`).
- In `cli/__init__.py`: Die beiden Symbole aus der Top-Level-Import-Liste
  entfernen und per Modul-`__getattr__` lazy nachreichen:
  ```python
  _LAZY_SURFACE = {
      "ValkeyBackend": ("swarmq.backend.valkey", "ValkeyBackend"),
      "RabbitMQBroker": ("swarmq.broker.rabbitmq", "RabbitMQBroker"),
  }

  def __getattr__(name: str) -> object:
      if name in _LAZY_SURFACE:
          module, attr = _LAZY_SURFACE[name]
          import importlib
          return getattr(importlib.import_module(module), attr)
      raise AttributeError(f"module 'swarmq.cli' has no attribute {name!r}")
  ```
- `_CLI_PUBLIC_SURFACE`-Whitelist in `test_cli_module_surface.py`:
  Einträge markieren als `# lazy via __getattr__` — Identity-Check
  bleibt erhalten (`cli.ValkeyBackend is swarmq.backend.valkey.ValkeyBackend`).

**Gates:**
- Neue Tests (6) grün.
- **Bestehende `test_cli_module_surface.py`-Identity-Checks bleiben grün**
  (R-8) — das Pattern ist der Beweis, dass "surface erhalten" und
  "lazy" kompatibel sind.
- `test_cli_help_output.py` grün.
- AST-Check: `grep "from swarmq.backend\\|from swarmq.broker" src/swarmq/cli/_shared.py src/swarmq/cli/__init__.py` zeigt nur Imports innerhalb von Funktionsbodies (nicht Top-Level).

**Nicht-Gate dieses Units:** Import-Latency-Messung — die kommt erst
mit dem Follow-up-Plan zum Tragen.

**Interaktion mit M-02/M-03:** Die zwei Whitelist-Einträge
`ValkeyBackend`/`RabbitMQBroker` werden **partiell re-öffnet** — sie
bleiben Surface (Identity-erhaltend), aber nicht mehr eager. Die
restlichen ambient re-exports (Path, Sequence, etc.) bleiben Won't-Fix.

**Risiko:** Gering. `__getattr__` ist PEP-562, gut unterstützt.
AST-basierte Tests sind deterministisch.

---

### Unit 2 — CLI-02: Check-Erkennung nach `argparse` (R-2)

**Problem:** Aktuelle Implementierung scannt `argv` als String-Liste
**vor** `parse_args()` und sucht `--check` als Token. Das scheitert an:
- `swarmq dlq retry worker --check` → `"worker"` + `"--check"` beide
  in `argv` → False-Positive.
- `swarmq worker --task --check` → `--check` ist der **Wert** von
  `--task`, kein eigenes Flag — ohne argparse-Spec-Wissen nicht
  unterscheidbar.

**Entscheidung (E3):** Pre-parse-Hack abschaffen, Check-Erkennung nach
`parser.parse_args()` verschieben. argparse kennt die Spec und macht
die Unterscheidung gratis korrekt.

**RED:**
```
tests/unit/test_cli_residuals.py::TestCheckDetectionPostParse
  - test_worker_with_check_flag_enters_check_path
  - test_dlq_retry_worker_check_does_not_enter_check_path
  - test_worker_task_check_as_task_name_does_not_enter_check_path
  - test_worker_without_check_enters_normal_path
  - test_schedule_list_never_enters_check_path
```

**GREEN:**
1. `_is_check_invocation(argv)` ersatzlos streichen **oder** als
   Thin-Wrapper auf der geparsten `Namespace`-Ebene neu definieren:
   ```python
   def _is_check_invocation(args: argparse.Namespace) -> bool:
       return getattr(args, "subcommand", None) == "worker" \
              and getattr(args, "check", False)
   ```
2. `main()` in `cli/__init__.py`: `parse_args()` zuerst, dann
   Namespace-basierter Check-Dispatch. Die bisherige Fallback-Logik
   für `_emit_argparse_check_error` (die auf argparse-Fehler bei
   fehlendem `--task` reagiert) muss in den Check-Handler selbst
   wandern — wird aktiv, wenn `args.subcommand == "worker" and
   args.check and not args.task`.

**Gates:**
- Neue Tests grün.
- Bestehende `--check`-Tests (`tests/unit/test_cli.py` +
  `tests/e2e/test_dlq_cli.py`) bleiben grün.
- Grep-Check: `grep -n '_is_check_invocation' src/swarmq/cli/` darf
  nur noch die neue Namespace-Signatur zeigen.

**Risiko:** Mittel — der Check-Pfad ist JSON-contract-bound und der
argparse-Error-Handling-Pfad (`_emit_argparse_check_error`) hat
Sonderverhalten. Extra-Test: `swarmq worker --check` ohne `--task`
emittiert weiterhin die JSON-Fehlerstruktur, nicht das argparse-
Default-Error-Format.

---

### Unit 3 — CLI-03: TTY-Guard für `ConsoleRenderer` (R-3)

**Problem:** `structlog.dev.ConsoleRenderer()` wird ohne `colors`-
Argument instanziiert. Default ist `colors=True` ungeachtet TTY-Status.

**Entscheidung (E5):** Statischer TTY-Check bei `setup_logging`-Aufruf.
Lazy-per-Record-Check wird nicht gebaut — matcht 99 %-Fall, hält den
Code simpel. capsys-Regression wird durch einen dedizierten Test
abgesichert.

**RED:**
```
tests/unit/test_cli_residuals.py::TestConsoleRendererTTY
  - test_no_ansi_escape_when_stderr_piped
  - test_ansi_escape_when_stderr_tty (skip-if-no-tty-fixture)
  - test_setup_logging_respects_capsys_redirect  # capsys-Regression-Guard
```
- `test_no_ansi_escape_when_stderr_piped`: subprocess mit
  `stderr=PIPE`, ruft eine Task auf, die structlog benutzt, asserted
  dass kein `\x1b[` im captured stderr auftaucht.
- `test_setup_logging_respects_capsys_redirect`: Ruft `setup_logging()`
  **innerhalb** der Test-Funktion nach `capsys`-Aktivierung auf
  (re-initialisiert den Renderer); asserted `"\x1b[" not in capsys.readouterr().err`.
  Dokumentiert die Invariante: "Wenn Caller stderr umleitet, muss
  `setup_logging` erneut aufgerufen werden."

**GREEN:** `ConsoleRenderer(colors=sys.stderr.isatty())` in
`setup_logging` (`src/swarmq/logging.py:27`). Bei `log_json=True`
bleibt der Pfad unverändert (kein `ConsoleRenderer`).

**Gates:**
- Neue Tests grün.
- Keine Regression in `tests/integration/test_cli_*.py`.

**Risiko:** Gering. User-visible Change nur bei **tatsächlichem**
TTY — bei gepipter Ausgabe ist das Verhalten objektiv korrekter.

---

### Unit 4 — KP-5 + Rest-Risiko #1: `get_scheduler_leader()` public (R-4)

**Problem:** `_run_schedule_status` greift direkt auf
`backend._client.get(SCHEDULER_LOCK_KEY)` zu. Bricht bei jedem
`ValkeyBackend`-Internal-Refactor stumm (AttributeError zur Laufzeit).

**Entscheidung (E4):** `ValkeyConnectionError` und `ValkeyTimeoutError`
werden **propagiert** — aktuelles Verhalten der direkten
`_client.get(SCHEDULER_LOCK_KEY)`-Zeile bleibt erhalten. Kein Swallow.
Ops-Visibility bei Backend-Ausfall bleibt intakt.

**Unit 4a (RED + GREEN): Backend-API**
```
tests/unit/test_valkey_backend.py::TestSchedulerLeader
  - test_get_scheduler_leader_returns_holder_when_locked
  - test_get_scheduler_leader_returns_none_when_unlocked
  - test_get_scheduler_leader_propagates_connection_error
  - test_get_scheduler_leader_propagates_timeout_error
```
`ValkeyBackend.get_scheduler_leader()` MUSS `async` sein, weil der
gesamte `self._client.*`-Layer awaitable ist (`backend/valkey.py:125 ff.`:
`async def get_result`, `get_progress`, etc.):
```python
async def get_scheduler_leader(self) -> str | None:
    """Return current scheduler-lock holder or None if unlocked.

    Propagates ValkeyConnectionError/ValkeyTimeoutError so callers can
    distinguish 'no leader' from 'backend unreachable'.
    """
    raw = await self._client.get(SCHEDULER_LOCK_KEY)
    if raw is None:
        return None
    return raw.decode() if isinstance(raw, bytes) else str(raw)
```

**Unit 4b (RED + GREEN): CLI-Konsumption**
```
tests/unit/test_cli_residuals.py::TestScheduleStatusUsesPublicAPI
  - test_schedule_status_calls_get_scheduler_leader
  - test_schedule_status_does_not_touch_private_client
  - test_schedule_status_exits_nonzero_on_connection_error
```
`_run_schedule_status` ruft `await backend.get_scheduler_leader()` statt
`await backend._client.get(SCHEDULER_LOCK_KEY)`. Bei
`ValkeyConnectionError` propagiert die Exception bis zum
CLI-Top-Level-Handler und führt zu Non-Zero-Exit mit stderr-Meldung
— unverändertes Verhalten gegenüber heute.

**Gates:**
- Beide neuen Test-Klassen grün.
- `tests/integration/test_cli_schedule.py` unverändert grün.
- Grep-Check im CI: `git grep "_client.get(SCHEDULER_LOCK_KEY)"`
  returns nothing unter `src/swarmq/cli/`.

**Risiko:** Mittel — Backend-API-Erweiterung. Neue Methode muss
threadsafe mit bestehender Lock-Semantik des Schedulers bleiben
(read-only, kein TOCTOU-Risiko).

---

### Unit 5 — CLI-04: TTY-aware `--format`-Default (R-5)

**Problem:** `--format` defaultet auf `table` auch bei
`swarmq dlq list | jq`. Agents bekommen eine Tabelle, die sie
parsen müssen.

**Entscheidung (E2):** Unit 5 bleibt im Plan — ist echtes UX-Improvement
für Agents — aber mit **Env-Var-Escape-Hatch**, um Legacy-Shell-Skripte
zu schützen, die bisher auf Table-on-Pipe gebaut haben. Non-Goals-
Wording oben wurde angepasst (Default-Changes sind ok, wenn reversibel
und im Changelog).

**RED:**
```
tests/unit/test_cli_residuals.py::TestFormatDefaultTTY
  - test_default_format_is_json_when_stdout_piped
  - test_default_format_is_table_when_stdout_tty
  - test_explicit_format_table_wins_over_pipe
  - test_explicit_format_json_wins_over_tty
  - test_env_var_override_forces_table_on_pipe
  - test_env_var_override_forces_json_on_tty
  - test_env_var_invalid_value_falls_back_to_tty_default
```
E2E-Ergänzung:
`tests/e2e/test_dlq_cli.py::test_dlq_list_json_on_pipe` —
`swarmq dlq list` in subprocess mit `stdout=PIPE` emittiert
parseable JSON.

**GREEN:**
- In `parser.py` **jede** `add_argument("--format", ...)`-Stelle
  (~10 Vorkommen: `dlq list`, `dlq peek`, `schedule list`, `schedule status`,
  `schedule add`, `schedule remove`, `inspect` u.a.) bekommt
  `default=None`. Help-Text:
  `default: table (TTY) / json (pipe); override via SWARMQ_DEFAULT_FORMAT=table|json`.
- In **jedem** Handler, der `args.format` liest, wird der Wert über
  einen Helper in `_shared.py` aufgelöst:
  ```python
  import os, sys
  _VALID_FORMATS = {"table", "json"}

  def resolve_format(explicit: str | None) -> str:
      if explicit is not None:
          return explicit
      override = os.environ.get("SWARMQ_DEFAULT_FORMAT")
      if override in _VALID_FORMATS:
          return override
      return "json" if not sys.stdout.isatty() else "table"
  ```
  Ungültige Env-Var-Werte werden still ignoriert (Fallback auf TTY-
  Default), nicht als Fehler gemeldet — Consumer-Skripte sollen bei
  Typo nicht crashen.
- Help-Text aller `--format`-Flags regeneriert; Snapshot-Update in
  `tests/unit/test_cli_help_output.py`.

**Gates:**
- Alle neuen Tests grün.
- `test_cli_help_output.py` — Golden-File regeneriert (Help-Text
  ändert sich). Commit enthält die regenerierten Snapshots.
- Changelog-Eintrag unter "Behaviour Changes" ergänzt mit Escape-
  Hatch-Hinweis.

**Risiko:** Mittel — User-visible Default-Change. Mitigation:
1. Explizites `--format=…` funktioniert weiter.
2. `SWARMQ_DEFAULT_FORMAT=table` stellt Alt-Verhalten wieder her.
3. Changelog-Eintrag prominent unter "Behaviour Changes".

**CI-Consideration:** CI-Umgebungen mit TTY auf stdout (Docker `-t`,
GHA `tty: true`) bekommen den Table-Default. Das ist gewollt —
stdout-ist-TTY ist die beste verfügbare Heuristik für "human
consumer". Wer JSON in CI will, setzt `SWARMQ_DEFAULT_FORMAT=json`
oder das explizite Flag.

---

### Unit 6 — Rest-Risiko #2: Live-Patch-Tests für alle Pfade (R-6)

**Problem:** `TestLivePatchThrough` deckt nur `run_worker_async` ab.
Für `_run_schedule_async`, `_run_dlq_async`, `_run_inspect_async`
existiert kein Pendant. Ein naiver Submodul-Verschieber würde
`monkeypatch.setattr(cli, "_run_schedule_async", fake)` stumm
wirkungslos machen und trotzdem grüne Suite bekommen.

**Entscheidung (E7):** Die Mutation-Demo wird **committed** als
`@pytest.mark.skip`-Test mit Docstring. Das verhindert, dass die
Invariante nur self-reported bleibt und jederzeit reproduzierbar ist.

**RED + GREEN (zusammen, weil reine Test-Erweiterung):**
```
tests/unit/test_cli_module_surface.py::TestLivePatchThrough
  - test_patch_run_worker_async_reaches_main_dispatch  (existiert)
  - test_patch_run_schedule_async_reaches_main_dispatch  (neu)
  - test_patch_run_dlq_async_reaches_main_dispatch       (neu)
  - test_patch_run_inspect_async_reaches_main_dispatch   (neu)
  - test_demonstration_submodule_import_defeats_facade_patch  (skipped demo)
```
Muster für die ersten vier: jeder Test baut `argv` für den jeweiligen
Subcommand, patched das `*_async`-Symbol über die Fassade mit einem
Sentinel-Callable, ruft `main()` auf, asserted dass der Sentinel genau
einmal mit den erwarteten Args aufgerufen wurde.

Der fünfte Test ist **skipped** und dokumentiert den negativen Pfad:
```python
@pytest.mark.skip(
    reason="Demonstration-only: shows that a submodule-local 'from X import Y' "
           "inside main() would defeat patch('swarmq.cli.Y'). Unskip locally to "
           "verify: swap the main()-dispatch line for a fresh submodule import "
           "and observe this test turning RED while the live-patch tests above "
           "stay GREEN. Do not unskip in CI."
)
def test_demonstration_submodule_import_defeats_facade_patch(...):
    ...
```

**Gates:**
- 4 live-patch Tests grün.
- Skipped-Demo-Test per `pytest --collect-only` nachweisbar
  (bleibt in der Suite, wird aber nicht ausgeführt).

**Risiko:** Gering. Reine Test-Addition, keine Produktionscode-Änderung.

---

### Unit 7 — M-04: `cli/__init__.py`-Docstring trimmen (R-7)

**Problem:** 32-zeiliger Module-Docstring in `cli/__init__.py`; nur
der "warum `main()` hier lebt"-Absatz ist load-bearing für Future-
Refactor-Entscheidungen.

**Entscheidung (E8):** Keine eigenen Tests für diesen Trim. Line-Count-
Assertions sind gameable (12 Zeilen Word-Salad passen); Content-
Assertions ("mentions 'patch'") gleichermaßen. Der Trim ist ein
einmaliger Refactor, den Code-Review (und nachfolgend `pydoc swarmq.cli`-
Smoke) vollständig absichern. Falls der Docstring künftig wieder
auswuchert, löst das ein Ruff-`D`-Rule-Setup (separater Plan).

**Ablauf:**
- Nur ein Commit: `refactor(cli-residuals): M-04 — trim cli/__init__.py docstring`.
- Docstring auf den "warum `main()` hier lebt"-Kern reduziert
  (~5–8 Zeilen).
- Weggelassene Historie als Kommentar verlinkt:
  `# Siehe docs/plans/2026-04-22-001-refactor-cli-split-plan.md für Hintergrund`.

**Gates:**
- Unit-Suite grün (keine neuen Tests nötig).
- `pydoc swarmq.cli` rendert sauber (manueller Smoke).
- Code-Review bestätigt, dass der load-bearing Absatz erhalten ist.

**Risiko:** Trivial.

---

## Won't-Fix Register (mit Begründung)

> Diese Findings **bleiben** offen. Der nächste ce-review wird sie
> erneut flaggen — der Verweis auf diesen Plan ist die kanonische
> Antwort.

| Finding | Severity | Begründung | Trigger für Re-Evaluation |
|---|---|---|---|
| **PS-01** | P1 | Branch ist test-validiert; retroaktives Splitten per Git-Rewrite wäre technisch möglich, aber der Aufwand (Rebase-Konflikte, Re-Test aller Zwischenstufen) rechtfertigt den Nutzen nicht. **Bewusst akzeptierte Einmal-Ausnahme.** | Nächster großer Refactor MUSS wieder strict-TDD laufen — kein weiterer PS-01-Freischein. |
| **PS-02** | P2 | `main()` im `__init__.py` ist load-bearing für `patch("swarmq.cli.run_worker_async")`-Semantik. Im Original-Plan + Docstring begründet. **Validierung:** R-6 / Unit 6 prüft diese Eigenschaft explizit auch für `schedule`/`dlq`/`inspect` via `TestLivePatchThrough`. | Nur wenn wir komplett auf `importlib.util.module_from_spec`-basierte Tests umstellen. |
| **KP-2** | P2 | `_DLQNotFound` in frozen surface whitelist (`test_cli_module_surface.py`). Rename = API-Break für externe Patcher — aber Underscore-Präfix signalisiert internal, kein konkreter externer Consumer nachgewiesen. **Sunset 2026-Q4 (Decision E6):** Falls bis 2026-12-31 kein Consumer via Issue/Docs/Slack bekannt wird, Rename in separatem Plan durchführen. | Sunset-Datum oder früherer Nachweis, dass kein Consumer existiert. |
| **KP-7** | P3 | ~~`match`-Statement braucht Python 3.14+; `pyproject.toml` pinnt Minimum noch niedriger.~~ **Korrektur:** `pyproject.toml:9` hat bereits `requires-python = ">=3.14"`, und `match` ist seit 3.10 verfügbar — die ursprüngliche Begründung greift nicht. Aktueller Grund: Stil-Präferenz (`if/elif`-Ketten sind für Small-N-Dispatch lesbar genug), nicht Kompatibilität. | Wenn Dispatch-Ketten wachsen oder Pattern-Matching auf Shape-Guards gebraucht wird. |
| **CLI-05** | P3 | `--burst`-Exit-Code-Mehrdeutigkeit ist dokumentierte Limitation in `--burst-idle-timeout`-Help. Disambiguation bräuchte neuen Exit-Code → API-Break. | Nur im Rahmen einer Exit-Code-Policy-Refactor. |
| **M-02 / M-03** (Rest-Whitelist) | P3 | Surface-Whitelist friert Re-Exports ein. **Partielle Auflösung:** Die zwei Heavy-Import-Einträge `ValkeyBackend`/`RabbitMQBroker` werden in Unit 1 auf lazy `__getattr__` umgestellt (Identity bleibt erhalten). Restliche ambient Re-Exports (Path, Sequence, etc.) bleiben bis Sunset 2026-Q4 eingefroren (Decision E6). | Sunset-Datum oder affirmativer Consumer-Nachweis. |

## Verification Matrix

| Gate | Kommando | Erwartung |
|---|---|---|
| Unit-Suite | `pytest tests/unit/ -x` | Pre-Baseline 1204 passed / 1 skipped → Post: mindestens 1204 + N neue Tests (ca. 17–22) passed, 1 skipped |
| Integration | `pytest tests/integration/ -x --no-rabbit` (Local-ENV) | Keine **neuen** Failures (pre-existing RabbitMQ-Konnexion bleibt) |
| E2E | `pytest tests/e2e/ -x` | 93 passed, 1 bekannter flaky — unverändert |
| Ruff | `ruff check src/ tests/` | ≤ 4 pre-existing findings (nicht wachsen) |
| Mypy | `mypy src/swarmq` | ≤ 15 pre-existing errors (nicht wachsen) |
| CLI-lokale Laziness (R-1a) | `grep -E "^from swarmq\\.(backend\\|broker)" src/swarmq/cli/_shared.py src/swarmq/cli/__init__.py` | Leer — CLI-Module ziehen Backend/Broker nicht mehr top-level. |
| Import-Latency (R-1b, Follow-up-Plan) | siehe `docs/plans/2026-04-23-002-fix-import-laziness-plan.md` | Nicht Gate dieses Plans. |
| Golden-File Help | `python -m pytest tests/unit/test_cli_help_output.py` | Grün nach Regeneration in Unit 5 |

## Commit-Plan (ein Commit-Pair pro Unit)

```
test(cli-residuals): CLI-01 — reproduce eager valkey import
fix(cli-residuals): CLI-01 — lazy broker/backend imports in _shared

test(cli-residuals): CLI-02 — reproduce --check false-positive
fix(cli-residuals): CLI-02 — _is_check_invocation strict-positional

test(cli-residuals): CLI-03 — reproduce ANSI on piped stderr
fix(cli-residuals): CLI-03 — TTY-guard ConsoleRenderer

test(cli-residuals): KP-5 — reproduce private _client access
fix(cli-residuals): KP-5/RR-1 — ValkeyBackend.get_scheduler_leader() public

test(cli-residuals): CLI-04 — reproduce table-on-pipe
fix(cli-residuals): CLI-04 — TTY-aware --format default + regen help snapshots

test(cli-residuals): RR-2 — live-patch tests + mutation-demo (skipped) for schedule/dlq/inspect
# Unit 6 ist reine Test-Addition ohne Produktionscode-Änderung — deshalb
# kein fix()-Commit. AGENTS.md's RED+GREEN-Regel gilt für Produktionscode;
# test-only Commits sind von Natur aus "Green on first run" und brauchen
# kein Pendant.

refactor(cli-residuals): M-04 — trim cli/__init__.py docstring
# Unit 7 hat keinen Test-Commit (Decision E8): Line-Count/Content-Tests für
# Docstrings sind gameable und liefern keinen dauerhaften Schutz. Code-Review
# + pydoc-Smoke reichen für einen einmaligen Trim.
```

Branch-Strategie: Zwei Optionen, eine wählen:
1. **Stacked auf `refactor/cli-split`** — neuer Branch `fix/cli-residuals`,
   rebased auf aktuellem CLI-Split-HEAD. Wird nach Merge vom CLI-Split
   self-hostend.
2. **Direkt auf `refactor/cli-split`** — 7 Commit-Pairs on top. Erhöht
   den Branch vor Merge. Bevorzugt, wenn CLI-Split noch nicht
   gemerged ist und in einem Review-Zyklus durchlaufen soll.

Empfehlung: **Option 2** (direkt auf `refactor/cli-split`), damit
der Merge in `main` alle Alt-Bugs mitnimmt und der nächste ce-review
gegen `main` clean ist.

## Resolved Decisions (document-review 2026-04-23)

Alle ursprünglich offenen Fragen sind entschieden und in die Units
eingearbeitet. Dieser Abschnitt dokumentiert die Entscheidungen für
Audit-Spur.

| Ref | Frage | Entscheidung | Eingearbeitet in |
|---|---|---|---|
| **E1** | Unit 1 — reicht `_shared.py`-Änderung für R-1? | Nein. Scope ausgeweitet auf `cli/__init__.py` ambient re-exports via `__getattr__`-Lazy-Pattern; Pre-Impl-Audit via `python -X importtime` ist Pflicht. M-02/M-03 partiell re-öffnet für die beiden Heavy-Import-Symbole. | Unit 1, Scope Boundaries, R-1, Won't-Fix M-02/M-03 |
| **E2** | Unit 5 — Scope-Creep vs. echter Fix? | Behalten mit Env-Var-Escape-Hatch `SWARMQ_DEFAULT_FORMAT=table\|json`. Non-Goals-Wording oben angepasst ("reversible Default-Changes sind ok"). Changelog-Eintrag Pflicht. | Nicht-Ziele, Unit 5, R-5, Definition of Done |
| **E3** | Unit 2 — Positional-Heuristik vs. argparse-basierte Detection? | **Refined during implementation (Commit `abb4ea1`):** Post-argparse hätte AC-06 (`test_check_argparse_error_emits_json`) gebrochen — bei argparse-Typvalidator-Fehlern gibt es kein `args.check`-Attribut. Statt dessen: **Pre-parse Minimal-Detector-argparse** mit `exit_on_error=False` + `parse_known_args`, der die arg-consuming worker-Flags (`--task`, `--module`, `--queue`, `--watch-dir`, `--log-level`, `--log-format`) kennt. Bugs #1 und #2 gefixt, AC-06 bleibt grün. | Unit 2, R-2, Commit `abb4ea1` |
| **E4** | Unit 4 — `ValkeyConnectionError` swallow oder propagate? | **Propagate.** Erhält heutiges Verhalten, Ops-Visibility intakt. `schedule status` exitet non-zero bei Backend-Ausfall. | Unit 4a/4b, R-4 |
| **E5** | Unit 3 — Static vs. lazy TTY-Check? | Static. Matcht 99 %-Fall, hält Code simpel. Dedizierter `test_setup_logging_respects_capsys_redirect`-Test schützt gegen Test-Capture-Regressionen. | Unit 3, R-3 |
| **E6** | KP-2 / M-02 / M-03 — Sunset-Date? | **2026-12-31 (Q4).** Falls bis dahin kein externer Consumer per Issue/Docs/Slack bekannt, Rename / Whitelist-Trim in separatem Plan. Verhindert unbegrenzten Freeze. | Won't-Fix KP-2 + M-02/M-03 |
| **E7** | Unit 6 — Mutation-Test manuell oder committed? | **Committed als `@pytest.mark.skip`-Demo** mit reproduzierbarer Docstring. Verhindert self-reported Effektivität. | Unit 6 |
| **E8** | Unit 7 — Docstring-Tests oder nur Refactor-Commit? | **Nur Refactor-Commit.** Line-Count/Content-Assertions sind gameable und liefern keinen dauerhaften Schutz. Code-Review + pydoc-Smoke reichen. | Unit 7, R-7, Commit-Plan |
| Golden-File-Regen (Unit 5) | Teil des GREEN-Commits oder separat? | Teil des GREEN-Commits — hält RED-GREEN-Sequenz pro Finding atomar. | Unit 5 Gates |

## Definition of Done

- [x] Pre-Implementation-Audit (`python -X importtime -c 'import swarmq.cli'`) ausgeführt 2026-04-23; ergab Scope-Korrektur (R-1b delegiert an Follow-up-Plan).
- [x] Alle 7 Units gelandet (Unit 6 + Unit 7 mit Test-Commit-Ausnahme — siehe Commit-Plan). Letzter Unit-Commit: `f385ab0`.
- [x] `pytest tests/unit` grün, Zählung gewachsen um mindestens ~18 Tests. **Tatsächlich: 1249 passed / 2 skipped vs. Baseline 1204/1 → +45 Tests** (Audit 2026-05-26).
- [x] `git grep "_client.get(SCHEDULER_LOCK_KEY)" src/swarmq/cli/` leer (verifiziert 2026-05-26).
- [x] `grep "^from swarmq\\.(backend\\|broker)" src/swarmq/cli/_shared.py src/swarmq/cli/__init__.py` leer (R-1a — verifiziert 2026-05-26).
- [x] Follow-up-Plan `2026-04-23-002-fix-import-laziness-plan.md` als Stub angelegt und mit diesem Plan verlinkt.
- [x] Changelog-Eintrag für CLI-04 ergänzt (Behaviour-Change **mit** Env-Var-Escape-Hinweis). Inhaltlich vorhanden in `website/reference/changelog.md` (en) + `.de.md` (de); **versioniert via Folge-Plan** `docs/plans/2026-05-26-001-fix-cli-residuals-dod-followups-plan.md` Unit 4 (`website/` war in `.gitignore:43` versteckt).
- [x] `SWARMQ_DEFAULT_FORMAT`-Env-Var in `--format`-Help-Texten dokumentiert (9 Vorkommen in `src/swarmq/cli/parser.py`); Reference-Docs via Changelog-Versionierung im Folge-Plan.
- [x] Sunset-Datum KP-2 / Rest-Whitelist (2026-12-31) in diesem Plan dokumentiert; **Follow-up-Reminder via Folge-Plan** `docs/plans/2026-05-26-001-...` Unit 2 in `docs/solutions/patterns/sunset-cli-surface-whitelist-kp2-2026-12-31.md` mit `sunset_date`-Frontmatter abgelegt.
- [x] Won't-Fix-Register als Referenz im Commit-Body des Plan-DoD-Markup-Commits (Folge-Plan Unit 3) verlinkt — siehe Abschnitt [Won't-Fix Register (mit Begründung)](#wont-fix-register-mit-begründung) für die kanonische Antwort auf erneutes Flagging von PS-01, PS-02, KP-2, KP-7, CLI-05, M-02, M-03.
- [x] Optional: neuer ce-review-Run bestätigt, dass keines der 7 gefixten Findings mehr flaggt (die 6 Won't-Fix-Findings bleiben flagged — erwartet). **Ausgeführt 2026-05-26**, Run-ID `20260526-090918-e0cb3bf1`. Ergebnis: 0 der 7 gefixten Findings erneut geflaggt; 6 Won't-Fix wie erwartet pre-existing. 17 neue Findings entdeckt (P2+P3), davon 11 applied (Commit `2e6e601`), 5 residual (alle dokumentiert), 6 testing zu testing_gaps demotiert. Verdict: Ready to merge.

**DoD-Verifikations-Audit:** 2026-05-26 (siehe `docs/plans/2026-05-26-001-fix-cli-residuals-dod-followups-plan.md`).
**ce-code-review Re-Run:** 2026-05-26, Run-ID `20260526-090918-e0cb3bf1`, Closing-Commit `2e6e601`.
