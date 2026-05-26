---
title: "Import-Laziness: valkey/aio_pika raus aus `import swarmq.cli`"
status: ready
execution_mode: incremental
quantification:
  baseline_import_swarmq_cli_ms: 187.6
  baseline_swarmq_pkg_init_ms: 185.8
  baseline_valkey_asyncio_cumulative_ms: 18.6
  target_median_reduction_percent: 60
  audit_completed: 2026-05-26
created: 2026-04-23
audit_completed: 2026-05-26
depends_on:
  - 2026-04-23-001-fix-cli-refactor-residuals-plan.md (Unit 1) — CLI-lokale Imports entkoppeln
parent_finding: CLI-01 (ce-review run 20260422-192644-bba2a351) — R-1b
---

## Overview

**Ausgelöst durch Audit-Erkenntnis in Plan 2026-04-23-001:** Auch nach
CLI-lokalen Lazy-Imports bleiben `valkey` und `aio_pika` in
`sys.modules` nach `import swarmq.cli`. Der ursprüngliche Stub-Plan
nannte `swarmq/app.py` und `swarmq/client.py` als Hauptverursacher.

**Audit 2026-05-26 hat den eigentlichen Hauptverursacher identifiziert:**
`src/swarmq/__init__.py` re-exportiert `ValkeyBackend` und
`RabbitMQBroker` als Top-Level Public-API (`from swarmq import ValkeyBackend`).
Jedes `import swarmq.cli` lädt zwingend Parent-`swarmq` → triggert
Zeile 2 + 3 von `swarmq/__init__.py` → eager `from swarmq.backend.valkey
import ValkeyBackend` → ganze `valkey.*`-Kette in `sys.modules`. Selbst
wenn alle anderen Dateien zu `TYPE_CHECKING` migrieren, ändert sich
ohne diese eine Datei **nichts** an der Import-Latency.

**Ziel:** `python -c "import swarmq.cli"` lädt weder `valkey` noch
`aio_pika`. Median von 10 Warm-Runs ≥ 60 % unter Pre-Baseline
(187.6 ms → ≤ 75 ms). Dies ist die volle Einlösung von R-1 aus Plan 001.

**Nicht-Ziele:** Keine Verhaltensänderung an `SwarmQ`/`Client`, kein
API-Break (Identity bleibt erhalten via PEP-562 `__getattr__`), keine
neue Funktionalität. Rein strukturelle Import-Umstellung.

## Problem Frame

1. **Top-Level Re-Exports als versteckter Kern-Bottleneck.** `swarmq/__init__.py`
   ist die kanonische User-API:
   ```python
   from swarmq import SwarmQ, ValkeyBackend, RabbitMQBroker
   ```
   Die `from … import ValkeyBackend`-Zeile ist eager — jeder
   `import swarmq.*` (auch CLI) zieht damit valkey rein. Lösung:
   PEP-562 `__getattr__` analog zum bereits in
   `src/swarmq/cli/__init__.py` etablierten Pattern. Identity wird
   beim ersten Zugriff erhalten, API bricht nicht.

2. **Type-only Imports brauchen kein Runtime-Binding.** Mit
   `from __future__ import annotations` werden alle Annotations zu
   Strings. Symbole müssen nicht zur Import-Zeit verfügbar sein.
   Standard-Lösung: Imports unter `if TYPE_CHECKING:`. Betroffen:
   - `src/swarmq/app.py:8-9` — Type-Annotation in `SwarmQ.__init__`
   - `src/swarmq/client.py:15-16` — Type-Annotation in `Client.__init__`
   - `src/swarmq/worker.py:24,26` — Type-Annotation in `Worker.__init__` + 2 Doku-Kommentare (rein dokumentarisch)
   - `src/swarmq/scheduler.py:38-39` — Type-Annotation in `ScheduleStore.__init__` + `SchedulerLoop.__init__`
   - `src/swarmq/cli/schedule_cmd.py:22` — Type-Annotation auf `_get_backend_factory`-Return
   - `src/swarmq/cli/dlq_cmd.py:17` — Type-Annotation auf `_get_broker_factory`-Return

3. **Runtime-Instantiierung im CLI.** `src/swarmq/cli/worker_cmd.py:54-55`
   instanziiert `RabbitMQBroker(broker_url)` und `ValkeyBackend(backend_url)`
   direkt — Type-Annotation und Runtime-Use. Lösung: Lazy-Local-Import
   innerhalb der Funktion `_resolve_default_factories` (Pattern wie
   `cli/_shared.py::_backend_from_env`).

4. **Keine `isinstance`-Checks gefunden.** Vollständiger Scan über
   `src/` und `tests/` zeigt 0 Treffer für `isinstance(x, ValkeyBackend)`
   oder `isinstance(x, RabbitMQBroker)`. Damit fällt die Hauptsorge
   um Runtime-Bindings.

5. **Latency-Impact aus Audit 2026-05-26 (`uv run python -X importtime -c "import swarmq.cli"`):**
   - Total: 187.6 ms cumulative
   - `swarmq` package init: 185.8 ms (99 % des Totals)
   - `valkey.asyncio` self+transitive: ~18.6 ms direkter Pfad, transitiv
     mit `valkey.client`, `valkey.connection`, `libvalkey` deutlich mehr
   - `prometheus_client` (via `swarmq.metrics` top-level): ~9 ms
   - Direkter `cli/*`-Code: < 1 ms (R-1a bereits gelandet)

   Vollständiger Trace: `.context/import-laziness/baseline-import-trace.txt`
   (gitignored).

## Requirements Trace

| ID  | Anforderung | Quelle | Satisfied by |
|-----|-------------|--------|--------------|
| R-1 | Nach `import swarmq.cli` enthält `sys.modules` weder einen Eintrag, der mit `valkey` beginnt, noch einen, der mit `aio_pika` beginnt. | CLI-01 R-1b | Units 1–3 |
| R-2 | Nach `import swarmq.cli` lädt sich weder `prometheus_client` noch dessen transitive Kette. (Stretch — kann an Unit 1 mitgenommen werden, falls `swarmq.metrics`-Import in `swarmq/__init__.py` ebenfalls top-level eager ist.) | Audit 2026-05-26 | Unit 1 (stretch) |
| R-3 | `from swarmq import ValkeyBackend` und `from swarmq import RabbitMQBroker` funktionieren weiterhin und liefern dieselbe Class-Identität wie `from swarmq.backend.valkey import ValkeyBackend` bzw. `from swarmq.broker.rabbitmq import RabbitMQBroker`. | Public-API-Erhalt | Unit 1 |
| R-4 | Median von 10 Warm-Runs `python -X importtime -c "import swarmq.cli"` mindestens **60 %** unter 187.6 ms Baseline (→ ≤ 75 ms). | CLI-01 R-1b Effektivität | Unit 4 |
| R-5 | `mypy src/swarmq` bleibt grün — alle `TYPE_CHECKING`-verschobenen Annotationen resolven korrekt via `from __future__ import annotations`. | Regression | Units 2–3 |
| R-6 | Keine Regression in `tests/unit/`, `tests/integration/`, `tests/e2e/`. Baseline nach Plan 001 + ce-review-Re-Run: 1257 passed / 2 skipped. | Regression | Alle Units |
| R-7 | Identity-Tests in `tests/unit/test_cli_module_surface.py` bleiben grün (CLI-Surface-Whitelist erhält Identity-Check). | R-8 aus Plan 001 | Unit 1 |

## Scope Boundaries

**In Scope:**
- `src/swarmq/__init__.py` — PEP-562 `__getattr__`-Pattern für Re-Exports von `ValkeyBackend`, `RabbitMQBroker`. Optional: gleiches Pattern für `SwarmQ`/`Client`, falls deren Imports `swarmq.app`/`swarmq.client` transitiv heavy laden (Unit 1 entscheidet).
- `src/swarmq/app.py` — `from swarmq.backend.valkey import ValkeyBackend` + `from swarmq.broker.rabbitmq import RabbitMQBroker` → `if TYPE_CHECKING:`.
- `src/swarmq/client.py` — analog.
- `src/swarmq/worker.py` — analog.
- `src/swarmq/scheduler.py` — analog.
- `src/swarmq/cli/schedule_cmd.py` — `ValkeyBackend`-Import → `if TYPE_CHECKING:`.
- `src/swarmq/cli/dlq_cmd.py` — `RabbitMQBroker`-Import → `if TYPE_CHECKING:`.
- `src/swarmq/cli/worker_cmd.py` — Lazy-Local-Import von `ValkeyBackend`/`RabbitMQBroker` innerhalb `_resolve_default_factories` (Z. 54-55).
- `tests/unit/test_cli_import_laziness.py` (neu) — alle R-1/R-2/R-3-Assertions.
- `scripts/bench_cli_import.py` (neu) — Micro-Benchmark für R-4-Verifikation.

**Out of Scope:**
- `src/swarmq/backend/valkey.py` — Backend-Implementierung selbst, runtime needed.
- `src/swarmq/broker/rabbitmq.py` — Broker-Implementierung selbst, runtime needed.
- `src/swarmq/locking.py`, `src/swarmq/limits.py` — nutzen `valkey.asyncio` direkt im Runtime-Code. Lazy hier würde nichts bringen, da diese Module sowieso nur on-demand geladen werden (nicht von cli).
- Rename/Refactor jenseits der Import-Position-Verschiebung.
- Neue Tests für unbeteiligte Module.
- mypy-Cleanups jenseits dessen, was die Verschiebungen erfordern.

## Target Changes Summary

```
src/swarmq/
  __init__.py                 # PEP-562 __getattr__ für ValkeyBackend, RabbitMQBroker (R-3, R-1)
  app.py                      # TYPE_CHECKING (R-1, R-5)
  client.py                   # TYPE_CHECKING (R-1, R-5)
  worker.py                   # TYPE_CHECKING (R-1, R-5)
  scheduler.py                # TYPE_CHECKING (R-1, R-5)
  cli/
    schedule_cmd.py           # TYPE_CHECKING (R-1, R-5)
    dlq_cmd.py                # TYPE_CHECKING (R-1, R-5)
    worker_cmd.py             # Lazy-local in _resolve_default_factories (R-1)

tests/unit/
  test_cli_import_laziness.py # neu — R-1, R-2, R-3 (4 Tests)

scripts/
  bench_cli_import.py         # neu — R-4
```

## Units

> **Reihenfolge:** Unit 1 zuerst (größter Effekt allein), dann Unit 2
> + 3 (Aufräumarbeiten), dann Unit 4 (Verifikation). Jede Unit ist ein
> Commit-Pair (RED + GREEN), bis auf Unit 4 (test+bench-only).

---

### Unit 1 — `swarmq/__init__.py` PEP-562 `__getattr__` (R-1, R-3)

**Problem:** `swarmq/__init__.py:2-3` lädt eager `ValkeyBackend` +
`RabbitMQBroker` für die Public-API. Diese Zeilen sind allein
verantwortlich für ~ 99 % der gemessenen 187.6 ms — entfernen sie sich
lazy, fallen die transitiven `valkey.*` / `aio_pika.*` weg.

**RED:**

```
tests/unit/test_cli_import_laziness.py::TestImportLaziness
  - test_import_swarmq_does_not_load_valkey
  - test_import_swarmq_does_not_load_aio_pika
  - test_import_swarmq_cli_does_not_load_valkey
  - test_import_swarmq_cli_does_not_load_aio_pika
```

Assertion-Pattern (subprocess für saubere `sys.modules`):
```python
import subprocess, sys, textwrap

def _modules_after(import_stmt: str) -> set[str]:
    code = textwrap.dedent(f"""
        import sys
        {import_stmt}
        print("\\n".join(m for m in sys.modules if "valkey" in m or "aio_pika" in m))
    """)
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    return set(result.stdout.strip().splitlines())

def test_import_swarmq_does_not_load_valkey() -> None:
    loaded = _modules_after("import swarmq")
    leaked = {m for m in loaded if m.startswith("valkey")}
    assert not leaked, f"swarmq import leaked: {sorted(leaked)}"
```

Bei der RED-Ausführung müssen alle 4 Tests rot werden — bestätigt die
Hypothese, dass die Top-Level-Imports tatsächlich `valkey`/`aio_pika`
in `sys.modules` einschleusen.

**GREEN — Pattern:**
```python
# src/swarmq/__init__.py
from __future__ import annotations

from typing import TYPE_CHECKING

from swarmq.app import SwarmQ            # SwarmQ ist die Haupt-API, lazyness optional
from swarmq.exceptions import (...)       # leichtgewichtig, bleibt eager
from swarmq.health import HealthChecker, HealthServer, serve_health  # prüfen
from swarmq.metrics import Metrics, MetricsServer, install_signal_collectors, serve_metrics  # ← prüfen ob heavy
from swarmq.middleware import MessageContext, Middleware
from swarmq.progress import ProgressInfo
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import DeliveryMode, Task, TaskInfo

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker

_LAZY_SURFACE = {
    "ValkeyBackend": ("swarmq.backend.valkey", "ValkeyBackend"),
    "RabbitMQBroker": ("swarmq.broker.rabbitmq", "RabbitMQBroker"),
}

def __getattr__(name: str) -> object:
    if name in _LAZY_SURFACE:
        module, attr = _LAZY_SURFACE[name]
        import importlib
        return getattr(importlib.import_module(module), attr)
    raise AttributeError(f"module 'swarmq' has no attribute {name!r}")

def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + list(_LAZY_SURFACE.keys()))
```

**Audit-Schritt INNERHALB Unit 1:** Prüfen, ob `swarmq.metrics`,
`swarmq.health`, `swarmq.app` selbst heavy transitive Imports ziehen.
Wenn ja, dieselbe Lazy-Behandlung für die Top-Level-Re-Exports
anwenden (oder Unit 1 splittet auf 1a/1b). Das Audit-Ergebnis im
GREEN-Commit-Body dokumentieren.

**Gates:**
- 4 neue Tests grün (sys.modules-Assertion).
- `from swarmq import ValkeyBackend; assert ValkeyBackend is …backend.valkey.ValkeyBackend` (Identity).
- Bestehender Identity-Check in `tests/unit/test_cli_module_surface.py` bleibt grün.
- `pytest tests/unit -q` insgesamt ≥ 1257 passed / 2 skipped (keine Regression).

**Risiko:** Mittel. `__getattr__` ist PEP-562, gut unterstützt. Edge-
Cases: `dir(swarmq)`, `vars(swarmq)`, `inspect.getmembers(swarmq)` —
letzteres triggert die lazy imports und defeated die Latenz beim
introspektierenden Caller (cosmetic, dokumentiert in `__getattr__`-
Docstring).

**Commit-Pair:**
```
test(import-laziness): U1 — reproduce eager valkey/aio_pika in sys.modules
fix(import-laziness): U1 — PEP-562 __getattr__ for swarmq.ValkeyBackend/RabbitMQBroker
```

---

### Unit 2 — TYPE_CHECKING-Umstellung in Type-only Konsumenten (R-1, R-5)

**Problem:** `app.py`, `client.py`, `worker.py`, `scheduler.py`,
`cli/schedule_cmd.py`, `cli/dlq_cmd.py` halten alle die
Backend/Broker-Klassen als Type-Annotation top-level. Alle haben
`from __future__ import annotations` — Annotations werden Strings,
die Imports sind syntaktisch überflüssig.

**Rationale:** Auch nach Unit 1 würde ein Konsument, der direkt
`from swarmq.app import SwarmQ` macht (statt `import swarmq` oder
`import swarmq.cli`), trotzdem die transitive Kette ziehen. Unit 2
schließt diese Seitentüren.

**RED:**

Erweitert dasselbe Test-File aus Unit 1:
```
tests/unit/test_cli_import_laziness.py::TestSideDoorImports
  - test_import_swarmq_app_does_not_load_valkey
  - test_import_swarmq_client_does_not_load_aio_pika
  - test_import_swarmq_worker_does_not_load_valkey
  - test_import_swarmq_scheduler_does_not_load_valkey
```

Diese Tests sind nach Unit 1 immer noch RED (Unit 1 fixt nur den
`swarmq`/`swarmq.cli`-Pfad).

**GREEN — Pattern pro Datei:**
```python
# Vorher:
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker

# Nachher:
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker
```

Die übrigen Stellen in der Datei brauchen keine Änderung — die
Annotations bleiben buchstäblich identisch (Strings dank `__future__`).

**Gates:**
- 4 neue Tests grün.
- `mypy src/swarmq` ohne neue Errors.
- Volle Unit-Suite weiterhin ≥ 1257 passed.

**Risiko:** Gering. Mechanische Verschiebung. Hauptfehlerquelle:
vergessen, dass eine non-Annotation-Verwendung (z.B. `isinstance`)
existiert — Audit hat 0 Treffer gezeigt, aber während der Umstellung
nochmal pro Datei mit `grep -n "ValkeyBackend\|RabbitMQBroker"`
gegenchecken.

**Commit-Pair:**
```
test(import-laziness): U2 — reproduce side-door eager imports
fix(import-laziness): U2 — TYPE_CHECKING for type-only backend/broker imports
```

---

### Unit 3 — `cli/worker_cmd.py` Lazy-Local-Import (R-1)

**Problem:** `cli/worker_cmd.py` ist der einzige Fall mit
**Runtime-Instantiierung** (`RabbitMQBroker(broker_url)`,
`ValkeyBackend(backend_url)` in Z. 54-55) — TYPE_CHECKING reicht nicht.

**Lösung:** Pattern wie `cli/_shared.py::_backend_from_env`:
Top-Level-Import streichen, Import innerhalb der konsumierenden
Funktion einbetten.

```python
# Vorher (Top-Level):
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker

# Nachher: nichts top-level. Innerhalb _resolve_default_factories:
def _resolve_default_factories(...):
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker
    broker = RabbitMQBroker(broker_url)
    backend = ValkeyBackend(backend_url)
    ...
```

**RED:** Erweitert dasselbe Test-File:
```
tests/unit/test_cli_import_laziness.py::TestCliWorkerCmd
  - test_import_swarmq_cli_worker_cmd_does_not_load_heavy_libs
```

**Gates:**
- Test grün.
- `swarmq worker --help` funktioniert unverändert.
- `swarmq worker --check` funktioniert unverändert.
- `swarmq worker` (echter Start) lädt die Klassen weiterhin korrekt.

**Risiko:** Gering. Pattern bereits etabliert in `_shared.py`.

**Commit-Pair:**
```
test(import-laziness): U3 — reproduce eager imports in cli/worker_cmd
fix(import-laziness): U3 — lazy-local backend/broker imports in worker_cmd
```

---

### Unit 4 — Latency-Benchmark + Final-Verifikation (R-4)

**Problem:** R-4 verlangt ≥ 60 % Reduktion gegen 187.6 ms Baseline.
Brauchen reproduzierbares Mess-Werkzeug, das nicht auf
umgebungsabhängige absolute Werte gated, sondern auf relative
Reduktion gegen einen mitgemessenen Pre-Baseline (oder fixierten
Anchor).

**Aktion:**

1. `scripts/bench_cli_import.py` (neu):
   ```python
   """Micro-benchmark for `import swarmq.cli` latency.

   Runs N warm-runs in fresh subprocesses, reports median + p95.
   Not a pytest gate (env-sensitive); developer feedback tool.
   """
   import subprocess, sys, statistics, time

   N = 10
   times: list[float] = []
   for _ in range(N):
       start = time.perf_counter_ns()
       subprocess.run([sys.executable, "-c", "import swarmq.cli"], check=True, capture_output=True)
       times.append((time.perf_counter_ns() - start) / 1e6)
   times.sort()
   print(f"median: {statistics.median(times):.1f} ms")
   print(f"p95:    {times[int(N * 0.95)]:.1f} ms")
   print(f"min:    {times[0]:.1f} ms")
   ```

2. Run + Ergebnis im Unit-4-Commit-Body dokumentieren:
   ```
   Pre-baseline (Audit 2026-05-26): 187.6 ms cumulative
   Post-fix (Audit <DATE>): <X> ms median
   Reduction: <Y> %
   ```

3. Optional: ein `@pytest.mark.benchmark`-gated Test, der den
   Run einmal ausführt und eine Soft-Assertion macht (warnt, fail
   nicht) — entscheiden in Unit 4.

**Gates:**
- Benchmark-Script läuft erfolgreich.
- Median ≤ 75 ms (R-4-Schwelle).
- Trace-Datei `.context/import-laziness/post-fix-import-trace.txt`
  erzeugt zur Audit-Spur.

**Risiko:** Trivial — reine Mess- und Doku-Arbeit.

**Commit (kein Pair — script+doc only):**
```
test(import-laziness): U4 — bench_cli_import.py + record final reduction
```

---

## Resolved Decisions (Audit 2026-05-26)

| Ref | Frage | Entscheidung | Eingearbeitet in |
|---|---|---|---|
| **E1** | Hauptbottleneck — `app.py`/`client.py` oder `swarmq/__init__.py`? | **`swarmq/__init__.py`** ist verantwortlich für 99 % der 185 ms Package-Init-Latency via Re-Exports. Unit 1 muss zuerst kommen. | Overview, Problem Frame, Unit 1 |
| **E2** | Public-API erhalten oder API-Break? | **Erhalten** via PEP-562 `__getattr__` (Identity bleibt). `from swarmq import ValkeyBackend` funktioniert weiterhin. | R-3, Unit 1 |
| **E3** | Runtime `isinstance`-Checks vorhanden? | **Nein** — vollständiger Scan über `src/` + `tests/` lieferte 0 Treffer. Damit sind alle 7 Stellen sicher für TYPE_CHECKING oder Lazy-Local. | Problem Frame Punkt 4, Open-Question 1 |
| **E4** | `cli/worker_cmd.py` Runtime-Use — TYPE_CHECKING reicht? | **Nein** — instanziiert die Klassen in `_resolve_default_factories`. Pattern: Lazy-Local-Import wie `_shared.py`. | Unit 3 |
| **E5** | Benchmark-Schwelle absolut vs. relativ? | **Relativ (≥ 60 %)** — konsistent mit Plan 001-E-Decisions. Absolute Werte umgebungsabhängig. | R-4, Unit 4 |
| **E6** | Stretch: `prometheus_client` und `swarmq.metrics` mit-laziness? | **Ja, falls Aufwand klein** — Audit zeigte ~9 ms Beitrag. Wird in Unit 1 Audit-Schritt entschieden. | R-2 (stretch), Unit 1 |

## Verification Matrix

| Gate | Kommando | Erwartung |
|---|---|---|
| Lazy-Modules (R-1) | `uv run python -c 'import swarmq.cli; import sys; assert not any(m.startswith("valkey") or m.startswith("aio_pika") for m in sys.modules), [m for m in sys.modules if m.startswith("valkey") or m.startswith("aio_pika")]'` | Exit 0 |
| Identity-Erhalt (R-3) | `uv run python -c 'from swarmq import ValkeyBackend; from swarmq.backend.valkey import ValkeyBackend as Canonical; assert ValkeyBackend is Canonical'` | Exit 0 |
| Unit-Suite (R-6) | `uv run pytest tests/unit -q` | ≥ 1257 passed / 2 skipped — gleich oder gewachsen um die ~5 neuen Tests |
| mypy (R-5) | `uv run mypy src/swarmq` | Keine neuen Errors |
| Latency-Benchmark (R-4) | `uv run python scripts/bench_cli_import.py` | Median ≤ 75 ms (60 % Reduktion gegen 187.6 ms) |
| Side-door (R-1) | `uv run python -c 'import swarmq.app; import sys; assert not any(m.startswith("valkey") for m in sys.modules)'` | Exit 0 |

## Commit-Plan

```
test(import-laziness): U1 — reproduce eager valkey/aio_pika in sys.modules
fix(import-laziness): U1 — PEP-562 __getattr__ for swarmq.ValkeyBackend/RabbitMQBroker

test(import-laziness): U2 — reproduce side-door eager imports (app/client/worker/scheduler)
fix(import-laziness): U2 — TYPE_CHECKING for type-only backend/broker imports

test(import-laziness): U3 — reproduce eager imports in cli/worker_cmd
fix(import-laziness): U3 — lazy-local backend/broker imports in worker_cmd

test(import-laziness): U4 — bench_cli_import.py + record final reduction
```

Branch-Strategie: Eigener Branch `fix/import-laziness`, abgezweigt von
`refactor/cli-split` (Plan-Stamm). Sollte nach Plan 001 + DoD-Followup-Plan
landen — die AST-basierten R-1a-Tests von Plan 001 bleiben unabhängig grün;
die `sys.modules`-basierten Tests dieses Plans brauchen die globale Laziness.

## Definition of Done

- [ ] Pre-Implementation-Audit (`uv run python -X importtime -c 'import swarmq.cli'`) als Baseline-Artefakt unter `.context/import-laziness/baseline-import-trace.txt` gespeichert. **Erledigt 2026-05-26 — 187.6 ms cumulative.**
- [ ] Unit 1 gelandet (`__getattr__` für `swarmq` Top-Level). Identity-Test grün.
- [ ] Unit 2 gelandet (TYPE_CHECKING in 6 Dateien). mypy grün.
- [ ] Unit 3 gelandet (Lazy-Local in cli/worker_cmd.py).
- [ ] Unit 4 gelandet (Benchmark-Script + Post-Fix-Trace).
- [ ] R-1 verifiziert via Test: kein `valkey`/`aio_pika` in `sys.modules` nach `import swarmq.cli`.
- [ ] R-3 verifiziert via Test: `from swarmq import ValkeyBackend` Identity erhalten.
- [ ] R-4 verifiziert via Benchmark: Median ≤ 75 ms (≥ 60 % Reduktion).
- [ ] R-6 verifiziert: `pytest tests/unit` ≥ 1257 passed / 2 skipped.
- [ ] Plan 2026-04-23-001 R-1b-Verweis aktualisiert auf "satisfied via 2026-04-23-002".
- [ ] Eintrag in `website/reference/changelog.md` unter "Performance" — bezeichnet die Latency-Reduktion als zero-API-impact-Behaviour-non-Change.
