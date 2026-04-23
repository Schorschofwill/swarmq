---
title: "Import-Laziness: valkey/aio_pika raus aus `import swarmq.cli`"
status: stub
execution_mode: incremental
created: 2026-04-23
depends_on:
  - 2026-04-23-001-fix-cli-refactor-residuals-plan.md (Unit 1) — CLI-lokale Imports entkoppeln
parent_finding: CLI-01 (ce-review run 20260422-192644-bba2a351) — R-1b
---

## Overview

**Ausgelöst durch Audit-Erkenntnis in Plan 2026-04-23-001:** Auch nach
CLI-lokalen Lazy-Imports bleiben `valkey` und `aio_pika` in
`sys.modules` nach `import swarmq.cli`, weil `swarmq/app.py` und
`swarmq/client.py` die Backend/Broker-Klassen **als Type-Annotations**
top-level importieren. `from __future__ import annotations` ist in
beiden Dateien aktiv — Annotations werden zur Laufzeit nicht
ausgewertet, der eager Import ist rein syntaktisch überflüssig.

**Ziel:** `python -c "import swarmq.cli"` lädt weder `valkey` noch
`aio_pika`. Median von 10 Warm-Runs ≥ 60 % unter Pre-Baseline
(~152 ms). Dies ist die volle Einlösung von R-1 aus Plan 001.

**Nicht-Ziele:** Keine Verhaltensänderung an `SwarmQ`/`Client`, kein
API-Break, keine neue Funktionalität. Rein strukturelle Import-
Umstellung.

## Problem Frame

1. **Type-only Imports brauchen kein Runtime-Binding.** Mit
   `from __future__ import annotations` werden alle Annotations zu
   Strings; die referenzierten Symbole müssen nicht zur Import-Zeit
   verfügbar sein. Standard-Python-Pattern ist das Verschieben dieser
   Imports unter `if TYPE_CHECKING:` aus `typing`.

2. **Betroffene Dateien (vom Audit identifiziert):**
   - `src/swarmq/app.py:8-9` — `ValkeyBackend`, `RabbitMQBroker` nur
     als Type-Annotation in `SwarmQ.__init__` und Methoden-Signaturen.
   - `src/swarmq/client.py:15-16` — analog in `Client.__init__`.
   - Evtl. weitere `swarmq/*.py` — vollständiger Scan ist Teil des
     Plans (Unit 0 / Audit).

3. **Latency-Impact (aus Audit 2026-04-23):** `swarmq.backend.valkey`
   cumulative ~88 ms, `swarmq.broker.rabbitmq` ~21 ms, plus transitive
   `valkey.*` / `aio_pika.*`-Ketten. Entkopplung bringt > 100 ms
   Ersparnis beim `swarmq --help`-Startup.

## Requirements Trace (Stub — vor Start ausfüllen)

| ID | Anforderung | Satisfied by |
|---|---|---|
| R-1 | `import swarmq.cli` lädt kein `valkey`/`aio_pika` in `sys.modules`. | Unit 0 + Unit 1 |
| R-2 | Median von 10 Warm-Runs `import swarmq.cli` ≥ 60 % unter 152 ms Baseline. | Unit 1 |
| R-3 | `mypy` bleibt grün — alle `TYPE_CHECKING`-verschobenen Annotationen resolven korrekt via `from __future__ import annotations`. | Unit 1 Gates |
| R-4 | Keine Regression in `tests/unit/test_client.py`, `tests/integration/*`, `tests/e2e/*`. | Unit 1 Gates |
| R-5 | Keine Runtime-Verwendung der Symbole (`isinstance(x, ValkeyBackend)`) bricht — solche Stellen brauchen den non-TYPE_CHECKING-Import und werden einzeln bewertet. | Unit 0 (Audit) |

## Units (Stub)

### Unit 0 — Vollständiger Import-Audit

- Grep über `src/swarmq/**/*.py` nach Top-Level-Imports von
  `swarmq.backend.valkey` / `swarmq.broker.rabbitmq` und Auflisten der
  Verwendungs-Kontexte (Type-Annotation vs. Runtime-Call).
- Liste als Artefakt speichern, in RED-Commit referenzieren.

### Unit 1 — TYPE_CHECKING-Umstellung

- Für jede per Audit identifizierte Datei: Top-Level-Import unter
  `if TYPE_CHECKING:` verschieben, `typing.TYPE_CHECKING`-Import
  ergänzen falls nötig.
- Runtime-Verwendungen (falls Audit welche findet) entweder lazy-local
  importieren oder als genuinely needed belassen.

### Unit 2 — Import-Latency-Test + Benchmark

- `tests/unit/test_cli_import_laziness.py` mit:
  - `test_import_swarmq_cli_does_not_load_valkey`
  - `test_import_swarmq_cli_does_not_load_aio_pika`
  - `test_import_swarmq_app_does_not_load_valkey`
  - `test_import_swarmq_client_does_not_load_aio_pika`
- Micro-Benchmark-Skript unter `scripts/bench_cli_import.py`: misst
  10 Warm-Runs, emittiert Median + p95. Nicht als pytest-Gate
  (umgebungsabhängig), aber als Entwickler-Feedback-Tool.

## Verification Matrix (Stub)

| Gate | Kommando | Erwartung |
|---|---|---|
| Lazy-Modules | `python -c 'import swarmq.cli; import sys; assert "valkey" not in sys.modules and "aio_pika" not in sys.modules'` | Exit 0 |
| Unit-Suite | `pytest tests/unit/ -x` | Unverändert grün vs. Plan-001-Post-Baseline |
| mypy | `mypy src/swarmq` | Keine neuen Errors |
| Benchmark | `python scripts/bench_cli_import.py` | Median ≤ 61 ms (≥ 60 % Reduktion) |

## Open Questions (Stub — vor Start schließen)

1. **Runtime-isinstance-Checks:** Gibt es Code-Stellen, die
   `isinstance(x, ValkeyBackend)` oder `isinstance(x, RabbitMQBroker)`
   zur Laufzeit machen? (Audit klärt.)
2. **Öffentliche Re-Exports:** `from swarmq import SwarmQ, ...` —
   welche Symbole exponiert das Package-Top-Level (`__init__.py`)?
   Ein `__getattr__`-Pattern dort würde das bestehende Benutzer-API
   erhalten.
3. **Benchmark-Strenge:** Absolute vs. relative Schwelle (Plan 001
   hat bereits auf relativ umgestellt).

## Branch-Strategie

Eigener Branch `fix/import-laziness`, abgezweigt von `refactor/cli-split`
(oder von `main` nach dessen Merge). Sollte **nach** Plan 001 Unit 1
landen — die AST-basierten R-1a-Tests von Plan 001 bleiben unabhängig
grün; die `sys.modules`-basierten R-1b-Tests dieses Plans brauchen die
globale Laziness.

## Trigger

Dieser Plan ist **Stub** — aktiviert werden, sobald Plan 001 Unit 1
gelandet ist und der CLI-lokale Teil grün steht. Dann Unit 0 (Audit)
durchlaufen, Requirements und Units konkretisieren, RED+GREEN-Zyklen
wie gewohnt.
