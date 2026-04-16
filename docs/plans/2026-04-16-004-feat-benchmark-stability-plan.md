---
title: "feat: Benchmark-Stabilität via Repeat + Median-Aggregation"
type: feat
status: active
date: 2026-04-16
baseline: /tmp/baseline_pubsub.csv
---

# feat: Benchmark-Stabilität via Repeat + Median-Aggregation

## Overview

Einzelne Bench-Runs können bis 16–22 % vom Median abweichen (empirisch aus N=10-Baseline). Der existierende globale Warm-up-Block (`benchmarks/bench.py:1167-1178`) eliminiert Cold-Start bereits. Die verbleibende Streuung ist natürlicher Jitter aus Docker, OS-Page-Cache, GC und TCP-Timing — kein Cold-Start.

Fix: Optionaler `--repeat N` Flag + Median-Aggregation über N Runs pro Szenario. Ohne zusätzlichen Warm-up (redundant zum bestehenden), ohne neue Dataclass (NamedTuple reicht). Minimaler Scope: zwei Implementation Units statt drei.

## Problem Frame

Ein einzelner Bench-Run beantwortet "wie schnell war es jetzt gerade?", nicht "wie schnell läuft das System stabil?". Für den nächsten SwarmQ-vs-TaskIQ-Vergleich oder Phase-zu-Phase-Regressions-Check brauchen wir Zahlen mit sichtbarer Streuung.

## Baseline Evidence

10 sequentielle Runs `pubsub_notify -n 5000` mit aktivem Warm-up:

| | RabbitMQ | Valkey |
|---|---:|---:|
| Median | 7.524 msg/s | 119.356 msg/s |
| Min | 6.364 | 108.274 |
| Max | 8.004 | 127.781 |
| Stdev | 528 | 5.272 |
| CV (stdev/mean) | 7,1 % | 4,4 % |
| Range (max−min)/median | 21,8 % | 16,3 % |

Rohdaten: `/tmp/baseline_pubsub.csv`

Interpretation:
- **Valkey CV 4,4 %:** unter Adversarial-Schwelle (5 %). Die ursprünglich beobachteten −18 % zwischen zwei Runs waren ein Outlier-Artefakt eines N=2-Vergleichs
- **Range 16–22 %:** Einzel-Runs können deutlich daneben liegen — Median über N Runs glättet das sichtbar
- **Cold-Start ausgeschlossen** — Warm-up ist aktiv, Varianz bleibt trotzdem

## Requirements Trace

- R1. CLI-Flag `--repeat N` (Default 1, kein Default-Mehrverbrauch)
- R2. `--repeat N` mit `N >= 1`, durchgesetzt durch argparse custom type (argparse hat keinen nativen min-constraint)
- R3. Bei `N >= 2`: Jedes Szenario läuft N mal, Ergebnisse werden als Median + Min + Max über Runs aggregiert
- R4. Pro-Run-Output bleibt sichtbar (nicht nur Final-Summary) — Debug-Sicht auf Varianz
- R5. Latenzen (P50/P95/P99) werden pro Run separat berechnet; aggregiert wird der Median der Per-Run-Perzentile
- R6. Bestehende CLI-Flags und Output-Format bei `--repeat 1` unverändert
- R7. Pre-Fill-Logik (z. B. `enqueue_throughput` vor `dequeue_throughput`) bleibt pro Run erhalten — der Repeat-Loop wrappt nur den Measurement-Call, nicht das Szenario-Setup

## Scope Boundaries

- Nur `benchmarks/bench.py`, keine anderen Files
- **Kein zusätzlicher Warm-up** — global ist bereits da, pro-Szenario wäre redundant
- **Keine AggregateResult-Dataclass** — `typing.NamedTuple` mit 4 Feldern reicht
- Keine neuen externen Dependencies (`statistics` ist stdlib)
- Default `--repeat 1` (legacy/kompatibel): Wall-Clock steigt nur wenn explizit mehr gewünscht

### Deferred to Separate Tasks

- CI-Integration: Benchmark-Smoke in GitHub Actions mit Trend-Dashboard
- JSON-Export der Aggregate-Daten
- Varianz-basierter Fail-Gate ("warn wenn CV > 10 %")
- Runtime-profiling um Jitter-Ursache tiefer zu analysieren

## Context & Research

### Relevant Code and Patterns

- `benchmarks/bench.py:122` — `BenchResult` Dataclass, Feldname `throughput_msg_s` (NICHT `rps`)
- `benchmarks/bench.py:~970` — `print_result()` Output-Funktion
- `benchmarks/bench.py:~1020` — `run_scenario()` Orchestrator, zentrale Stelle für Repeat-Loop
- `benchmarks/bench.py:~1140` — `main()` CLI mit argparse
- `benchmarks/bench.py:1167-1178` — **existierender globaler Warm-up-Block**, bleibt unverändert
- `tests/integration/test_benchmark_smoke.py` — Smoke-Test, muss nach Fix weiter passen
- `tests/unit/test_benchmark_guard.py` — AST-Guards, nicht betroffen (Repeat-Loop öffnet keine neuen Channels)

### Institutional Learnings

- Session-History: Benchmark-Fix-Kette hat `_bench_channel`, `_ensure_connection`, `_BENCH_HANG_TIMEOUT`, AST-Guards etabliert. Repeat-Layer setzt auf diesen Primitiven auf, nicht darunter
- Document-Review (2026-04-16) hat den ursprünglich geplanten Warm-up als Duplikat + falsche Prämisse identifiziert. Dieser Plan adressiert alle 11 Findings durch Scope-Reduktion

## Key Technical Decisions

- **Default `--repeat 1`, nicht 3:** Single-Run bleibt Default-Verhalten. Der User wählt bewusst, wann Repeat lohnt. Keine implizite 3×-Laufzeit für jeden bench-Aufruf
- **NamedTuple statt Dataclass:** `AggregateStats` als `NamedTuple` mit `median/min/max/count` pro Metrik. Keine separate Klasse, keine Feldnamen-Diskussion, lokal im File definiert
- **Repeat-Loop wrappt nur Measurement-Call:** Pre-Fill-Logik (`rmq.enqueue_throughput()` vor `rmq.dequeue_throughput()`, eigene Queues in `pubsub_notify`) bleibt pro Run. Das repliziert das originale Szenario-Setup faithful
- **Latenzen: Median der Per-Run-Perzentile:** pro Run P50/P95/P99, dann Median über Runs. Nicht pooled — sagt mehr über Stabilität aus
- **Kein expliziter Warm-up in Repeat-Layer:** global existiert bereits. Erste Repeat-Iteration ist nicht "wärmer" als Runs 2-N weil Connection + Caches schon warm sind

## Open Questions

### Resolved During Planning

- **Default-Repeat:** 1 (kompatibel; `--repeat 3` oder `--repeat 5` manuell bei Bedarf)
- **Warm-up-Integration:** keiner neuer; Adversarial-Review hat gezeigt dass globaler Warm-up ausreicht
- **Aggregation:** Median + Min + Max (robust bei kleinem N, zeigt Spannweite)
- **Legacy-Pfad:** der `N == 1`-Branch IST der Legacy-Pfad — unverändertes Verhalten

### Deferred to Implementation

- Exaktes Output-Format der Aggregate-Zeile (ein-Zeilen-Summary vs mehrzeilig) — Style-Entscheidung bei Implementation

## Implementation Units

- [ ] **Unit 1: `--repeat N` Flag + Repeat-Loop + Aggregation**

**Goal:** `--repeat N` steuert wie oft jedes Szenario ausgeführt wird. Bei `N > 1` werden die Einzelergebnisse aggregiert (Median/Min/Max über Runs).

**Requirements:** R1, R2, R3, R5, R6, R7

**Dependencies:** Keine

**Files:**
- Modify: `benchmarks/bench.py`
- Test: `tests/unit/test_benchmark_aggregate.py` (neu, pure-Python, kein Docker)

**Approach:**
- `argparse`: `--repeat/-r` mit custom `positive_int` type-function, die `argparse.ArgumentTypeError` bei `< 1` wirft
- `AggregateStats = NamedTuple("AggregateStats", [("median", float), ("min", float), ("max", float), ("count", int)])`
- Reine Funktion `_aggregate_rps(results: list[BenchResult]) -> AggregateStats` — liest `r.throughput_msg_s` (NICHT `rps`)
- Reine Funktion `_aggregate_latencies(results: list[BenchResult]) -> dict | None` — pro Run P50/P95/P99 berechnen, davon Median über Runs
- Orchestrator-Anpassung in `run_scenario()`: vorher stand `r = await rmq.enqueue_throughput(...)` — jetzt:
  - wenn `repeat == 1`: wie bisher (gleicher Code-Pfad)
  - wenn `repeat > 1`: Liste sammeln, `for _ in range(repeat): r = await rmq.enqueue_throughput(...); print_result(r); results.append(r)`; am Ende `print_aggregate(results, _aggregate_rps(results), _aggregate_latencies(results))`
- Pre-Fill-Calls (z. B. `await rmq.enqueue_throughput(...)` vor `await rmq.dequeue_throughput(...)` in Szenario 2) bleiben im Repeat-Loop — das ganze Szenario-Setup wird pro Run wiederholt
- `purge()` bleibt vor dem Szenario (nicht pro Run) — der `_ensure_connection`-Probe ist robust genug

**Patterns to follow:**
- `statistics.median`, `.min()`, `.max()` aus stdlib
- Code-Style in bench.py: ASCII + Kommentare wie bestehender Code
- Testdateien in `tests/unit/` sind pure-Python ohne Docker (vgl. `test_benchmark_guard.py`)

**Test scenarios:**
- Happy path: `_aggregate_rps([r1, r2, r3])` mit `throughput_msg_s=[100, 200, 150]` → `AggregateStats(median=150, min=100, max=200, count=3)`
- Edge case: `_aggregate_rps([r1])` → `AggregateStats(median=X, min=X, max=X, count=1)` mit demselben X (kein Fehler)
- Happy path Latenzen: 3 BenchResults mit `latencies=[...]` → `_aggregate_latencies` gibt dict mit `p50_median/p95_median/p99_median`
- Edge case Latenzen: BenchResult ohne `latencies` (leere Liste) → `_aggregate_latencies` gibt `None`
- Edge case Mischung: ein Run ohne Latenzen + zwei mit → `_aggregate_latencies` ignoriert leere und rechnet über die nicht-leeren, oder gibt `None` (Implementation entscheidet — konservativ `None`)

**Test expectation — für Orchestrator-Änderung in `run_scenario()`:** keine neuen Unit-Tests; Verhalten wird durch Smoke-Test in Unit 2 abgedeckt. Reine-Funktion-Tests stehen hier.

**Verification:**
- `python bench.py -n 500 -s enqueue_throughput --repeat 3` zeigt 3 `Throughput:`-Zeilen + 1 Aggregate-Zeile
- `python bench.py --repeat 1` läuft wie bisher
- `python bench.py --repeat 0` wird von argparse abgelehnt
- `uv run pytest tests/unit/test_benchmark_aggregate.py` grün

---

- [ ] **Unit 2: Aggregate-Output + Smoke-Test-Update**

**Goal:** Eine kompakte Aggregate-Zeile nach den N Runs in der Per-Run-Ausgabe; Smoke-Test verifiziert das Repeat-Verhalten End-to-End.

**Requirements:** R3, R4

**Dependencies:** Unit 1

**Files:**
- Modify: `benchmarks/bench.py`
- Modify: `tests/integration/test_benchmark_smoke.py`

**Approach:**
- Neue Funktion `print_aggregate(name: str, rps_agg: AggregateStats, lat_agg: dict | None)`:
  - Eine Zeile: `AGGREGATE [3 runs] msg/s: median=X min=Y max=Z`
  - Falls `lat_agg` nicht None: zusätzlich `P50_med=.. P95_med=.. P99_med=..`
- `print_aggregate` wird in `run_scenario()` NACH der Repeat-Schleife aufgerufen (siehe Unit 1)
- Final-Tabelle (`GEWINNER PRO SZENARIO`) bleibt unverändert: nimmt den letzten `BenchResult` (der in `results` mit `.extend(...)` appended wird) oder — bei Aggregation — schreibt median als Haupt-msg/s-Wert. Entscheidung bei Implementation; Default: letztes Ergebnis gewinnt (Status quo)
- Neuer Smoke-Test `test_benchmark_repeat_mode_emits_aggregate`:
  - `python bench.py -n 300 -s enqueue_throughput --repeat 2`
  - assert exit 0
  - assert Output enthält genau 2 `Throughput:`-Zeilen für RabbitMQ (= 2 Runs pro Broker)
  - assert Output enthält mindestens 1× `AGGREGATE`
  - assert kein `Connection closed`, kein `TimeoutError`, kein `Traceback`
- Bestehender Smoke-Test `test_benchmark_all_scenarios_small_load` läuft weiter (`--repeat` default = 1, Verhalten unverändert)

**Test scenarios:**
- Happy path (neuer Smoke): `--repeat 2 -n 300 -s enqueue_throughput` → exit 0, 2 Throughput-Zeilen pro Broker, 1 AGGREGATE-Zeile
- Regression (bestehender Smoke, unverändert): `--repeat` nicht gesetzt → default 1 → kein AGGREGATE-Marker

**Verification:**
- `uv run pytest tests/integration/test_benchmark_smoke.py` grün
- Manuell: `python bench.py -n 5000 -s pubsub_notify --repeat 10` zeigt sichtbar die Baseline-Varianz (CV 4-7 %)

## System-Wide Impact

- **Interaction graph:** `run_scenario()` wird zum Repeat-Orchestrator. Keine neuen Integrationen — alle Calls gehen über `_bench_channel`, `_ensure_connection`, `_BENCH_HANG_TIMEOUT`
- **Error propagation:** Fehler in einem Run propagieren und brechen das Szenario. Kein Silent-Retry — Instabilität wird sichtbar, nicht versteckt
- **State lifecycle risks:** Pre-Fill + Measurement bleiben pro Run zusammen. `_ensure_connection` fängt Stale-Connections ab (schon etabliert)
- **API surface parity:** `--repeat 1` (Default) = Status quo. Keine externen Consumer bekannt
- **Unchanged invariants:** globaler Warm-up, `BenchResult`, `print_result`, `purge()`-Verhalten, AST-Guards — alles unverändert

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| User erwartet implizit gemittelte Zahlen und sieht einzel-Run mit `--repeat 1` | Default 1 = alter Code-Pfad; User entscheidet bewusst für Aggregation |
| Neue Smoke-Test-Parameter flaky (n=300, 2 Runs) | n=300 läuft in ~1-2s pro Run; 240s Timeout großzügig |
| Field-Name-Drift (ein Reviewer hat `rps` angenommen obwohl es `throughput_msg_s` heißt) | Dieser Plan nennt den korrekten Namen explizit; Unit-Tests fail-fast auf Tippfehler |
| Repeat-Loop macht pre-existing Connection-Races sichtbarer | `_ensure_connection`-Probe + AST-Guards sind der bestehende Schutz |

## Sources & References

- **Code:** `benchmarks/bench.py`, `tests/integration/test_benchmark_smoke.py`, `tests/unit/test_benchmark_guard.py`
- **Baseline-Daten:** `/tmp/baseline_pubsub.csv` (10 Runs pubsub_notify mit aktivem Warm-up)
- **Document-Review 2026-04-16:** 4 Reviewer, 14 Findings. Critical: Cold-Start-Prämisse war falsch; globaler Warm-up existiert bereits. Dieser Plan adressiert alle 11 actionable Findings durch Scope-Reduktion
- **Ersetzt:** ursprüngliche Plan-Version (separater Warm-up + AggregateResult-Dataclass + 3 Units), die durch Baseline-Evidence widerlegt wurde
