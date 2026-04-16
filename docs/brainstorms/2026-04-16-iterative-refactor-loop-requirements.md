---
title: "Iterative Evidence-Driven Refactor Loop für `src/swarmq/`"
type: feature-brief
status: deferred
date: 2026-04-16
audience: solo-developer (self-improvement of internal code quality)
deferred_at: 2026-04-16
deferred_reason: |
  Vier Document-Reviewer (coherence, scope-guardian, product-lens,
  feasibility) konvergieren auf "wrong work, wrong time":

  - **Product-Lens (P0)**: Phase 3b.2 (Lock), 3b.3 (Cancellation),
    Phase 4 (Workflows) werden den Worker-Code ohnehin neu strukturieren.
    Refactoring vor Feature-Completeness ist Procrastination — Bench zeigt
    SwarmQ ist auf Augenhöhe mit taskiq-rmq, nächster Adoption-Hebel ist
    Feature-Surface, nicht sauberer worker.py.
  - **Feasibility (P1)**: bench_realistic.py importiert NICHT aus
    src/swarmq/ (verifiziert) — misst nur aio_pika + valkey direkt.
    Das "5%-Regression"-Stopper-Kriterium ist unmessbar. Vor jedem
    Refactor müsste eine SwarmQ-using Bench-Suite gebaut oder die
    gelöschte Comparison-Suite reaktiviert werden.
  - **Scope-Guardian (P1)**: Loop-Infrastruktur (Sammeln/Auswählen/
    Verifizieren mit Stop-Kriterium) ist Overkill — ein Single-Pass
    liefert dasselbe ohne Ceremony.
  - **Coherence (P1)**: Stop-Kriterium und Success-Kriterium widersprechen
    sich; Loop-Termination nicht garantiert.

  Wiederbesuch sinnvoll nach Phase 4 (Workflows komplett) — dann ist die
  Worker-Architektur stabil und Refactoring lohnt sich, ohne dass es
  durch nachfolgende Features wieder invalidiert wird.
---

# Iterative Evidence-Driven Refactor Loop für `src/swarmq/`

## Problem Frame

Der SwarmQ-Code (~2.7k LOC, 19 Module) ist funktional komplett für Phase 3a/3b.1
und durch 478 Tests abgesichert. Es gibt keinen sichtbaren Performance-Engpass —
der gerade durchgeführte Library-Vergleich zeigt SwarmQ auf Augenhöhe mit
`taskiq-rmq` (gleicher Broker), bei `noop` sogar 4.8× schneller. Trotzdem ist
unklar, ob einzelne Funktionen **vereinfacht** werden können (Lesbarkeit,
weniger Duplikation, kleinere zyklomatische Komplexität) oder ob es **versteckte
Hotspots** gibt, die im Benchmark unter den globalen Kennzahlen verborgen
bleiben.

Das Refactoring darf das **Input/Output-Verhalten** der Funktionen nicht
ändern — die Public-API bleibt stabil, alle bestehenden Tests müssen grün
bleiben.

## Goal

Eine **iterative Evidenz-Schleife** etablieren, die in wiederholbaren Runden
Maintainability- und Performance-Findings sammelt und gezielt behebt — ohne
spekulative Mikro-Refactorings ohne messbaren Nutzen.

## Scope

**In-Scope** (refactor-Ziel):
- `src/swarmq/*.py` — alle 19 Python-Module der Library
  - Hot path: `worker.py` (719 Z.), `client.py` (392 Z.)
  - Mid path: `middleware.py`, `signals.py`, `app.py`, `task.py`
  - Helpers: `limits.py`, `serialization.py`, `logging.py`, `config.py`,
    `backoff.py`, `exceptions.py`, `registry.py`
  - Adapter: `broker/rabbitmq.py`, `backend/valkey.py`
  - CLI: `cli.py` (frisch, niedrigste Priorität — wenig Verbesserungs-Hebel)

**Out-of-Scope**:
- `src/swarmq/lua/*.lua` — Lua-Scripts, eigene Welt, separate Optimierungs-Logik
- `tests/` — Tests sind Verifikator, keine Refactor-Ziele (Coverage-Lücken werden
  ad-hoc geschlossen, wenn ein Refactoring sie braucht)
- `benchmarks/` — Mess-Werkzeuge, kein Library-Code
- Public-API-Signaturen (Funktions-Inputs/Outputs/Exceptions) — siehe Constraints

## Constraints

1. **Keine Verhaltensänderung an der Public-API**: Funktions-Signaturen,
   Rückgabewerte, geworfene Exception-Typen bleiben identisch.
2. **478 Tests müssen grün bleiben**: kein Refactoring lands ohne grüne
   Test-Suite (unit + integration + e2e).
3. **Benchmark-Regression ist ein Stopper**: vor/nach Vergleich gegen
   `benchmarks/bench_realistic.py` (oder Comparison-Suite, falls aktiviert) —
   Throughput-Regression >5% blockt den PR.
4. **Ein PR pro Runde**: jede Iteration ist ein einzelner, reviewbarer Commit/PR
   mit klarem Vorher/Nachher-Eintrag.

## Iterationen — Runde-Definition

Jede Runde besteht aus 4 Schritten:

1. **Sammeln**: parallel
   - `compound-engineering:review:maintainability-reviewer` über alle In-Scope-Module
   - `compound-engineering:review:performance-reviewer` über alle In-Scope-Module
   - Profiler-Lauf (`pyinstrument` oder `py-spy`) gegen
     `benchmarks/bench_realistic.py` für Hotspot-Liste
2. **Auswählen**: Top-3 Maintainability-Findings (P1/P2) **und** Top-3
   Performance-Hotspots in einem PR bündeln. Findings unterhalb dieser Schwelle
   sammelt sich für die nächste Runde.
3. **Umsetzen**: Refactoring implementieren, Tests laufen lassen, Benchmark-Diff
   protokollieren.
4. **Verifizieren**: alle 478 Tests grün, Benchmark-Diff ohne Regression >5%,
   PR mergen, nächste Runde planen.

## Stop-Kriterium

Die Schleife läuft, **solange mindestens eines der beiden Signale gegeben ist**:

- `maintainability-reviewer` oder `performance-reviewer` melden noch P1- oder
  P2-Findings über In-Scope-Module
- Profiler zeigt mindestens einen Hotspot, der **>10 % der Bench-Zeit**
  verbraucht und nicht durch eine externe Bibliothek (aio-pika, valkey)
  bestimmt ist

Wenn beide Signale weg sind → natürlicher Stop. Bewusst kein Hard-Limit auf
Runden — die Schleife endet, wenn der Code "leer-refactored" ist.

## Success Criteria

Pro Runde:
- Mindestens 1 P1-Maintainability-Finding behoben **oder** ≥5 % Throughput-Gewinn
  in mindestens einem Benchmark-Szenario
- Alle 478 Tests grün
- Keine Benchmark-Regression >5 % in irgendeinem Szenario

Nach Abschluss der Schleife (insgesamt):
- Keine offenen P1/P2-Findings über In-Scope-Module
- Kein Profiler-Hotspot >10 % Bench-Zeit (ohne externe Lib)
- Dokumentierter Vorher/Nachher-Bericht (Throughput, P99, LOC, größte Funktion)

## Sicherheitsnetz

- **Bestehende 478 Tests** (unit + integration + e2e) als Hauptverifikator
- **`benchmarks/bench_realistic.py`** als Performance-Baseline pro Runde
- **Optional pro Runde**: wenn ein Refactoring an einer Public-API-Funktion
  ansteht (z. B. `SwarmQ.schedule`, `Task.run`, `Middleware`), kann ad-hoc ein
  `hypothesis` Property-Test ergänzt werden, bevor der Umbau passiert
- **Kein Big-Bang Property-Test-Setup vorab** — die 478 bestehenden Tests
  decken die Public-API-Verhalten gut ab; Property-Tests werden ad-hoc, nicht
  als Sicherheits-Voraussetzung, hinzugefügt

## Risks

| Risiko | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Maintainability-Reviewer findet vor allem Style-Findings ohne echten Wert | hoch | gering | Triagieren auf P1/P2; P3 ignorieren |
| Performance-Reviewer schlägt Mikro-Optimierungen vor, die im Bench nicht messbar sind | mittel | gering | Profiler-Evidenz vor Reviewer-Vorschlag — Reviewer-only-Findings ohne Hotspot werden nicht umgesetzt |
| Refactoring eines Hot-Path-Moduls (worker.py) verschiebt Hotspots, sodass die nächste Runde komplett anders aussieht | mittel | mittel | Pro Runde nur 3 Perf-Hotspots — kleine Änderungen, klare Diff-Lesbarkeit |
| Subtile Verhaltensänderung trotz Tests grün (z. B. Timing-Sensitivität in Worker-Pipeline) | gering | hoch | Integration- und e2e-Tests fangen das ab; bei Public-API-Änderung Property-Test ad-hoc dazu |
| Schleife läuft endlos, weil Reviewer immer wieder neue Findings melden | mittel | mittel | Pro Runde nur die Top-3; Findings die zwei Runden in Folge nicht angegangen werden, werden bewusst ge­paßt (Backlog statt Schleife) |

## Open Questions (für Planning)

- Welcher Profiler ist Standard — `pyinstrument`, `py-spy`, `cProfile`?
  (Empfehlung: `pyinstrument` für reines Python, `py-spy` falls Coroutine-Trace
  gebraucht wird.)
- Wie wird der "Vorher/Nachher"-Report pro PR formatiert? (Empfehlung: kompakter
  Block in der PR-Description: `Throughput: X → Y t/s (+/- %)`, `P99: X → Y ms`,
  `LOC: X → Y`.)
- Soll die Comparison-Suite (`benchmarks/comparison/`, derzeit nur auf Branch
  `feat/benchmark-comparison-suite`) reaktiviert werden, um vs. taskiq-rmq zu
  vergleichen, oder reicht `bench_realistic.py` alleine?

## Non-Goals

- Public-API-Änderung (Signaturen, Exception-Typen, Rückgabewerte)
- Architektur-Umbauten (kein Modul-Split, kein Pattern-Wechsel — nur
  Vereinfachung innerhalb bestehender Strukturen)
- Lua-Script-Refactoring
- Test-Refactoring außerhalb der ad-hoc-Property-Test-Ergänzungen
- Phase 3b.2/3b.3-Features (Lock, Cancellation) — diese laufen separat

## Notes

- Begründung für "Beides (gemischter Pass)" als Ziel: Maintainability allein wäre
  niedrig-Wert (Code ist schon kompakt), Performance allein wäre vorzeitig (kein
  Engpass im Benchmark). Die Kombination nutzt den gleichen Sammel-Schritt für
  beide Ziele und reduziert den Kontextwechsel pro Runde.
- Begründung für "iterativ statt Big-Bang": Hot-Path-Refactoring (worker.py)
  bewegt Hotspots — Big-Bang würde Phase 2 zwingen, neu zu profilen. Kleine
  Runden mit kombiniertem Maint+Perf-Pass sind robuster.
