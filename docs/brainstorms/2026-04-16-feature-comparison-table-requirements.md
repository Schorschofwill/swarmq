---
title: "Feature-Vergleichstabelle: SwarmQ vs. Python Task-Queue-Ökosystem"
type: feature-brief
status: deferred
date: 2026-04-16
audience: solo-developer (self-reflection)
deferred_at: 2026-04-16
deferred_reason: |
  Plan 006 derived from this brief was reviewed by five reviewers
  and deferred (see docs/plans/2026-04-16-006-...md frontmatter).
  Revisit when SwarmQ is try-able by external users (post-Phase 3c).
---

# Feature-Vergleichstabelle: SwarmQ vs. Python Task-Queue-Ökosystem

## Problem Frame

SwarmQ ist nach 2 Tagen Entwicklung bei Phase 3b.1 (von 5 geplanten Phasen). Der Entwickler braucht eine **ehrliche, datenbasierte Übersicht** wo SwarmQ im Vergleich zu etablierten Python-Task-Queues steht — explizit **nicht** als Marketing, sondern als Self-Reflection-Tool zur Steuerung der weiteren Roadmap. Aktuell existieren mehrere Recherche-Dokumente (`docs/evaluation-task-queues.md`, `docs/recherche-issues-*.md`), aber keine konsolidierte Side-by-Side-Sicht.

Das Doc steuert was zuerst gebaut wird — wenn z.B. drei der sechs Konkurrenten kein Workflow-Composition haben, ist das ein Differenzierungs-Punkt, kein Lückenfüller.

## Goal

Eine **lokal öffenbare HTML-Datei** (`docs/comparison.html`) die SwarmQ ehrlich gegen 6 etablierte Libraries stellt. Selbstkritisch: SwarmQ-Spalte zeigt explizit fehlende Features als "📅 geplant Phase X" oder "❌ nicht im Scope" — niemals geschönt.

## Out of Scope

- Marketing-Polish, Hero-Section, Logo, Animations
- Public-README-Embed (kommt später falls überhaupt)
- Interaktive Sortier-/Filter-Funktionen
- Performance-Benchmark-Reproduzierbarkeit (Zahlen aus eigenen `benchmarks/` + öffentlich dokumentierte Werte anderer)
- Live-Daten von GitHub-API (Stars/Releases sind statisch zum Erstellungszeitpunkt)

## Stakeholder & Audience

- **Primary:** Georg (Entwickler) als Self-Reflection-Tool
- **Secondary (latent):** falls SwarmQ extern publiziert wird, kann das Doc Basis für ein gepoliertes README sein — aber das ist Phase 5+, nicht jetzt

## Compared Libraries (7)

| # | Library | Warum drin |
|---|---|---|
| 1 | **Celery** | Marktführer, 25k Stars, Referenz-Vergleich |
| 2 | **TaskIQ** | Async-first Konkurrent, ähnliche Generation wie SwarmQ |
| 3 | **Dramatiq** | Globales Rate-Limiting, klare API — direkter Konkurrent für die Phase-3b-Features |
| 4 | **arq** | Async, Redis-only, lightweight — überlapt im Zielgruppensegment |
| 5 | **RQ** | Redis Queue, 9k Stars, sync-Modell — wichtige Trennlinie |
| 6 | **Huey** | 5k Stars, multi-Backend, mature — bisher nicht recherchiert |
| 7 | **SwarmQ** | Self |

Bestehendes Material (`docs/evaluation-task-queues.md`) deckt 1-4 ab. **RQ und Huey** brauchen frische Recherche im Implementation-Schritt.

## Compared Axes (8)

Jede Achse wird als eigene Sub-Tabelle dargestellt — eine Mega-Tabelle wäre unleserlich.

1. **Reliability & Message-Garantien**
   At-most/at-least-once, ack-Semantik, Default-Sicherheit, Retry-Modell, DLQ, exactly-once-Story

2. **Performance**
   Dokumentierte oder gemessene Throughput-Zahlen, Latenz-Profil, Concurrency-Modell (Prozesse/Threads/asyncio/gevent), reale Bench-Daten aus `benchmarks/`

3. **Workflow-Features**
   chains, groups, chords, Pipelines, dynamische Sub-Tasks, Result-Weitergabe

4. **Komfort-Features**
   Rate-Limiting (per-Worker vs global), Lock/Mutex, Semaphore, Dedup/Unique-Tasks, Cancel, Progress-Tracking, Scheduler/Periodic

5. **Maturity & Ökosystem**
   Erstes Release, Alter, Stars (Crawl-Datum dokumentieren), letzter Release, aktive Contributors-Schätzung, dokumentierter Production-Use, Major-Sponsoren

6. **Backend-Support**
   Broker-Optionen (RabbitMQ/Redis/Postgres/SQS/...), Result-Backend-Optionen, Lock-In-Bewertung

7. **Async-Support**
   Native asyncio im Worker, Sync-Bridges, Concurrency-Hauptmechanismus

8. **Operations**
   Monitoring (eigenes UI?, Prometheus?), Metrics-Format, CLI-Reichweite, Doc-Qualität-Bewertung, Tutorials/Quickstart, Health-Endpoints

## Honesty Mechanics

Die Spalten-Inhalte folgen einer expliziten Symbol-Konvention die Selbstüberhebung verhindert:

| Symbol | Bedeutung |
|---|---|
| ✅ | Vorhanden, dokumentiert, mindestens eine reale Production-Story |
| 🟡 | Teilweise vorhanden oder mit signifikanten Caveats (Caveat als Tooltip oder Footnote) |
| 📅 | Geplant in Roadmap mit Phase-Referenz (nur für SwarmQ valide; alle anderen nutzen ✅/🟡/❌) |
| ❌ | Fehlt komplett oder explizit Out-of-Scope der Library |

**SwarmQ-Spalte folgt derselben Skala.** Phase 3b.2/3b.3/3c/4/5-Features kriegen `📅` mit Phase-Tag, **nicht** `✅`. Aktuelle Maturity-Zelle bleibt schmerzhaft ehrlich:
- Alter: 2 Tage
- Stars: 0 (nicht published)
- Production-Use: 0
- Tests: 448 (Differenzierungs-Punkt vs. mancher etablierter Library mit weniger Test-Coverage)

## Layout

Ein einzelnes HTML-File mit:
- Header: Crawl-Datum + Erklärung der Symbol-Konvention
- 8 Sub-Tabellen (eine pro Achse)
- Footer: Quellen-Liste (verlinkt zu `docs/evaluation-task-queues.md` und externen Library-Repos)

Kein CSS-Framework, kein JavaScript. Inline-CSS für minimale Lesbarkeit (zebra rows, sticky header per Tabelle, monospace für Zahlen-Spalten). Datei muss mit `firefox docs/comparison.html` lokal öffenbar sein, zero Build-Step.

## Data Sources

| Library | Primärquelle |
|---|---|
| Celery, TaskIQ, Dramatiq, arq | `docs/evaluation-task-queues.md` (existiert) + `docs/recherche-issues-*.md` |
| RQ, Huey | **Neu recherchieren** im Implementation-Schritt — separater Sub-Plan oder als Teil des Implementation-Plans |
| SwarmQ | Eigene `src/swarmq/`, `docs/plans/`, `benchmarks/comparison/results/` |
| Performance | Eigene `benchmarks/bench.py --repeat 5` Outputs (frisch generiert) + dokumentierte Zahlen anderer Libraries |
| Maturity | GitHub-Repos der Libraries (Stars-Crawl-Datum festhalten) |

## Acceptance Criteria

- [ ] HTML-File `docs/comparison.html` existiert
- [ ] Lokal mit Browser ohne Build-Step öffenbar
- [ ] 7 Library-Spalten (oder Zeilen — Layout-Entscheidung in Planning), 8 Achsen
- [ ] SwarmQ-Spalte enthält **mindestens 5 ehrliche Markierungen**: `📅`, `❌` oder `🟡` mit klarem Caveat — **nicht** überall `✅`
- [ ] Mindestens eine Achse zeigt SwarmQ als **schwächer** als die Konkurrenz (z.B. Maturity, Operations)
- [ ] Mindestens eine Achse zeigt SwarmQ als **gleichauf oder stärker** wenn empirisch belegbar (z.B. Test-Coverage, atomare Lua-Primitives)
- [ ] Quellen-Footer mit Crawl-Datum
- [ ] Symbol-Konvention im Header erklärt

## Decision Trail (resolved during brainstorming)

- **Audience:** Self-Reflection (nicht Public-Marketing) — bestimmt Polish-Level
- **Library-Set:** Top-7 (Celery + TaskIQ + Dramatiq + arq + RQ + Huey + SwarmQ) — RQ/Huey brauchen Neu-Recherche
- **Achsen-Set:** Alle 8 vorgeschlagenen Achsen — User akzeptierte volle Breite
- **Honesty-Mechanik:** 4-Symbol-Skala mit `📅` exklusiv für SwarmQ-Roadmap-Tags
- **Format:** Statisches HTML, lokal öffenbar, kein Framework

## Open Questions for Planning

- Layout: Achsen als Sub-Tabellen vertikal vs. ein einzelnes Mega-Grid? (Empfehlung: vertikal, je 1 Tabelle pro Achse)
- Wie tief gehen Caveats? (Footnote vs. inline kursiv vs. hover-Tooltip)
- RQ/Huey-Recherche: separater Mini-Plan oder als Teil des HTML-Plans?
- Performance-Achse: nur eigene Bench-Werte mit `--repeat 5`, oder auch Zahlen von anderen Libraries (deren Bench-Methoden inkompatibel sind)?

## Related Docs

- [docs/evaluation-task-queues.md](evaluation-task-queues.md) — bestehende Vergleichsmatrix für 4 Libraries
- [docs/recherche-issues-celery.md](recherche-issues-celery.md), `recherche-issues-{taskiq,dramatiq,arq}.md` — Issue-Patterns
- [docs/benchmark-broker-results.md](benchmark-broker-results.md) — Broker-Vergleich
- `benchmarks/bench.py` — eigene Benchmark-Suite mit `--repeat`-Support für Median-Werte
- `docs/plans/2026-04-16-005-feat-phase3b-rate-limiting-plan.md` — exemplarisch für aktuellen SwarmQ-Stand
