---
title: "feat: Honest Feature-Comparison HTML Table"
type: feat
status: deferred
date: 2026-04-16
origin: docs/brainstorms/2026-04-16-feature-comparison-table-requirements.md
deferred_at: 2026-04-16
deferred_reason: |
  Five-reviewer ce:review converged on DEFER. Convergence summary:
  - Product-lens: premise-mismatch (1-reader artefact built like
    public-marketing tool); opportunity cost vs Phase 3c (CLI) and
    3b.2 (Lock+Dedup); plan advances no roadmap phase.
  - Scope-guardian: subtraction test passes — direct HTML edit meets
    all 10 requirements without tools/ + generator + asserts.
  - Adversarial: honesty-asserts are epistemically empty (check
    symbol distribution, not symbol truth) and "self-reflection"
    claim is unargued — no roadmap decision named that hinges on it.
  - Feasibility: tools/ import path not in pyproject.toml; R6/R7
    assertions ambiguous; performance sub-columns clash with
    Cell NamedTuple shape.
  - Coherence: R7 "Lua atomicity" not anchored in any of the 8 axes;
    "self-reported vs independently measured" perf split has no
    data backing for RQ/Huey.

  Decision: defer until SwarmQ is try-able by a stranger (Phase 3c
  CLI complete). At that point a comparison table writes itself
  with much less effort and serves a real audience.

  Review artifacts:
  .context/compound-engineering/document-review/plan006-{coherence,
  scope, feasibility, adversarial, product}.json
---

# feat: Honest Feature-Comparison HTML Table

## Overview

Generate a single self-contained HTML file (`docs/comparison.html`) that places SwarmQ next to six other Python task-queue libraries across eight feature axes. The file is built once from a deterministic Python script and is meant to be opened locally with a browser. Honest by construction: a 4-symbol scale separates `✅ shipped` from `📅 planned` (only valid for SwarmQ), and the acceptance criteria force the SwarmQ column to surface its weak spots, not bury them.

The work breaks down into three concrete steps: fill the two missing library cells (RQ + Huey), assemble all data in a structured form, and render that data as HTML.

## Problem Frame

The brainstorm doc (`docs/brainstorms/2026-04-16-feature-comparison-table-requirements.md`) is the source of truth here — read it first if anything below is unclear. The shorter version: SwarmQ is two days old, mid-Phase-3b. Without a side-by-side view it's easy to either over- or under-estimate where the project stands. The HTML file is a self-reflection tool, not a marketing artefact.

## Requirements Trace

- **R1.** A single file `docs/comparison.html` opens in a browser without a build step (no JS, no external CSS, no fetch).
- **R2.** Seven columns: Celery, TaskIQ, Dramatiq, arq, RQ, Huey, SwarmQ. Order: most established → newest → SwarmQ on the right.
- **R3.** Eight sections (one HTML `<table>` each): Reliability, Performance, Workflows, Comfort Features, Maturity, Backend Support, Async Support, Operations.
- **R4.** Symbol convention rendered in the page header: `✅ / 🟡 / 📅 / ❌` with explicit semantics. `📅` is only ever used in the SwarmQ column.
- **R5.** SwarmQ column contains **at least 5** non-`✅` cells (`📅` / `🟡` / `❌`) — the acceptance test for "honest".
- **R6.** **At least one axis** shows SwarmQ as weaker than at least three other libraries (Maturity is the obvious candidate; Operations is the secondary).
- **R7.** **At least one axis** shows SwarmQ at parity or stronger when empirically defensible (test count + atomic Lua primitives are the candidates).
- **R8.** Footer lists data sources per library with crawl date.
- **R9.** RQ and Huey have no existing in-repo research — both need to be sourced before the HTML can be assembled.
- **R10.** Performance numbers for SwarmQ come from a fresh `bench.py --repeat 5` run; numbers for other libraries are quoted with their source URL.

## Scope Boundaries

- **In scope:** the HTML file, a Python generator script that produces it, the data file the generator reads.
- **Out of scope:** marketing polish, hero section, animations, CSS frameworks, JavaScript interactivity (sort/filter), GitHub Actions automation to keep stars current, embedding into README.
- **Deliberately not parameterised:** which libraries appear, the symbol scale, the axis list — all locked by the brainstorm.

### Deferred to Separate Tasks

- README integration of the table (Phase 5+ once SwarmQ is more mature).
- Dynamic re-crawl of star counts (manual update for now; the file carries a crawl date).
- Inclusion of further libraries (Procrastinate, Faktory, etc.) — current set is the brainstorm-locked seven.

## Context & Research

### Existing material that must be reused

- `docs/evaluation-task-queues.md` — already covers Celery, TaskIQ, Dramatiq, arq across multiple axes. Primary input for those four columns.
- `docs/recherche-issues-{celery,taskiq,dramatiq,arq}.md` — known pain-points per library. Source for the "honest weaknesses" cells of each non-SwarmQ column.
- `docs/benchmark-broker-results.md` and `benchmarks/comparison/results/` — performance numbers for SwarmQ.
- `docs/plans/2026-04-16-001-feat-phase3a-middleware-signals-plan.md`, `…-005-feat-phase3b-rate-limiting-plan.md` — current SwarmQ feature surface.
- `src/swarmq/` — authoritative truth about which features are actually shipped vs. planned.

### Material that must be created

- A condensed feature/issue summary for **RQ** (`https://github.com/rq/rq`) and **Huey** (`https://github.com/coleifer/huey`) following the same shape as the existing `recherche-issues-*.md` files but compact (single section per library).

### Institutional learnings to apply

- `docs/solutions/patterns/measure-before-planning-variance-claims-2026-04-16.md` — for the Performance axis: cite eigene Bench-Werte als Median über N=5 läufe, nicht als Einzelmessung. Sonst ist die Spalte nicht ehrlich, sondern lucky.

## Key Technical Decisions

- **Layout: 8 vertical tables, libraries as columns.** Eine Mega-Tabelle wäre für 7×8 Zellen mit unterschiedlichen Sub-Feldern unleserlich. Pro Achse eine eigene `<table>` mit konsistentem Spalten-Header. Der Achsen-Header ist sticky innerhalb der jeweiligen Tabelle.
- **Generator als Python-Script, nicht direkt HTML editieren.** Daten und Präsentation getrennt: `tools/build_comparison.py` liest `tools/comparison_data.py` (oder ein YAML/JSON) und schreibt `docs/comparison.html`. Vorteil: Daten-Korrekturen brauchen kein HTML-Wissen, und die Honesty-Acceptance-Tests können direkt auf den Daten laufen.
- **Caveats als Tooltip via `<abbr title="…">` oder Inline-Footnote.** Kein JS — `<abbr>` ist nativ, kein Layout-Bruch. Längere Caveats wandern in eine pro-Achse-Footnote-Liste unter der jeweiligen Tabelle.
- **Symbol-Skala wie im Brainstorm:** `✅` shipped + production-real, `🟡` partial/caveat, `📅 phase X` planned (nur SwarmQ), `❌` fehlt/out-of-scope. Eine Legende im Page-Header.
- **Performance-Achse: zwei Sub-Spalten pro Library** — "Self-reported / docs" und "Independently measured (where available)". Verhindert dass dokumentierte Marketing-Zahlen direkt mit eigenen Median-Werten verglichen werden.
- **Maturity-Spalten zeigen absolute Zahlen + Crawl-Datum** (`Stars: 25.4k @ 2026-04-16`). Niemals "viel/wenig".
- **Tests-Coverage als Spalte in Maturity** (eigener Defensive-Stand): SwarmQ 448, andere zur Recherche-Zeit. Wenn die anderen mehr haben, steht es so da.
- **Generator-Script ist deterministisch und idempotent:** mehrfaches Ausführen produziert byte-identische HTML-Datei (einfach diffbar, gut für PR-Review).
- **Acceptance-Tests als Asserts im Generator:** das Script crashed wenn R5/R6/R7 verletzt werden. Macht "Ehrlichkeit" maschinell prüfbar.

## Open Questions

### Resolved during planning

- **Layout:** vertikale Sub-Tabellen, eine pro Achse (siehe Key Decisions).
- **Caveat-Tiefe:** `<abbr title>` für kurze Caveats, Per-Tabelle-Footnotes für längere.
- **RQ/Huey-Recherche:** als Implementation-Unit im selben Plan (Unit 1), kein separater Mini-Plan — Aufwand pro Library ist überschaubar (~30 min Recherche + Verdichtung).
- **Performance-Vergleichbarkeit:** zwei Sub-Spalten ("Self-reported", "Independently measured") + explizite Quellen-Tags.

### Deferred to implementation

- Format der Daten-Datei (`comparison_data.py` mit Dataclasses vs. `comparison_data.yaml` mit Schema). Beides funktioniert; Entscheidung beim Schreiben.
- Genaue CSS-Token-Werte (Farben, Font, Padding). Stilistisch, nicht semantisch — beim HTML-Bau festlegen.

## Implementation Units

- [ ] **Unit 1: Recherche-Verdichtung für RQ und Huey**

**Goal:** Zwei neue, kompakte Recherche-Notizen — eine pro Library — die für jede der 8 Vergleichs-Achsen mindestens eine belegte Aussage liefern.

**Requirements:** R9, R8 (Quellen-Footer braucht Crawl-Datum)

**Dependencies:** Keine

**Files:**
- Create: `docs/recherche-rq.md`
- Create: `docs/recherche-huey.md`

**Approach:**
- Format spiegelt `docs/recherche-issues-celery.md` Struktur, aber kompakter: ~150-300 Zeilen pro File statt der 1000+ in den existing recherches
- Jede Achse einmal beantworten (Reliability, Performance, etc.) mit Verweis auf Quellen-URL und Crawl-Datum
- Stars-Count, letzter Release, Contributor-Count zum Crawl-Zeitpunkt festhalten — diese landen direkt in der Maturity-Achse
- Quelle: GitHub-Repo der jeweiligen Library, deren offizielle Docs, ggf. ein bekannter "production user" Hinweis

**Patterns to follow:**
- `docs/evaluation-task-queues.md` Sektion 4 (arq) als Längen-Vorbild — kompakter als die `recherche-issues-*.md` aber stuktiert
- Tabellen-Snippets, keine Prosa-Essays

**Test scenarios:** keine — reine Recherche, kein ausführbarer Code

**Test expectation: none — pure documentation, no code**

**Verification:**
- `docs/recherche-rq.md` und `docs/recherche-huey.md` enthalten mindestens je eine Aussage zu jeder der 8 Achsen
- Crawl-Datum dokumentiert
- Mindestens 3 Quellen-Links pro File

---

- [ ] **Unit 2: SwarmQ-Performance-Daten frisch generieren**

**Goal:** Frische Median-msg/s-Werte aus `bench.py --repeat 5` einfangen, damit die Performance-Achse für SwarmQ aktuell ist (nicht aus alten Logs).

**Requirements:** R10

**Dependencies:** Keine

**Files:**
- Modify: keine — Output landet in `benchmarks/comparison/results/` (gitignored)
- Create: `docs/comparison-perf-snapshot.md` (kleine Markdown-Datei mit den extrahierten Median-Zahlen + Run-Datum + Hardware-Notiz)

**Approach:**
- `cd benchmarks && python bench.py --repeat 5 -n 10000 -s all` (n=10000 als Kompromiss zwischen Realismus und Wall-Clock-Zeit)
- Ergebnisse aus stdout extrahieren, Median-Spalte je Szenario in `docs/comparison-perf-snapshot.md` festhalten
- Hardware (CPU-Modell, RAM, Docker-Setup) als Footnote — macht spätere Reproduktionen ehrlich vergleichbar

**Test expectation: none — measurement step**

**Verification:**
- `docs/comparison-perf-snapshot.md` existiert mit min/median/max je Szenario je Broker
- Run-Datum und Hardware dokumentiert

---

- [ ] **Unit 3: Daten-Datei + HTML-Generator + comparison.html**

**Goal:** Strukturierte Daten in `tools/comparison_data.py`, deterministisches Build-Script `tools/build_comparison.py`, Output `docs/comparison.html`. Acceptance-Asserts laufen im Generator.

**Requirements:** R1, R2, R3, R4, R5, R6, R7, R8

**Dependencies:** Unit 1 (RQ/Huey-Daten), Unit 2 (Performance-Snapshot)

**Files:**
- Create: `tools/__init__.py` (leer)
- Create: `tools/comparison_data.py` — Dataclasses: `Library`, `AxisCell`, `Axis` als Module-Level-Konstanten
- Create: `tools/build_comparison.py` — main: liest data, validiert via Asserts, schreibt HTML
- Create: `docs/comparison.html` — generierter Output, Teil des Repos
- Test: `tests/unit/test_comparison_build.py` — pytest, prüft die Asserts auf den realen Daten

**Approach:**
- `Library = NamedTuple("Library", name, github_url, stars, first_release_year, crawl_date, …)`
- `Cell = NamedTuple("Cell", symbol, short_text, caveat | None)` — `symbol ∈ {"✅", "🟡", "📅", "❌"}`
- `Axis = NamedTuple("Axis", id, title, columns, rows)` — `rows: dict[Library, list[Cell]]`
- 8 Axis-Konstanten, jede mit allen 7 Library-Spalten gefüllt
- `build_comparison.main()`:
  - Validierungs-Asserts:
    - `📅` Symbol darf nur in der SwarmQ-Spalte vorkommen (R4)
    - SwarmQ-Spalte über alle 8 Achsen aggregiert: ≥ 5 Cells nicht `✅` (R5)
    - Mindestens eine Achse: SwarmQ-Cell ist `❌` oder `📅` UND mindestens 3 andere Libraries haben dort `✅` (R6)
    - Mindestens eine Achse: SwarmQ-Cell ist `✅` UND mindestens 3 andere haben dort `❌`/`🟡` (R7) — empirisch via Test-Coverage und Lua-Atomarität
  - Wenn ein Assert fehlschlägt: `SystemExit(1)` mit klarer Message (welcher Honesty-Check verletzt ist)
  - HTML wird mit `string.Template` oder f-strings gebaut — keine externen Template-Engines
  - Inline-CSS im `<style>`-Block, ~30-50 Zeilen, monospace für Zahlen-Spalten, zebra-stripes, sticky pro-Tabelle-Header
  - Footer mit Quellen-Block: pro Library ein `<details>` mit URLs + Crawl-Datum
- `tests/unit/test_comparison_build.py`:
  - Importiert `tools.comparison_data` und `tools.build_comparison`
  - Ruft die einzelnen Validation-Asserts direkt auf
  - Verifiziert dass `build_comparison.main()` ohne Exception durchläuft
  - Verifiziert dass `docs/comparison.html` nach dem Build existiert und die Symbol-Legende enthält

**Patterns to follow:**
- Generator-Script-Stil wie `bench.py` (Modul-Level-Funktionen, klare main, keine Klassen-Hierarchie)
- Pytest-Tests mit Klassen + sprechenden Test-Namen (siehe `tests/unit/test_benchmark_aggregate.py`)
- Inline-Dokumentation der Honesty-Asserts in build_comparison.py — die Asserts sind das Vertrauens-Fundament des ganzen Files

**Test scenarios:**
- Happy path: alle 8 Achsen mit echten Daten gefüllt, Build läuft durch, HTML enthält 8 `<table>`s
- Honesty assert R4: testbar mit synthetischer Daten-Variante (Cell mit `📅` in nicht-SwarmQ-Spalte) → `SystemExit`
- Honesty assert R5: testbar mit Variante wo SwarmQ alles `✅` hat → `SystemExit`
- Honesty assert R6: testbar mit Variante wo SwarmQ überall stärker ist als Vergleich → `SystemExit`
- Honesty assert R7: testbar mit Variante wo SwarmQ keine einzige Stärke hat → `SystemExit`
- Output-Smoke: HTML-File existiert, ist `> 5 KB`, enthält "✅" "🟡" "📅" "❌" "SwarmQ" als Substrings
- Determinism: zweimal `build_comparison.main()` aufrufen → byte-identische Datei (leicht via `hashlib`)

**Verification:**
- `python tools/build_comparison.py` läuft ohne Fehler durch
- `docs/comparison.html` existiert, lokal mit `firefox docs/comparison.html` öffenbar
- `uv run pytest tests/unit/test_comparison_build.py` grün
- Manuelle Sichtprüfung: SwarmQ-Spalte enthält mindestens 5 nicht-`✅`-Cells, ist nicht das beste Tool in jeder Zeile

## System-Wide Impact

- **Interaction graph:** Neuer Top-Level-Ordner `tools/` (für Build-Scripts die nicht zur Library gehören). Keine Änderung an `src/swarmq/`.
- **Error propagation:** Build-Script crashed bei Honesty-Verletzung — bewusst, das ist der Test.
- **State lifecycle risks:** keine — pure Daten-Transformation, keine externen Abhängigkeiten zur Build-Zeit.
- **API surface parity:** keine — kein User-facing API verändert.
- **Unchanged invariants:** swarmq-Library, Tests, Build, Plans bleiben unberührt.

## Risks & Dependencies

| Risk | Mitigation |
|---|---|
| RQ/Huey-Recherche zu oberflächlich → falsche Aussagen in der Tabelle | Pro Library mindestens 3 Quellen, jede Cell mit konkretem Beleg (URL oder Doc-Verweis) |
| Performance-Achse vergleicht inkomparable Zahlen (eigene Bench vs. fremde Marketing-Werte) | Zwei Sub-Spalten, explizit getrennt, Quelle pro Zelle |
| Honesty-Asserts zu schwach → Tabelle wird doch zu rosig | Asserts werden auf den realen Daten getestet (in Unit 3 Tests). Wenn Acceptance verletzt wird, schlägt Build fehl |
| Honesty-Asserts zu strikt → kein Build möglich obwohl Daten ehrlich sind | Asserts haben klare Fehlermeldungen die zeigen welche Cell die Regel verletzt; iterativ adjustierbar |
| Star-Counts veralten | Crawl-Datum im Footer und in der Maturity-Tabellen-Legende. Spätere Re-Builds sind manuell |

## Documentation / Operational Notes

- README-Integration explizit deferred (siehe Brainstorm Out-of-Scope).
- Wenn die Tabelle später in einer extern-präsentierten Form gebraucht wird, ist `tools/comparison_data.py` der Einstieg — nicht das HTML editieren.

## Sources & References

- Origin document: [docs/brainstorms/2026-04-16-feature-comparison-table-requirements.md](../brainstorms/2026-04-16-feature-comparison-table-requirements.md)
- Existing eval: [docs/evaluation-task-queues.md](../evaluation-task-queues.md)
- Existing per-library research: `docs/recherche-issues-{celery,taskiq,dramatiq,arq}.md`
- Performance reference: `benchmarks/bench.py`, `benchmarks/comparison/results/`
- SwarmQ feature surface: `src/swarmq/`, `docs/plans/2026-04-16-005-feat-phase3b-rate-limiting-plan.md`
- Library repos to crawl in Unit 1: `https://github.com/rq/rq`, `https://github.com/coleifer/huey`
