---
title: "User Documentation — Scaffold & Content Requirements"
type: feature-brief
status: ready-for-planning
date: 2026-04-16
audience: future content-writing agent (or human)
roadmap_refs: [R5.4, R5.5]
---

# User Documentation — Scaffold & Content Requirements

## Problem Frame

SwarmQ nähert sich Feature-Complete (Phase 3, 476 Tests grün). Ohne eine
öffentliche Dokumentation für End-Nutzer — in diesem Fall Python-Entwickler,
die die Library integrieren wollen — ist das Projekt nicht adoptierbar.
`README.md` reicht als Keimzelle, aber nicht als dauerhafte Doku-Quelle:
Navigation fehlt, Suchbarkeit fehlt, Versioning fehlt, und die
Anforderungen aus `aufgabe.md §17.1/17.2` (Entwickler-HTML-Docs +
KI-Agenten-Doku) werden nicht erfüllt.

Parallel dazu wird ab v0.4 Versioning wichtig (API-Oberfläche wächst,
Deprecations kommen). Eine Doc-Site mit `/latest/`, `/stable/`, `/v0.3/`
parallel ist nur mit einer geeigneten Infrastruktur machbar.

## Goal

Ein produktionsreifes Dokumentations-Grundgerüst für SwarmQ, in dem ein
späterer Agent (oder Mensch) die eigentlichen Inhalte schreiben kann,
ohne noch einmal über Technologie-, Struktur- oder Design-Entscheidungen
nachdenken zu müssen.

Nicht Ziel dieser Phase: die tatsächlichen Inhalte. Dieses Dokument und
der Folgeplan beschreiben den Ausgangszustand und die Content-Aufgabe,
nicht die Content-Produktion selbst.

## Scope

### In-Scope (bereits umgesetzt)

- **Technologie-Stack:** MkDocs Material 9.7 mit Custom-Theme-Override,
  `mkdocstrings[python]` für spätere API-Autogen, `mkdocs-llmstxt` für
  AI-Agent-Support.
- **Verzeichnisstruktur:** `website/` als MkDocs-`docs_dir`, `overrides/`
  für Material-Template-Overrides, `mkdocs.yml` im Repo-Root,
  `.readthedocs.yaml` im Repo-Root.
- **Diátaxis-Navigation:** Overview, Getting Started, Tutorials, How-to
  guides, Reference, Explanation — konfiguriert in `mkdocs.yml`.
- **23 Stub-Seiten** mit Scaffold-Admonition und Themenausblick:
  - `index.md`, `getting-started.md`
  - `tutorials/index.md`, `tutorials/first-email-worker.md`
  - `guides/index.md` + 8 Guide-Stubs (defining-tasks, scheduling-and-retries,
    middleware, signals, rate-limiting, locking-and-dedup, dead-letter-queues,
    testing-tasks) — `middleware.md` ist als komplett ausgearbeitetes Muster
    vorangestellt
  - `reference/index.md`, `reference/api.md`, `reference/cli.md`,
    `reference/configuration.md`, `reference/errors.md`,
    `reference/changelog.md`
  - `explanation/index.md`, `explanation/architecture.md`,
    `explanation/delivery-semantics.md`, `explanation/design-decisions.md`
- **Design-Sprache:** Dark-default, Amber-Akzent (`#F59E0B` / `#D97706`),
  Inter + JetBrains Mono, Stripe/Linear-minimalistischer Stil, WCAG-AA-
  konforme Kontraste. Implementiert in `website/stylesheets/swarmq.css`.
- **AI-Agent-Doku Phase 1:** `/llms.txt` und `/llms-full.txt` werden
  automatisch aus allen Markdown-Seiten generiert, Sektionen nach
  Diátaxis-Gruppen strukturiert.
- **Hosting-Plan:** Read the Docs (`.readthedocs.yaml` liegt bereit,
  Build-OS Ubuntu 24.04, Python 3.14).
- **Dependency-Pflege:** Docs-Deps in `pyproject.toml` unter
  `[project.optional-dependencies].docs`. Install via
  `uv sync --extra docs`.
- **Design-Prototyp erhalten:** `docs/design-prototype/` bleibt als
  Design-Referenz und Vorlage stehen (HTML-Version der fünf Kernseiten).
- **Grundlagen-Recherche dokumentiert:** `docs/recherche-documentation-
  best-practices.md` (8.100 Wörter) deckt Schreibstil, Tonalität,
  visuelle Präsentation, Benchmark-Sites, Checklisten je Seitentyp ab.

### In-Scope (Aufgabe für den späteren Agenten)

- **Content-Produktion** für alle 23 Stub-Seiten.
- **Eine dedizierte AI-Agent-Reference** (`reference/agent/`-Bereich) für
  die in §17.2 geforderten maschinenlesbaren Bestandteile: Error-Katalog,
  Entscheidungsbäume, kanonische Message-Formate. Diese Seiten sind
  Markdown und werden von `llms.txt` mitindiziert.
- **Anschluss an Read the Docs** (Repo → RTD-Account verknüpfen,
  `swarmq.readthedocs.io` aktivieren, Custom Domain später optional).
- **mkdocstrings-Verdrahtung** gegen die echten Docstrings von
  `src/swarmq/`, sobald die Library-API stabil genug ist.

### Out-of-Scope (diese Phase und Folgeplan)

- Lokalisierung / i18n — SwarmQ ist pre-v1, Englisch reicht.
- Videos oder animierte Demos.
- Analytics-Einbindung (Plausible, GA).
- Screenreader-/Accessibility-Testing-Workflow (separate Aufgabe).
- Doc-Versioning-Flow mit `mike` — kommt ab v0.4.
- Migration bestehender `docs/recherche-*.md`-Inhalte in die User-Doku
  (das sind interne Entwickler-Notizen, bleiben getrennt).

## Closed Decisions

Diese Entscheidungen sind aus der Konversation gefallen und gelten als
fixiert, solange nicht im Plan explizit neu eröffnet:

1. **MkDocs Material**, nicht Sphinx/Furo, nicht Docusaurus, nicht Custom
   HTML. Grund: R5.4-Entscheidung aus `docs/brainstorms/implementation-
   order-requirements.md` bestätigt, Python-Ökosystem-passend,
   Template-Overrides decken den Design-Prototyp ab.
2. **Custom-Theme-Override statt Vanilla-Material-Theme**. Grund: Design-
   Prototyp unter `docs/design-prototype/` wurde visuell bestätigt und
   als Zielbild akzeptiert. CSS-Override in
   `website/stylesheets/swarmq.css` bildet Farben, Fonts, Radien,
   Code-Block-Stil und Callout-Farben aus dem Prototyp nach.
3. **Dark als Default**, System-Preference als zweite Option, Light als
   Toggle. Grund: explizit vom Nutzer angefragt, Prototype-Feedback
   bestätigt.
4. **Amber-Akzentfarbe** (`#F59E0B` Light / `#FBBF24` Dark) statt
   Indigo/Blau. Grund: differenziert SwarmQ vom Dev-Tool-Einheitsbrei
   (Celery, Dramatiq, TaskIQ, arq haben kein Branding), semantisch
   passend zur „Swarm"-Assoziation.
5. **Diátaxis-Struktur** (Tutorials / How-To Guides / Reference /
   Explanation) als Navigations-Rückgrat. Grund: industrie-etablierter
   Standard, verhindert das bekannte Anti-Pattern „Alles in einer
   Guide-Liste". Siehe `docs/recherche-documentation-best-practices.md`
   §2.1.
6. **`llms.txt` + `llms-full.txt` Phase 1, dedizierter Agent-Bereich
   Phase 2**. Grund: Markdown als Single Source of Truth, zwei getrennte
   Sites wären doppelte Pflege. §17.2-Anforderung wird durch den
   `reference/agent/`-Unterbereich in Phase 2 erfüllt.
7. **Read the Docs als Hosting**, nicht GitHub Pages, nicht Cloudflare
   Pages. Grund: Python-Ökosystem-Konvention (`swarmq.readthedocs.io` ist
   die erratbare URL), kostenloses Versioning ab v0.4, PR-Preview-Builds,
   kostenlose Suche. Custom Domain kann später ergänzt werden.
8. **Englisch als Dokumentationssprache**. Grund: Zielgruppe ist das
   globale Python-Entwickler-Publikum. Deutsch bleibt für interne
   Brainstorms/Plans/Recherchen (dieses Dokument inklusive).
9. **Line-length Constraint auf ~65 Zeichen im Fließtext** via
   `max-width: 65ch` auf `.md-typeset > p`. Grund: Baymard-Studie,
   optimaler Lesefluss. Code-Blöcke und Tabellen dürfen die Grenze
   überschreiten.
10. **Scaffold-Admonitions bleiben drin**, bis der echte Content
    geschrieben ist. Grund: sichtbarer Hinweis für Leser + Werkzeug-
    Freundlichkeit (Grep nach "Scaffold" listet alle offenen Seiten).

## Non-Goals

- Der Content wird **nicht** in dieser Phase geschrieben. Wer diesen
  Brainstorm liest, soll verstehen, **wo der Schreibauftrag anfängt**,
  nicht, **was geschrieben werden muss** — das steht im Plan.
- Das Theme wird **nicht** auf „Material-Vanilla plus kleine Tweaks"
  reduziert. Der Prototyp ist Design-Zielbild.
- Es werden **keine** weiteren Seitentypen eingeführt, die nicht in der
  Diátaxis-Gliederung vorkommen. Wenn ein Content-Bedürfnis nicht
  reinpasst, wird es in einem der vier Typen untergebracht oder
  aufgeteilt — nicht ein fünfter Typ erfunden.

## Acceptance Criteria

Dieses Dokument ist abgeschlossen, wenn folgende Artefakte existieren und
konsistent aufeinander verweisen:

- [x] `mkdocs.yml` mit Diátaxis-Nav und Custom-Theme-Override
- [x] `website/` mit 23 Stub-Seiten und Scaffold-Admonitions
- [x] `website/stylesheets/swarmq.css` mit Dark-Default und Amber-Akzent
- [x] `.readthedocs.yaml` mit Python 3.14 und Docs-Extra
- [x] `pyproject.toml` hat `docs`-Optional mit Material, mkdocstrings, llmstxt
- [x] `docs/recherche-documentation-best-practices.md` als Referenz
- [x] `docs/design-prototype/` als Design-Zielbild
- [x] `docs/brainstorms/2026-04-16-user-documentation-requirements.md` (dieses Dokument)
- [x] `docs/plans/2026-04-16-010-feat-user-documentation-plan.md` (Folgeartefakt)

## References

- `aufgabe.md` §17.1 — Entwickler-Dokumentation (HTML)
- `aufgabe.md` §17.2 — KI-Agenten-Dokumentation
- `docs/brainstorms/implementation-order-requirements.md` R5.4, R5.5 — Roadmap-Einordnung
- `docs/recherche-documentation-best-practices.md` — Style-, Ton- und UX-Regeln
- `docs/design-prototype/` — Design-Zielbild (5 HTML-Seiten)
- `docs/plans/2026-04-16-010-feat-user-documentation-plan.md` — Content-Schreibplan

## Open Items for the Plan

Diese Punkte sind bewusst nicht im Brainstorm entschieden, weil sie erst
beim Schreiben selbst sinnvoll beantwortbar sind. Der Plan nimmt sie als
offene Entscheidungen in den Arbeitsgang mit:

- **Schreibreihenfolge:** Getting Started zuerst (als Referenz für alle
  anderen) oder Tutorial zuerst (als längster Roundtrip)?
- **Docstring-Autogen-Einstiegszeitpunkt:** direkt bei API-Ref-Arbeit
  oder nachdem die Library auf v0.5 stabilisiert ist?
- **RTD-Anbindung:** vor oder nach dem ersten Content-Pass? Davor hat
  den Vorteil früher Deploy-Preview auf PRs.
