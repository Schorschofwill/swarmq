---
title: "feat: User Documentation — Content Production"
type: feat
status: implemented
date: 2026-04-16
origin: docs/brainstorms/2026-04-16-user-documentation-requirements.md
---

# feat: User Documentation — Content Production

## Overview

Das Dokumentations-Grundgerüst für SwarmQ steht (MkDocs Material unter
`website/`, Diátaxis-Navigation, Custom-Theme-Override, Scaffold-Seiten,
`.readthedocs.yaml`, automatisches `llms.txt`). Dieser Plan beschreibt,
wie die 23 Scaffold-Seiten in echten Content überführt werden und wie
die Live-Deployment-Kette auf Read the Docs aktiviert wird.

Der Plan ist so geschrieben, dass ein Agent ohne Zugang zur vorherigen
Konversation ihn vollständig abarbeiten kann: alle Entscheidungen sind
im Brainstorm fixiert, alle Style-Regeln stehen im Recherche-Dokument,
alle Seiten-Checklisten hängen unten direkt an.

## Problem Frame

MkDocs Material rendert heute 23 Seiten mit Scaffold-Admonitions und
Platzhalter-Text. Das Design steht, die Struktur steht, die Technologie
steht. Was fehlt, ist das Schreiben der eigentlichen Inhalte — eine
Aufgabe, die nicht einmalig in einer Sitzung erledigt werden kann,
sondern in mehreren Durchgängen Seite für Seite passiert.

(see origin: `docs/brainstorms/2026-04-16-user-documentation-requirements.md`)

## Requirements Trace

- **R5.4** (origin: `docs/brainstorms/implementation-order-requirements.md`):
  HTML-Dokumentation für Entwickler mit Getting Started, Core Concepts,
  Task-Definition, Client API, Worker, Middleware, Serialisierung, Error
  Handling, Testing, CLI Reference.
- **R5.5** (origin: derselbe): KI-Agenten-Dokumentation als
  maschinenlesbare API-Spec, Message-Formate, CLI-Spec,
  Entscheidungsbäume, Beispiele, Fehlerkatalog.
- **`aufgabe.md §17.1/§17.2`**: detaillierte Anforderungen, die R5.4/R5.5
  präzisieren.

## Scope Boundaries

### In-Scope

- **Content-Produktion** für die 23 existierenden Scaffold-Seiten unter
  `website/`.
- **Neuer `reference/agent/`-Unterbereich** für den §17.2-Anteil, mit
  mindestens: `reference/agent/index.md` (Einstieg + Regeln),
  `reference/agent/message-formats.md` (kanonische Payload-Schemata),
  `reference/agent/decision-trees.md` (Wann-welcher-Task, Fehler-Pfade),
  `reference/agent/error-catalog-machine.md` (strukturierter Error-
  Katalog). Diese Seiten werden in `mkdocs.yml` unter `Reference →
  Agent` ergänzt und laufen über die `llms.txt`-Indexierung mit.
- **mkdocs.yml-Anpassungen**, soweit für Content-Hinzufügungen nötig
  (neue Einträge in `nav:`).
- **Read-the-Docs-Anbindung:** Repo mit RTD verknüpfen, Build auslösen,
  ggf. Custom-Domain setzen.
- **Sanity-Check-Runs** nach jedem größeren Content-Batch:
  `mkdocs build --strict`, optional ein Linkchecker.
- **Scaffold-Admonition-Entfernung** auf jeder Seite, sobald sie echt
  befüllt ist.

### Out-of-Scope

- `mkdocstrings`-Autogen gegen `src/swarmq/` — wird erst aktiviert,
  sobald die Library-API stabil ist (Phase 5 oder später). Bis dahin
  bleibt `reference/api.md` handgeschriebene Referenz mit dem Muster
  aus dem aktuellen Stub.
- Migrationen der internen `docs/recherche-*.md`-Notizen in die
  User-Doku. Die bleiben als Entwickler-Kontext getrennt.
- Neue Seitentypen außerhalb Diátaxis. Alles Neue wird in einem der
  vier Bereiche untergebracht.
- Design-Änderungen am Theme. Wenn Design-Issues auffallen, sind sie
  in einem separaten Plan zu führen — dieser Plan ändert keine CSS.
- Versioning-Setup via `mike` — kommt separat ab v0.4.
- Lokalisierung, Videos, Analytics.

### Deferred to Separate Tasks

- **Docstring-Autogen für API-Reference:** separates Sub-Ticket, sobald
  Library v0.5+.
- **Tutorial-Reihe über die Startphase hinaus** (nur `first-email-worker`
  ist im Scaffold): weitere Tutorials nach Bedarf in Folge-Plänen.
- **Changelog-Automation** (z. B. git-cliff oder towncrier): separate
  Tooling-Aufgabe, nicht Teil der Content-Phase.

## Environment Setup

Der ausführende Agent braucht nur diese Befehle, um lokal zu arbeiten:

```bash
# Repo-Root: /home/georg/Dokumente/Development/swarmq_v2
uv sync --extra docs           # installiert Material, mkdocstrings, llmstxt
uv run mkdocs serve            # Live-Reload auf http://127.0.0.1:8000
uv run mkdocs build --strict   # Sanity-Check (muss ohne Warnings bauen)
```

Falls `mkdocs` nach einem anderen `uv sync`-Aufruf verschwindet
(beobachtet während der Scaffold-Phase), erneut `uv sync --extra docs`
laufen lassen.

## Pre-Work (muss der Agent zuerst lesen)

Reihenfolge ist nicht zufällig — das spätere Arbeiten hängt an dieser
Reihenfolge:

1. **`docs/brainstorms/2026-04-16-user-documentation-requirements.md`** —
   10 Minuten. Was ist bereits entschieden, was nicht.
2. **`docs/recherche-documentation-best-practices.md`** §1 TL;DR und §5
   Checklisten pro Seitentyp — 15 Minuten. Das sind die operativen
   Schreibregeln. §5 hängt unten für direkten Zugriff an.
3. **`docs/design-prototype/getting-started.html`** in einem Browser
   oder Editor — 5 Minuten. So fühlt sich der Zielzustand an.
4. **`README.md`** — 5 Minuten. Die Quick-Start-Abschnitte sind gute
   Content-Keimzellen für `index.md` und `getting-started.md`.
5. **`aufgabe.md §17`** — 5 Minuten. Lastenheft-Niveau.

## Writing Order

Die empfohlene Reihenfolge ist **nicht alphabetisch und nicht nach
Navigation**, sondern nach Abhängigkeit und Risiko:

### Phase 1 — Mental-Model-Setup (eine Sitzung)

Damit alle anderen Seiten sich darauf beziehen können:

1. `explanation/architecture.md` — *erst klären, wie das System
   funktioniert*, dann Nutzer darauf eingehen lassen. Kurze Prosa mit
   einem SVG/Mermaid-Diagramm der drei Rollen (Client / Broker / Worker).
2. `explanation/delivery-semantics.md` — at-least-once Kontrakt explizit
   machen (Idempotenz-Erwartung). Dieser Text wird später in mindestens
   drei anderen Seiten referenziert.
3. `explanation/design-decisions.md` — kann kurz bleiben, dient als
   Vorwärtsverweisziel aus API- und How-To-Seiten.

### Phase 2 — Einstieg (eine Sitzung)

4. `index.md` — Landing. Hero-Textmessage + Feature-Grid wurde im
   Scaffold so angelegt wie im Prototyp. Hier wird primär Tone of
   Voice getunt (kurze Sätze, konkrete Verben, keine Marketing-
   Superlative). Siehe §5.1 der Recherche.
5. `getting-started.md` — die wichtigste Seite der ganzen Doku. Muss
   ein kompletter, unveränderter Copy-Paste-Runthrough zum laufenden
   Worker in unter 5 Minuten sein. `README.md` ist die Vorlage — dort
   ist die Sequenz bereits validiert. Siehe §5.2 der Recherche.

### Phase 3 — Reference-Kern (zwei Sitzungen)

Reference vor How-To, weil How-Tos auf Reference verweisen:

6. `reference/api.md` — eine Klasse nach der anderen im Muster des
   Stubs (`SwarmQ`, `Task`, `TaskInfo`, `Middleware`, `Client`,
   `TaskHandle`). Pro Methode: Signatur, Parameter-Tabelle, Returns,
   Raises, Example. Siehe §5.5 der Recherche.
7. `reference/cli.md` — alle `swarmq`-Subcommands mit Flag-Tabellen
   und Beispielen. Command-Liste aus `src/swarmq/cli.py` ableiten.
   Siehe §5.6 der Recherche.
8. `reference/configuration.md` — sämtliche `SWARMQ_*`-Env-Vars und
   `TaskInfo`-Felder in Tabellenform. Quelle: `src/swarmq/config.py`
   und `src/swarmq/task.py`.
9. `reference/errors.md` — jede Exception aus `src/swarmq/exceptions.py`
   in eigenem Eintrag: **When raised**, **Cause**, **Recovery**. Muster
   existiert im Stub.

### Phase 4 — How-To Guides (drei bis vier Sitzungen)

10. `guides/defining-tasks.md`
11. `guides/scheduling-and-retries.md`
12. `guides/rate-limiting.md`
13. `guides/signals.md`
14. `guides/locking-and-dedup.md`
15. `guides/dead-letter-queues.md`
16. `guides/testing-tasks.md`
17. `guides/middleware.md` — ist bereits als Komplett-Muster im
    Scaffold. Inhaltlich nur noch verfeinern, Struktur passt schon.

Jedes Guide: genau eine Frage beantworten, mit mindestens einem
lauffähigen Code-Block und Cross-Links zur passenden Reference-Seite.
Siehe §5.4 der Recherche.

### Phase 5 — Tutorial (eine Sitzung, längster Einzeltext)

18. `tutorials/first-email-worker.md` — vollständiger Hands-on-
    Durchgang mit konkretem Outcome („funktionsfähiger Email-Worker").
    Schritt-für-Schritt, jeder Code-Block muss laufen. 45–60 Minuten
    Lese-/Tipp-Zeit. Siehe §5.3 der Recherche.

### Phase 6 — Changelog + Indexseiten (eine Sitzung)

19. `reference/changelog.md` — Keep-a-Changelog-Format, Git-History
    ist die Quelle (`git log --oneline --reverse`). Siehe §5.8.
20. `tutorials/index.md`, `guides/index.md`, `reference/index.md`,
    `explanation/index.md` — kurze Einstiegsseiten, die die
    dahinterstehenden Sektionen vorstellen und Querverweise setzen.

### Phase 7 — Agent-Reference (zwei Sitzungen, neuer Bereich)

21. `reference/agent/index.md` — neue Seite. Erklärt, wie ein
    KI-Agent die Doku nutzt (`/llms.txt` bzw. `/llms-full.txt` Einstieg),
    Regeln für stabile Contracts, Verweis auf die Unterseiten.
22. `reference/agent/message-formats.md` — kanonische Payload-Schemata
    für Task-Dispatch, Result, Cancel-Flag, DLQ-Einträge.
    Maschinenlesbar gehaltener Markdown-Tabellenstil.
23. `reference/agent/decision-trees.md` — strukturierte Flussdiagramme
    in Mermaid für typische Entscheidungen (welche Queue, welcher
    Retry, wann DLQ, wann Rate-Limit).
24. `reference/agent/error-catalog-machine.md` — Spiegelung von
    `reference/errors.md` in einem strikt tabellarischen Format
    (Exception-Klasse, HTTP-ähnlicher Code, strukturierte
    Metadaten-Keys).

`mkdocs.yml` muss nach dieser Phase um diese vier Einträge ergänzt
werden, und `llmstxt.sections` bekommt eine neue Gruppe `Agent
reference`.

## Work Units

Jede Seite ist ihre eigene Work Unit. Pro Work Unit gilt:

**Eingang:**
- Scaffold-Datei mit Scaffold-Admonition und Themenausblick (vorhanden)
- Relevante §5.X-Checkliste aus der Recherche
- Cross-Link-Ziele der Diátaxis-Nachbarseiten

**Arbeitsgang:**
1. Stub lesen; Themenausblick als Agenda nehmen.
2. Entsprechende Checkliste aus `docs/recherche-documentation-best-
   practices.md §5` öffnen.
3. Inhalt schreiben. Jeder Code-Block muss compilieren / lauffähig
   sein (mental oder via `uv run python -c`).
4. Scaffold-Admonition entfernen.
5. `uv run mkdocs build --strict` laufen lassen — bei Warnings fixen,
   nicht ignorieren.
6. Querverweise prüfen: tote Links zu anderen Seiten in `website/` sind
   blockierend, tote Links zu noch ungeschriebenen Seiten werden im
   Arbeitsgang akzeptiert und im nächsten Durchgang geschlossen.

**Ausgang (Definition of Done pro Seite):**
- Scaffold-Admonition entfernt
- Keine `TODO`/`FIXME`-Kommentare mehr drin
- Build läuft `--strict` ohne Warnings
- Mindestens ein lauffähiger Code-Block pro How-To / Tutorial / Getting-
  Started-Seite
- Mindestens eine Cross-Reference (`./other-page.md`) pro How-To
- Zeilenlänge im Fließtext hält `65ch`-Constraint (visuell prüfen)
- Tone-Check: aktive Verben, Imperativ für Anweisungen, „you" statt
  „the user", kein Marketing-Ton. Siehe Recherche §2.2.

## Quality Gates

- **`mkdocs build --strict` muss grün sein** nach jedem Commit. Kein
  Merge mit Warnings.
- **Keine externen Bilder**, die nicht im Repo liegen. Screenshots
  gehen unter `website/assets/screenshots/` mit Dark- und Light-Variante.
- **Keine inline-HTML-Blöcke größer als zehn Zeilen.** Wenn ein Block
  so groß werden würde, ist das ein Signal für eine neue Material-
  Komponente oder eine bessere Strukturierung.
- **`/llms.txt` nach jedem Build prüfen** (`curl -s
  http://127.0.0.1:8000/llms.txt | head`): neue Seiten müssen in der
  richtigen Sektion auftauchen.

## Hosting Activation (separate, kurze Work Unit)

Kann parallel zum Content laufen, empfohlen **vor** Phase 2:

1. Repo auf GitHub pushen (falls noch nicht öffentlich).
2. Auf `readthedocs.org` → Import Project → Repo auswählen.
3. Build sollte sofort grün gehen, da `.readthedocs.yaml` bereits korrekt
   liegt. Falls nicht: Build-Log lesen, Python-Version oder Extras
   anpassen.
4. URL verifizieren: `https://swarmq.readthedocs.io/en/latest/`.
5. Später (nach v0.4): Custom Domain `docs.swarmq.io` einrichten.

## Verification

Nach Abschluss **aller** Work Units muss gelten:

- `uv run mkdocs build --strict` grün.
- Alle 27 Seiten (23 Scaffold + 4 neue Agent-Reference) ohne Scaffold-
  Admonition.
- `grep -r "Scaffold" website/` liefert leeres Ergebnis.
- `curl -s https://swarmq.readthedocs.io/en/latest/llms.txt` gibt die
  vollständige Indexstruktur aus.
- Mindestens drei Cross-Links pro Kapitel-Index-Seite.
- Changelog deckt alle bisher veröffentlichten Versionen ab.
- Landing-Page → Getting Started → First Tutorial: zusammenhängender
  Onboarding-Pfad ohne Brüche.

## Appendix A — Checklisten pro Seitentyp

Die detaillierten Checklisten stehen in
`docs/recherche-documentation-best-practices.md §5`. Als
Schnellzugriff hier die Ankerziele:

| Seitentyp | Recherche-Abschnitt | Hauptprinzip |
|---|---|---|
| Landing / Index | §5.1 | Ein Satz Value Prop + Quick Start Link |
| Getting Started | §5.2 | Copy-paste-runnable in <5 Minuten |
| Tutorial | §5.3 | Konkretes Outcome, Schritt für Schritt |
| How-To Guide | §5.4 | Eine Frage, eine Antwort, verlinkt zur Reference |
| API Reference | §5.5 | Signatur, Params, Returns, Raises, Example |
| CLI Reference | §5.6 | Usage-Block, Flag-Tabelle, Beispiele |
| Troubleshooting | §5.7 | Symptom → Ursache → Fix |
| Changelog | §5.8 | Keep-a-Changelog + SemVer |

## Appendix B — Design-Sprache in Kurzform

Der Agent soll beim Schreiben **nichts** am Design ändern. Zur
Orientierung, welche Komponenten zur Verfügung stehen:

- **Admonitions** (Material-Syntax): `!!! note`, `!!! tip`,
  `!!! warning`, `!!! danger`, `!!! info`, `!!! example`, `!!! abstract`
  („Scaffold" war abstract — wird entfernt).
- **Code-Tabs**: `=== "uv"` / `=== "pip"` Syntax für alternative
  Install-/Run-Varianten.
- **Tabellen**: native Markdown-Tabellen reichen.
- **Codeblock-Titel**: ` ```python title="myapp/tasks.py" `
- **Keyboard-Hinweise**: `++ctrl+c++` rendert als Taste.
- **Feature-Grid** (Landing): `<div class="grid cards" markdown>` + Liste.
- **Mermaid-Diagramme**: ` ```mermaid ` Blöcke, Material rendert sie
  automatisch (in `mkdocs.yml` schon konfiguriert).
- **Farben und Fonts sind fixiert** — kein `<style>` in Markdown-
  Seiten. Wenn eine Komponente fehlt, ins CSS-Override unter
  `website/stylesheets/swarmq.css` ergänzen.

## Appendix C — Deliverables Checklist

| Nummer | Datei | Phase | Status |
|---|---|---|---|
| 01 | `website/explanation/architecture.md` | 1 | scaffold |
| 02 | `website/explanation/delivery-semantics.md` | 1 | scaffold |
| 03 | `website/explanation/design-decisions.md` | 1 | scaffold |
| 04 | `website/index.md` | 2 | scaffold |
| 05 | `website/getting-started.md` | 2 | scaffold |
| 06 | `website/reference/api.md` | 3 | scaffold mit Muster |
| 07 | `website/reference/cli.md` | 3 | scaffold mit Muster |
| 08 | `website/reference/configuration.md` | 3 | scaffold mit Muster |
| 09 | `website/reference/errors.md` | 3 | scaffold mit Muster |
| 10 | `website/guides/defining-tasks.md` | 4 | scaffold |
| 11 | `website/guides/scheduling-and-retries.md` | 4 | scaffold |
| 12 | `website/guides/rate-limiting.md` | 4 | scaffold |
| 13 | `website/guides/signals.md` | 4 | scaffold |
| 14 | `website/guides/locking-and-dedup.md` | 4 | scaffold |
| 15 | `website/guides/dead-letter-queues.md` | 4 | scaffold |
| 16 | `website/guides/testing-tasks.md` | 4 | scaffold |
| 17 | `website/guides/middleware.md` | 4 | scaffold mit Muster |
| 18 | `website/tutorials/first-email-worker.md` | 5 | scaffold |
| 19 | `website/reference/changelog.md` | 6 | scaffold |
| 20 | `website/tutorials/index.md` | 6 | scaffold |
| 21 | `website/guides/index.md` | 6 | scaffold |
| 22 | `website/reference/index.md` | 6 | scaffold |
| 23 | `website/explanation/index.md` | 6 | scaffold |
| 24 | `website/reference/agent/index.md` | 7 | nicht angelegt |
| 25 | `website/reference/agent/message-formats.md` | 7 | nicht angelegt |
| 26 | `website/reference/agent/decision-trees.md` | 7 | nicht angelegt |
| 27 | `website/reference/agent/error-catalog-machine.md` | 7 | nicht angelegt |

## References

- `docs/brainstorms/2026-04-16-user-documentation-requirements.md` — Brainstorm, Entscheidungen
- `docs/recherche-documentation-best-practices.md` — Schreib- und UX-Regeln, Checklisten
- `docs/design-prototype/` — Design-Zielbild (fünf HTML-Seiten)
- `aufgabe.md §17.1 / §17.2` — Lastenheft
- `README.md` — Content-Keimzelle für Landing und Getting Started
- `src/swarmq/cli.py` — Quelle für CLI-Referenz
- `src/swarmq/config.py`, `src/swarmq/task.py` — Quellen für Configuration-Referenz
- `src/swarmq/exceptions.py` — Quelle für Error-Katalog
