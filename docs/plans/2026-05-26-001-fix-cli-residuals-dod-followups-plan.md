---
title: "Fix cli-residuals DoD followups — Test-Bug, Changelog-Tracking, Sunset-Reminder, Plan-Markup"
status: ready
execution_mode: incremental
created: 2026-05-26
depends_on:
  - 2026-04-23-001-fix-cli-refactor-residuals-plan.md — Eltern-Plan, dessen DoD diese Arbeit schließt
parent_findings:
  - DoD-Audit 2026-05-26 (siehe Conversation-Trace)
---

## Overview

Die DoD-Verifikation des Residuals-Plans `2026-04-23-001` hat vier
offene Punkte ergeben, die separat geschlossen werden müssen, bevor
`refactor/cli-split` mergebar ist:

1. **Latenter Test-Bug** in `tests/unit/test_cli_residuals.py:436` —
   `RuntimeWarning: coroutine 'AsyncMockMixin._execute_mock_call' was
   never awaited`. Test passt grün, prüft die R-4-Invariante aber
   nicht durchgängig.
2. **Changelog-Eintrag CLI-04 nicht versioniert.** `website/` ist
   `docs_dir` in `mkdocs.yml:8` (also die mkdocs-Quelle, nicht der
   Build-Output), steht aber in `.gitignore:43`. Der CLI-04-Behaviour-
   Change-Eintrag ist lokal vorhanden, aber für alle anderen Devs
   und CI unsichtbar. Systemisch: 556 KB Doku-Source sind nicht
   getrackt.
3. **KP-2-Sunset-Reminder (2026-12-31) fehlt** in `docs/solutions/`.
   Der Plan dokumentiert das Datum, aber außerhalb des Plans gibt
   es keinen Trigger, der uns am Stichtag erinnert.
4. **Plan-DoD-Checkboxen unmarkiert** trotz Erfüllung; **Won't-Fix-
   Register nicht im HEAD-Commit-Body verlinkt**.

Ziel: Alle vier in **vier unabhängigen Units** schließen, jede mit
eigenem Commit. Reihenfolge: Test-Bug zuerst (echter Defekt), dann
Reminder + Plan-Markup (mechanisch), Changelog/`.gitignore` als
letzte Unit (braucht Entscheidung).

**Nicht-Ziele:**
- Vollständiger Audit aller anderen ce-review-Findings (separate Runs).
- Generelle Doku-Pipeline-Diskussion über `.gitignore`-Hygiene jenseits
  der einen Zeile.
- Neue Tests jenseits der einen Test-Reparatur.

## Requirements Trace

| ID  | Anforderung | Quelle | Satisfied by |
|-----|-------------|--------|--------------|
| F-1 | `test_run_schedule_status_invokes_public_api` läuft ohne `RuntimeWarning`; das Pytest-Filter `error::RuntimeWarning` (eng begrenzt auf diese Test-Datei) lässt den Test fallen, wenn die Warnung wiederkehrt. Die R-4-Invariante (`get_scheduler_leader` aufgerufen, `_client.get` nicht) bleibt das Gate. | DoD-Audit Punkt 5 | Unit 1 |
| F-2 | Der CLI-04-Behaviour-Change-Eintrag liegt an einem **getrackten** Pfad, der vom mkdocs-Build erfasst wird, und ist im Repo per `git ls-files` auffindbar. | DoD-Audit Punkt 1+2 (R-5 Changelog-Eintrag) | Unit 4 |
| F-3 | `docs/solutions/patterns/` (oder vergleichbar) enthält einen kurzen Eintrag mit Trigger-Datum `2026-12-31`, der auf KP-2 + Whitelist-Trim verweist. Der Eintrag erscheint im `ce-learnings-researcher`-Index per Frontmatter. | DoD-Audit Punkt 3 (Sunset-Reminder) | Unit 2 |
| F-4 | DoD-Checkboxen in `2026-04-23-001` sind aktualisiert: erfüllte Items als `[x]`, mit Status-Vermerk wo "teilweise" geschlossen via dieses Plans. Won't-Fix-Register ist in einem Commit-Body dieses Plans verlinkt (kanonischer Audit-Pointer). | DoD-Audit Punkt 4 | Unit 3 |
| F-5 | `pytest tests/unit` bleibt grün (1249 + ggf. ±1 für Unit 1). Kein neuer ruff/mypy-Fehler. | Regression | Alle Units |

## Scope Boundaries

**In Scope:**
- `tests/unit/test_cli_residuals.py` — Test-Reparatur (Unit 1).
- `docs/solutions/patterns/` (neuer Eintrag) — Sunset-Reminder (Unit 2).
- `docs/plans/2026-04-23-001-fix-cli-refactor-residuals-plan.md` —
  DoD-Markup (Unit 3).
- `.gitignore` Zeile 43 **oder** Anlegen eines getrackten CHANGELOG-Pfads
  + Routing des CLI-04-Eintrags dorthin (Unit 4 — Optionen).
- `website/reference/changelog.md` + `website/reference/changelog.de.md` —
  bewegen in getrackten Pfad **oder** Re-Tracking durch `.gitignore`-Edit
  (Unit 4).

**Out of Scope:**
- Alle übrigen 555 KB unter `website/` — wenn `website/` re-getrackt wird,
  ist das ein "git add -A website/"-Followup, aber außerhalb der
  Behaviour-Change-Sichtbarkeit irrelevant für die DoD dieses Plans.
- mkdocs-i18n-Reorganisation (separates Working-Tree-WIP in `mkdocs.yml`).
- ce-review-Re-Run (DoD-Punkt war "optional", nicht Gate).

## Units

> Reihenfolge: 1 → 2 → 3 → 4. Unit 4 zuletzt, weil sie eine
> Entscheidung verlangt und potenziell den größten Diff erzeugt.

---

### Unit 1 — F-1: Test-Reparatur `test_run_schedule_status_invokes_public_api`

**Problem:** `AsyncMock()` als `fake_backend` liefert für alle ungesetzten
Attribute automatisch coroutine-returning Methoden. `_run_schedule_status`
ruft vermutlich auf `fake_backend` mehr Methoden auf als nur
`get_scheduler_leader`/`close` (z. B. `get_progress`, `get_result`, oder
`ScheduleStore`-interne Aufrufe, die wiederum aufs Backend gehen). Eine
dieser auto-AsyncMock-Coroutines wird ohne `await` weggeworfen → die
RuntimeWarning.

**Root-Cause-Hypothesen (in Reihenfolge der Wahrscheinlichkeit):**
1. `ScheduleStore` wird mit `fake_backend` instantiiert und ruft im
   Konstruktor oder in `list_schedules` synchron eine Methode auf
   (`backend.something()` ohne `await`), die jetzt eine ungenutzte
   Coroutine ist.
2. `_run_schedule_status` ruft `fake_backend.something_else()` direkt
   in einem nicht-async-Kontext (z. B. einem ConfigBuilder).
3. `monkeypatch.setattr(ScheduleStore, "poll_due", _empty_poll)` ist
   die falsche Stelle — die echte Codepath ruft `list_schedules` oder
   `get_schedules`, nicht `poll_due`.

**Untersuchungsschritte:**
1. `grep -n "fake_backend\\|backend\\." src/swarmq/cli/schedule_cmd.py`
   im `_run_schedule_status`-Body lesen.
2. `pytest tests/unit/test_cli_residuals.py::TestScheduleStatusUsesPublicAPI::test_run_schedule_status_invokes_public_api -W error::RuntimeWarning -x`
   ausführen — die Coroutine-Warnung wird zum Fehler, der Stack zeigt
   die genaue Aufrufstelle.
3. Den AsyncMock entsprechend präzisieren: betroffene Methode explizit
   als `AsyncMock(return_value=...)` setzen mit dem Rückgabewert, der
   den Codepath am realistischsten simuliert.

**RED-Fix-Sequenz:**
- Pytest-Filter eng auf diesen einen Test einschränken (per Decorator
  `@pytest.mark.filterwarnings("error::RuntimeWarning")`) — vermeidet
  Kollateralschäden gegen pre-existing Warnings anderswo. Damit wird
  der aktuelle Test RED.
- Mock präzisieren bis grün — der Stacktrace führt zur fehlenden
  Methode.
- Optional: Wenn der Test sich als brüchig erweist (zu viele Mock-Setups),
  durch einen Fake-Class-Stub ersetzen (`class _FakeBackend: async def
  get_scheduler_leader(self): return "leader-xyz"` etc.) — robuster als
  `AsyncMock()` mit selektivem Setup.

**Gates:**
- `pytest tests/unit/test_cli_residuals.py -W error::RuntimeWarning -x` grün.
- Volle Suite weiter grün (1249 passed / 2 skipped, ggf. ±1).
- Die Original-Assertions (`get_scheduler_leader.assert_awaited_once()`
  + `_client.get.assert_not_called()`) bleiben semantisch erhalten.

**Risiko:** Gering. Lokale Test-Reparatur, keine Produktionscode-
Änderung. Wenn die Hypothese falsch ist, zeigt der `-W error`-Run die
korrekte Stelle innerhalb von Sekunden.

**Commit:** `fix(cli-residuals-dod): F-1 — repair AsyncMock setup in schedule_status test`

---

### Unit 2 — F-3: KP-2-Sunset-Reminder in `docs/solutions/patterns/`

**Problem:** Der DoD-Punkt verlangte einen Reminder außerhalb des
Plans, der am `2026-12-31` triggern kann (Re-Evaluation der KP-2-
Rename- und Whitelist-Trim-Frage). Pläne werden i. d. R. nicht
proaktiv durchsucht, `docs/solutions/` aber wohl (`ce-learnings-
researcher`).

**Aktion:**
- Neue Datei: `docs/solutions/patterns/2026-12-31-sunset-kp2-cli-surface-whitelist.md`
  (Datum-im-Dateinamen = Trigger; `ce-compound-refresh`/`ls`-Sort macht
  Stichtag sichtbar).
- Frontmatter:
  ```yaml
  ---
  type: workflow-learning
  topic: cli-surface-whitelist
  trigger_date: 2026-12-31
  parent_plan: docs/plans/2026-04-23-001-fix-cli-refactor-residuals-plan.md
  finding_refs: [KP-2, M-02, M-03]
  status: pending-evaluation
  ---
  ```
- Body (kurz, ≤ 30 Zeilen): Wer/was/wann zu prüfen ist
  - Konsumentenanalyse: GitHub Issues + Docs nach `_DLQNotFound`,
    `ValkeyBackend`, `RabbitMQBroker` als Surface-Patch-Targets durchsuchen.
  - Wenn keine externen Konsumenten: separaten Plan für Rename
    `_DLQNotFound → _DLQNotFoundError` + Whitelist-Trim öffnen.
  - Wenn ja: neuen Sunset-Termin setzen (z. B. +12 Monate).
- Link in `docs/solutions/MEMORY.md` (falls existent) oder Eintrag im
  Index.

**Gates:**
- Datei existiert und ist im Repo getrackt.
- Frontmatter parseable (`python -c "import yaml; yaml.safe_load(open('…').read().split('---')[1])"` oder einfacher: in passender Reviewer-Sicht funktional).

**Risiko:** Trivial — reine Doku.

**Commit:** `docs(cli-residuals-dod): F-3 — sunset-reminder for KP-2 + whitelist trim (2026-12-31)`

---

### Unit 3 — F-4: Plan-DoD-Markup + Won't-Fix-Register-Verlinkung

**Problem:** Die DoD-Liste im Eltern-Plan zeigt nur einen Haken
(Pre-Impl-Audit). Alle anderen Items sind durch die Arbeit der sieben
Units faktisch erfüllt, aber unmarkiert. Der letzte Commit (`f385ab0`)
verweist nicht auf das Won't-Fix-Register.

**Aktion:**
- In `docs/plans/2026-04-23-001-fix-cli-refactor-residuals-plan.md`
  alle DoD-Checkboxen aktualisieren:
  - `[x]` für: 7 Units gelandet, pytest grün +45, beide grep-Gates
    leer, Follow-up-Stub angelegt, Sunset-Datum dokumentiert.
  - `[x] (via 2026-05-26-001)` für: Changelog-Eintrag, env-var-Doku,
    Won't-Fix-Verlinkung — mit Datei-Link zu diesem Plan als Spur.
  - `[ ]` lassen nur, was bewusst offen bleibt (optionaler ce-review).
- Won't-Fix-Verlinkung: Im Commit-Body dieses Markup-Commits
  (Unit 3) das Won't-Fix-Register als kanonische Antwort referenzieren
  (sektion-Link + Sunset-Datum-Verweis). Das ist die DoD-konforme
  Alternative zum Rewrite von `f385ab0`, da Amend dort destruktiv wäre.

**Gates:**
- Plan-Diff zeigt nur Checkbox-Toggles + Status-Vermerke, keine
  inhaltlichen Plan-Änderungen.
- Commit-Body enthält Verweis auf
  `docs/plans/2026-04-23-001-fix-cli-refactor-residuals-plan.md#wont-fix-register-mit-begründung`
  (oder semantisches Äquivalent).

**Risiko:** Trivial.

**Commit:** `docs(cli-residuals-dod): F-4 — mark DoD checkboxes + link won't-fix register`

---

### Unit 4 — F-2: Changelog-Eintrag CLI-04 versioniert ablegen

**Problem:** `website/` ist gitignored, aber gleichzeitig
mkdocs-`docs_dir`. Das ist ein systemischer Bug. Für unseren Scope
zählt nur, dass der CLI-04-Behaviour-Change-Eintrag versioniert ist.
Die größere Doku-Frage (alle 556 KB re-tracken) ist Out-of-Scope.

**Optionen (Entscheidung Teil dieses Units):**

**Option A — `.gitignore`-Zeile entfernen + `website/` re-tracken.**
- Pro: Richtigt das systemische Problem; mkdocs-Doku ist endlich
  versioniert. CLI-04-Changelog fließt automatisch ein.
- Contra: Großer Commit (~556 KB, viele Dateien). Wer den Original-
  Eintrag in `.gitignore` setzte, hatte vielleicht einen Grund (CI?
  Generator?). Risiko, etwas zu brechen.

**Option B — Eigenes `CHANGELOG.md` im Repo-Root einführen.**
- Pro: Klein, gezielt, getrennt von der mkdocs-Doku. "Keep a
  Changelog"-Konvention. Wird zur kanonischen Quelle, mkdocs kann
  später via Snippet-Include daraus generieren.
- Contra: Duplikation mit `website/reference/changelog.md`; jemand
  muss die beiden konsolidieren.

**Option C — Nur die zwei `changelog.*.md`-Dateien `git add -f`
forcen, Rest von `website/` bleibt ignoriert.**
- Pro: Minimaler Eingriff, gezielt für CLI-04.
- Contra: Heuchlerisch — die ganze Doku-Source bleibt unsichtbar,
  nur eine Datei wird Sonderfall.

**Empfehlung:** **Option A**, weil sie das eigentliche Problem
(unversionierte Doku) löst und damit auch alle zukünftigen
Behaviour-Change-Einträge "einfach funktionieren". Vor Ausführung
**eine Sache prüfen**: `git log --all -- website/` und `git log --all
--diff-filter=D -- website/` — gibt es Spuren, warum `website/`
gitignored wurde (z. B. ein historischer Push, der Build-Artefakte
einchecken sollte)? Wenn ja, im Commit-Body dokumentieren.

**Aktion (Option A — anpassen wenn anders entschieden):**
1. `.gitignore`: Zeile `website/` (Zeile 43) entfernen.
2. `git add website/` — landet 556 KB Doku-Source.
3. Spot-Check: Sind sensitive/private Dateien drin (`.env`, Secrets,
   Keys)? `find website/ -type f \( -name "*.env" -o -name "*secret*"
   -o -name "*.key" \) -print` muss leer sein.
4. `mkdocs build` lokal → kein neuer Fehler vs. vorher.
5. Commit-Body erwähnt explizit: CLI-04-Changelog-Eintrag ist jetzt
   versioniert (DoD-Punkt F-2 erfüllt).

**Gates:**
- `git ls-files website/reference/changelog.md` ist non-empty.
- `git ls-files website/reference/changelog.de.md` ist non-empty.
- `mkdocs build` läuft fehlerfrei (oder bricht aus pre-existing
  Gründen, nicht durch unsere Änderung).
- Keine offensichtlichen Secrets im Diff.

**Risiko:** Mittel. Größter Diff dieses Plans. Mitigation: vor
Commit `git diff --stat --cached | tail` sichten, dass nur Doku-
Files landen.

**Commit:** `docs(cli-residuals-dod): F-2 — re-track website/ docs source (fixes hidden CLI-04 changelog)`

> Falls Option B/C statt A: ein Commit (B: `docs: add root CHANGELOG.md
> with CLI-04 entry`, C: `docs: force-track CLI-04 changelog files`).

---

## Verification Matrix

| Gate | Kommando | Erwartung |
|---|---|---|
| Unit-Suite | `pytest tests/unit -q` | 1249 passed / 2 skipped (±1 für Unit 1) |
| Test-Warning-Free | `pytest tests/unit/test_cli_residuals.py -W error::RuntimeWarning` | Grün |
| Changelog versioniert | `git ls-files website/reference/changelog.md` | Non-empty |
| Sunset-Reminder | `ls docs/solutions/patterns/2026-12-31-sunset-kp2-*.md` | Existiert |
| DoD-Plan-Markup | `grep -c "^- \[x\]" docs/plans/2026-04-23-001-fix-cli-refactor-residuals-plan.md` | ≥ 8 (von 10) |
| Ruff/Mypy | `ruff check src/ tests/ && mypy src/swarmq` | Keine neuen Findings |

## Commit-Plan

```
fix(cli-residuals-dod): F-1 — repair AsyncMock setup in schedule_status test
docs(cli-residuals-dod): F-3 — sunset-reminder for KP-2 + whitelist trim (2026-12-31)
docs(cli-residuals-dod): F-4 — mark DoD checkboxes + link won't-fix register
docs(cli-residuals-dod): F-2 — re-track website/ docs source (fixes hidden CLI-04 changelog)
```

Branch-Strategie: direkt auf `refactor/cli-split` (gleiche Logik wie
beim Eltern-Plan — alles fließt zusammen in den Merge).

## Open Decisions

| Ref | Frage | Entscheidung erforderlich vor |
|---|---|---|
| D-1 | Unit 4: Option A (re-track gesamtes `website/`), B (root `CHANGELOG.md`) oder C (force-track nur die zwei changelog-Dateien)? | Beginn Unit 4 |
| D-2 | Unit 1: Wenn AsyncMock-Setup zu fragil wird — Wechsel zu Fake-Class-Stub ok? | Beginn Unit 1 (kann implementierungs-internal entschieden werden) |

## Definition of Done

- [ ] Unit 1 gelandet: `test_run_schedule_status_invokes_public_api`
      ohne `RuntimeWarning`; Test-spezifisches `error::RuntimeWarning`
      Filter aktiv.
- [ ] Unit 2 gelandet: Sunset-Reminder-Datei in `docs/solutions/`
      angelegt, mit Frontmatter und Trigger-Datum `2026-12-31`.
- [ ] Unit 3 gelandet: DoD-Checkboxen im Eltern-Plan `2026-04-23-001`
      aktualisiert; Won't-Fix-Register im Commit-Body verlinkt.
- [ ] Unit 4 gelandet: CLI-04-Changelog-Eintrag ist `git ls-files`-sichtbar;
      gewählte Option (A/B/C) im Commit-Body begründet.
- [ ] Volle Unit-Suite grün; ruff/mypy ohne neue Findings.
- [ ] Eltern-Plan-DoD damit faktisch geschlossen (bis auf optionalen
      ce-review-Re-Run).
