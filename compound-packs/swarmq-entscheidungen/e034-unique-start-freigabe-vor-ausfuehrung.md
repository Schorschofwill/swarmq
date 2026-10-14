---
title: unique_until="start" gibt das Flag erst unmittelbar vor der Ausführung frei, nach Rate-Limit und Lock
applies_when:
  - Änderungen an der Reihenfolge der Gates in der Worker-Pipeline oder an der Unique-Freigabe im start-Modus
  - eine Fehlersuche meldet, dass ein requeueter oder rate-limitierter Task im start-Modus das Flag noch hält
  - eine Fehlersuche meldet fehlende Dedup während des Retry-Backoffs im start-Modus
tags: [unique, dedup, worker, pipeline, rate-limit, lock]
id: E-034
datum: 2026-04-17
status: gilt
quelle:
  - Commit 7ac9fd6 (R3.5 ce-review P1 #4 / ADV-002, „start-mode release AFTER rate-limit + lock checks“)
  - docs/brainstorms/2026-04-17-phase3-5-unique-tasks-requirements.md:286-294 (Retry-Semantik nach Mode, Einschränkung start)
  - docs/plans/2026-04-17-004-feat-phase3-5-unique-tasks-plan.md (Unit 10)
  - src/swarmq/worker/pipeline.py:355-362 (Docstring)
  - website/guides/locking-and-dedup.md:212-217
tests:
  - tests/unit/worker/test_worker.py::TestWorkerUniqueRelease::test_start_mode_releases_at_pickup
  - tests/unit/worker/test_worker.py::TestWorkerUniqueRelease::test_start_mode_rate_limited_keeps_flag_for_redelivery
---

**Entscheidung.** Im Modus `unique_until="start"` gibt der Worker das Unique-Flag genau einmal frei, und zwar erst, wenn der Task tatsächlich gleich läuft: nach Rate-Limit- und Lock-Prüfung. Wird die Nachricht vorher zurückgestellt oder requeuet, bleibt das Flag gesetzt. Nach dem Start gibt es keine zweite Freigabe.

**Begründung.** Eine Freigabe schon beim Abholen hätte während des Requeue-Fensters ein paralleles `schedule()` an der Dedup vorbeigelassen (ADV-002). Den Modus `start` wählen Nutzer bewusst für kurze TTLs, etwa gegen doppelte Formular-Absendungen.

**Bewusste Folgen.** Während des Retry-Backoffs eines `start`-Tasks gibt es keinen Dedup-Schutz (dokumentierte Einschränkung). Ein rate-limitiertes oder auf einen Lock wartendes Duplikat bleibt abgewiesen, solange die Nachricht nicht läuft.

**Überdenken nur mit neuem Argument.** Offen: FS-13. Endet eine Nachricht terminal **vor** der Freigabe (Expiry, Pre-Start-Cancel, Ende der Lock-Wartezeit, Delivery-Limit, unbekannter Task), bleibt das Flag bis `unique_for` stehen, weil die terminalen Pfade im start-Modus annehmen, die Freigabe sei schon beim Abholen geschehen. Das ist ein Fehler, keine Folge dieser Entscheidung. Ein Fix muss die Einmal-Freigabe dieses Tests einhalten.
