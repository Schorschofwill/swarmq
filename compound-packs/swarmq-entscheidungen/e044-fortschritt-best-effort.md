---
title: Fortschrittsmeldungen sind best effort, auf 500 ms je Task gedrosselt und ohne Reihenfolgezusage
applies_when:
  - Änderungen an Task.update_progress, am Fortschritts-Rückkanal des CPU-Modus oder an watch_progress
  - eine Fehlersuche meldet verlorene, gedrosselte oder umgeordnete Fortschrittsmeldungen
  - ein Plan will Fortschritt zuverlässig oder geordnet zustellen
tags: [progress, valkey, cpu-modus, best-effort]
id: E-044
datum: 2026-04-17
status: gilt
quelle:
  - Commit 8d4b593 (R3.7 Unit 3, Task.update_progress)
  - src/swarmq/task.py:272 (_PROGRESS_MIN_INTERVAL_S = 0.5), :363-370 (Docstring „Progress is best-effort“), :392-397 (HSET und Publish außerhalb der Sperre)
  - docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:314 („progress (best effort) → Valkey“), :521 (gedrosselt wie heute)
tests:
  - tests/unit/test_task.py::TestUpdateProgress::test_throttles_second_call_within_window
  - tests/unit/test_task.py::TestUpdateProgress::test_final_update_always_flushes
  - tests/unit/test_task.py::TestUpdateProgress::test_backend_hset_failure_swallowed
  - tests/unit/test_task.py::TestUpdateProgress::test_publish_failure_swallowed
---

**Entscheidung.** `update_progress()` ist best effort. Je Task-Instanz geht höchstens eine Meldung pro 500 ms an Valkey; die Endmeldung (`current == total`) geht immer, genau einmal. Valkey-Fehler beim Schreiben oder Veröffentlichen werden geloggt und geschluckt, damit der Task-Körper nicht abstürzt. HSET und Publish laufen außerhalb der Sperre. Im CPU-Modus drosselt das Kind gleich und schickt die Meldungen über den Rückkanal an den Loop-Prozess.

**Begründung.** Fortschritt dient der Anzeige. Er darf weder den Task gefährden noch Valkey mit Meldungsstürmen belasten oder nebenläufige Aufrufe an der Valkey-Latenz serialisieren.

**Bewusste Folgen.** Fehlende Zwischenstände, eine gedrosselte Meldung und eine Umordnung (eine langsame Zwischenmeldung landet nach der Endmeldung, etwa bei einem Valkey-Stall) sind keine Fehler. Das Ergebnis des Tasks ist davon nicht betroffen.

**Überdenken nur mit neuem Argument.** FS-25 (Fehlersuche Runde 2): Reihenfolge als LÜCKE eingestuft, „unbegrenzt viele Tasks“ als nicht haltbar. Eine Reihenfolgezusage gab es nie; sie wäre eine neue Anforderung.
