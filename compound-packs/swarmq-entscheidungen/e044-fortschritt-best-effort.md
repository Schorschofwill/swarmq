---
title: Fortschrittsmeldungen sind best effort, auf 500 ms je Task gedrosselt und ohne Reihenfolgezusage
applies_when:
  - Änderungen an Task.update_progress, am Fortschritts-Rückkanal des CPU-Modus oder an watch_progress
  - eine Fehlersuche meldet verlorene, gedrosselte oder umgeordnete Fortschrittsmeldungen
  - ein Plan will Fortschritt zuverlässig oder geordnet zustellen
  - eine Fehlersuche meldet, dass ProgressInfo-Zeitfelder str statt datetime sind
tags: [progress, valkey, cpu-modus, best-effort]
id: E-044
datum: 2026-04-17
status: gilt
quelle:
  - Commit 8d4b593 (R3.7 Unit 3, Task.update_progress)
  - src/swarmq/task.py:272 (_PROGRESS_MIN_INTERVAL_S = 0.5), :363-370 (Docstring „Progress is best-effort“), :392-397 (HSET und Publish außerhalb der Sperre)
  - docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:314 („progress (best effort) → Valkey“), :521 (gedrosselt wie heute)
  - docs/plans/2026-04-17-006-feat-r3.7-progress-tracking-plan.md:75, :222 (Feldtypen str), :186-190 (Startzeit per time.time())
tests:
  - tests/unit/test_task.py::TestUpdateProgress::test_throttles_second_call_within_window
  - tests/unit/test_task.py::TestUpdateProgress::test_final_update_always_flushes
  - tests/unit/test_task.py::TestUpdateProgress::test_backend_hset_failure_swallowed
  - tests/unit/test_task.py::TestUpdateProgress::test_publish_failure_swallowed
---

**Entscheidung.** `update_progress()` ist best effort. Je Task-Instanz geht höchstens eine Meldung pro 500 ms an Valkey; die Endmeldung (`current == total`) geht immer, genau einmal. Valkey-Fehler beim Schreiben oder Veröffentlichen werden geloggt und geschluckt, damit der Task-Körper nicht abstürzt. HSET und Publish laufen außerhalb der Sperre. Im CPU-Modus drosselt das Kind gleich und schickt die Meldungen über den Rückkanal an den Loop-Prozess.

**Begründung.** Fortschritt dient der Anzeige. Er darf weder den Task gefährden noch Valkey mit Meldungsstürmen belasten oder nebenläufige Aufrufe an der Valkey-Latenz serialisieren.

**Bewusste Folgen.** Fehlende Zwischenstände, eine gedrosselte Meldung und eine Umordnung (eine langsame Zwischenmeldung landet nach der Endmeldung, etwa bei einem Valkey-Stall) sind keine Fehler. Das Ergebnis des Tasks ist davon nicht betroffen.

**Nachtrag 2026-10-15 (Fehlersuche Runde 3, F3-15): Zeitfelder.** `ProgressInfo` führt `updated_at` und `started_at` als `str` und `eta_seconds` als `int | None`; die Startzeit stammt aus der Wanduhr (`time.time()`). Für `elapsed`/`eta_seconds` darf daneben die monotone Uhr laufen. Dass `started_at` heute aus `time.monotonic()` stammt und als Datum von 1970 erscheint, ist der Befund F3-15 (Fix in 0.12.3, IO und CPU-Kind); `api.md:756-757` nennt fälschlich `datetime`.

**Überdenken nur mit neuem Argument.** FS-25 (Fehlersuche Runde 2): Reihenfolge als LÜCKE eingestuft, „unbegrenzt viele Tasks“ als nicht haltbar. Eine Reihenfolgezusage gab es nie; sie wäre eine neue Anforderung.
