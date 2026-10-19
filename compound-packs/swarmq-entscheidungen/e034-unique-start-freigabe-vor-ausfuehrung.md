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
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md (KTD12, Nachtrag FS-13, 2026-10-14)
tests:
  - tests/unit/worker/test_worker.py::TestWorkerUniqueRelease::test_start_mode_releases_at_pickup
  - tests/unit/worker/test_worker.py::TestWorkerUniqueRelease::test_start_mode_rate_limited_keeps_flag_for_redelivery
  - tests/unit/worker/test_befund_f4_18_failed_redeliver_keeps_unique_flag.py::test_failed_redeliver_after_failed_deferral_keeps_the_unique_flag
  - tests/unit/worker/test_failed_redeliver_requeue_paths.py::test_failed_nack_on_a_requeue_path_keeps_the_unique_flag
---

**Entscheidung.** Im Modus `unique_until="start"` gibt der Worker das Unique-Flag genau einmal frei, und zwar erst, wenn der Task tatsächlich gleich läuft: nach Rate-Limit- und Lock-Prüfung. Wird die Nachricht vorher zurückgestellt oder requeuet, bleibt das Flag gesetzt. Nach dem Start gibt es keine zweite Freigabe.

**Begründung.** Eine Freigabe schon beim Abholen hätte während des Requeue-Fensters ein paralleles `schedule()` an der Dedup vorbeigelassen (ADV-002). Den Modus `start` wählen Nutzer bewusst für kurze TTLs, etwa gegen doppelte Formular-Absendungen.

**Bewusste Folgen.** Während des Retry-Backoffs eines `start`-Tasks gibt es keinen Dedup-Schutz (dokumentierte Einschränkung). Ein rate-limitiertes oder auf einen Lock wartendes Duplikat bleibt abgewiesen, solange die Nachricht nicht läuft.

**Nachtrag 2026-10-14 (FS-13, KTD12).** Endet eine Nachricht terminal **vor** dem Startpunkt (Expiry, Pre-Start-Cancel, Ende der Lock-Wartezeit bzw. Lock-Render-Fehler samt DLX-Rückfall, Delivery-Limit, unbekannter Task), gibt der Worker das Flag owner-geprüft frei wie im completion-Modus. Der Zustellkontext trägt dazu den Merker `start_release_reached`, den der Runner nach der Freigabe beim Start setzt (unabhängig vom Erfolg der Freigabe); erst mit gesetztem Merker ist die terminale Freigabe im start-Modus ein No-op. Damit bleibt es bei genau einer Freigabe nach dem Start, und Zurückstellungen (`Redelivered`) geben weiter nicht frei. Restfenster wie im completion-Modus: Liegen zwei Kopien derselben `task_id` vor, kann das terminale Ende der einen das Flag freigeben, während die andere noch zurückgestellt wartet. Festgehalten in `tests/unit/worker/test_befund_fs13_unique_until_start.py` und `tests/unit/worker/test_finalize.py` (ohne `entscheidung`-Marker, außer dem E-008-Test).

**Nachtrag 2026-10-19.** Scheitert die Rückgabe an Kanal- oder Verbindungsverlust, bleibt das Flag (F4-18, 0.13.0rc2). Ein `nack(requeue=True)`, das mit `BrokerConnectionError` scheitert, gilt als erfolgte Rückgabe (WARNING `delivery_settle_failed`, Outcome `Redelivered`); der Broker stellt die unbestätigte Nachricht ohnehin neu zu. Festgeschrieben für die Rückstellungs-Gates und die übrigen Requeue-Pfade der Pipeline in den beiden oben ergänzten Tests (`start`-Fälle mit Marker E-034).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt (FS-13 ist mit dem Nachtrag behoben).
