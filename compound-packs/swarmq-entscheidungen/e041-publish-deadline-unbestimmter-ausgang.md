---
title: Die Publish-Deadline deckelt den ganzen Aufruf; ein unbestimmter Ausgang mit Zusatzlauf wird bewusst getragen
applies_when:
  - Änderungen an der Publish-Deadline, an PublishIndeterminate oder an der Behandlung gescheiterter Retry- und DLQ-Publishes
  - eine Fehlersuche meldet einen Task, der nach einem gescheiterten DLQ-Publish erneut läuft und zugleich in der DLQ liegt
  - eine Fehlersuche meldet, dass schedule() nach PublishIndeterminate die unique-Sperre nicht freigibt
  - eine Fehlersuche meldet einen Zusatzlauf nach task_defer_failed_redelivered oder task_expired_dlq_publish_failed
tags: [broker, publish, deadline, dlq, unique, at-least-once]
id: E-041
datum: 2026-08-29
status: gilt
quelle:
  - docs/plans/2026-08-28-001-fix-publish-deadline-port-guard-logfix-plan.md:48 (KD2, user-directed), :61 (R4), :63 (R11, Entscheidung Georg im Code-Review 2026-08-29), :82 (AE2), :179 (KTD1), :182 (KTD4)
  - website/reference/errors.md:26 und :443 (PublishIndeterminate)
  - Georg 2026-10-17 (Fehlersuche Runde 4, offene Registerfrage aus 2b/2c)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (Vor dem Fix Punkt 16)
  - 'Georg 2026-10-19 (Entscheidungsvorlage 0.13.0, Punkt 6, Option C: Abbruch im Publish hält die Unique-Sperre, 0.13.1)'
tests:
  - tests/unit/client/test_publish_deadline_rollback.py::TestDeadlineErrorReachesTheCaller::test_unique_claim_survives_an_indeterminate_publish
  - tests/unit/broker/test_publish_connection_loss.py::test_dead_letter_losing_the_copy_confirm_is_indeterminate_and_keeps_the_original
  - tests/unit/worker/test_retry.py::test_indeterminate_retry_publish_redelivers_the_original_without_ack
  - tests/unit/worker/gates/test_backend_defer.py::test_failed_deferral_publish_falls_back_to_redeliver
  - tests/unit/worker/gates/test_expiry.py::TestExpiryErrorFields::test_indeterminate_dlq_publish_redelivers_the_expired_original
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_cancel_during_the_publish_keeps_the_unique_claim
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_cancel_of_schedule_many_during_the_publish_keeps_the_unique_claims
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_publish_of_schedule_many_ending_in_cancelled_error_keeps_its_claim
---

**Entscheidung.** Die Publish-Deadline (Default 120 s) deckelt den gesamten Publish-Vorgang samt Wiederholungen, nicht den einzelnen Versuch. Reißt sie, während der Broker die Nachricht schon haben kann, ist der Ausgang unbestimmt (`PublishIndeterminate`, Unterklasse von `BrokerConnectionError`): Der Aufrufer darf nicht von Nicht-Zustellung ausgehen. Scheitert so ein Retry- oder DLQ-Publish, bleibt die auslösende Nachricht unbestätigt und wird neu zugestellt. Im Client bleibt die Unique-Sperre bei unbestimmtem Ausgang stehen.

**Begründung.** Der Aufrufer bekommt eine planbare Obergrenze statt eines unbegrenzt hängenden Publish. Eine zusätzliche Ausführung ist billiger als ein stiller Verlust (AE2). Bei der Unique-Sperre ist Blockieren die richtige Richtung für ein Einmaligkeits-Feature.

**Bewusste Folgen.** Nach einem unbestimmten DLQ-Publish läuft der Task mindestens ein weiteres Mal, möglicherweise neben einem Eintrag in der DLQ. Ein Wiederholversuch von `schedule()` nach `PublishIndeterminate` wird bis zum Ablauf der Sperre als Duplikat abgewiesen. Beides ist kein Fehler.

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg): vier Pfade.** „Scheitert so ein Retry- oder DLQ-Publish, bleibt die auslösende Nachricht unbestätigt und wird neu zugestellt“ gilt für alle vier Kopie-Pfade des Workers: die Retry-Kopie (`retry_publish_failed`), die DLQ-Kopie (`dlq_publish_failed`), die Zurückstellung bei Backend-Ausfall (`task_defer_failed_redelivered`) und die Ablauf-DLQ (`task_expired_dlq_publish_failed`). Ein Zusatzlauf oder eine zusätzliche Kopie nach unbestimmtem Ausgang ist auf jedem dieser Pfade kein Fehler. Festgeschrieben für Retry, Zurückstellung und Ablauf-DLQ in den drei neuen Tests unter `tests:`, für die DLQ-Kopie im bestehenden Broker-Test.

**Überdenken nur mit neuem Argument.** Die Entscheidung trägt den Preis für eine Frist, die **mitten in einem Versuch** reißt. FS-02 Teil a ist behoben (U3, Plan 2026-10-14-0748): Ist die Frist schon **vor Beginn** verstrichen (systematisch am Drain-Ende, siehe E-001), sendet swarmq nichts mehr und meldet `BrokerConnectionError` statt `PublishIndeterminate` (E-042). Seit U8 (FS-08, Ergänzung a3) gilt das auch für eine Frist, die erst beim Kanalaufbau oder im Backoff reißt, solange noch kein Versuch gesendet haben kann (erster Versuch, oder alle vorigen nachweislich nicht gesendet). Reißt sie, nachdem ein Versuch seine Frames eingereiht hat, oder nach einem womöglich gesendeten (etwa nach einem negativen Confirm, auch wenn danach ein nachweislich nicht gesendeter Versuch folgte), bleibt es bei `PublishIndeterminate`. Steht der Versuch beim Reißen noch vor dem Einreihen (Warteschlange der Kanal-Sperre bei offener Verbindung), gilt er als nicht gesendet (Review U8, E-042). Festgehalten in `tests/unit/broker/test_publish_channel_closing.py` (ohne `entscheidung`-Marker).

**Nachtrag 2026-10-19 (0.13.1; Georg, Entscheidungsvorlage 0.13.0, Punkt 6, Option C): Abbruch im Publish.** „Im Client bleibt die Unique-Sperre bei unbestimmtem Ausgang stehen“ gilt auch für einen Abbruch (`asyncio.CancelledError`, etwa Timeout oder `task.cancel()` beim Aufrufer) während des Publish im sofortigen Pfad von `schedule()` und in `schedule_many()`: aio-pika kann die Frames schon geschrieben haben, der Ausgang ist so unbestimmt wie bei `PublishIndeterminate`. Bis 0.13.0 gab ein roher Abbruch im sofortigen Pfad die Sperre frei. Der `CancelledError` wird weitergeworfen, nie verschluckt; in `schedule_many` zählt ein Publish, der mit `CancelledError` endet, wie ein unbestimmter Eintrag (seine Sperre bleibt, der Batch wirft den `CancelledError` vor einem bestimmten Fehler). Ein Abbruch in `pre_enqueue`, also vor dem Publish, gibt weiter frei: Dann ist sicher nichts gesendet. Für den verzögerten Pfad und die CLI regelt E-059 (Nachtrag 0.13.1) dasselbe, für den Abbruch beim Setzen des Claims E-052.

