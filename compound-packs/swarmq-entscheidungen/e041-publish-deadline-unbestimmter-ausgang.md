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
  - 'docs/plans/2026-10-19-1500-feat-swarmq-0-14-plan.md (R4, KTD4, U4; offene Frage 4 mit (b) entschieden: Kriterium maybe_sent wie E-042)'
tests:
  - tests/unit/client/test_publish_deadline_rollback.py::TestDeadlineErrorReachesTheCaller::test_unique_claim_survives_an_indeterminate_publish
  - tests/unit/broker/test_publish_connection_loss.py::test_dead_letter_losing_the_copy_confirm_is_indeterminate_and_keeps_the_original
  - tests/unit/worker/test_retry.py::test_indeterminate_retry_publish_redelivers_the_original_without_ack
  - tests/unit/worker/gates/test_backend_defer.py::test_failed_deferral_publish_falls_back_to_redeliver
  - tests/unit/worker/gates/test_expiry.py::TestExpiryErrorFields::test_indeterminate_dlq_publish_redelivers_the_expired_original
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_cancel_during_the_publish_keeps_the_unique_claim
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_cancel_of_schedule_many_during_the_publish_keeps_the_unique_claims
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_publish_of_schedule_many_ending_in_cancelled_error_keeps_its_claim
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_schedule_many_with_a_failed_and_a_cancelled_publish_raises_cancelled_and_keeps_only_that_claim
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_cancel_in_pre_enqueue_before_the_publish_releases_the_unique_claim
  - tests/unit/broker/test_publish_blocked.py::test_own_cancel_while_blocked_is_marked_as_unsent
  - tests/unit/broker/test_publish_channel_closing.py::test_an_own_cancel_while_waiting_for_the_channel_lock_is_marked_unsent
  - tests/unit/broker/test_publish_channel_closing.py::test_an_own_cancel_in_the_backoff_after_an_unsent_attempt_is_marked_unsent
  - tests/unit/broker/test_publish_channel_closing.py::test_an_own_cancel_in_the_backoff_after_a_nacked_attempt_stays_unmarked
  - tests/unit/broker/test_publish_channel_closing.py::test_an_own_cancel_while_the_channel_opens_is_marked_unsent
  - tests/unit/broker/test_publish_channel_closing.py::test_an_own_cancel_after_the_frame_was_enqueued_stays_unmarked
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_cancel_marked_unsent_by_the_broker_releases_the_unique_claim
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_caller_timeout_around_schedule_raises_timeout_and_releases_the_claim
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_second_cancel_during_the_release_is_not_swallowed
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_schedule_many_releases_only_the_claim_of_a_publish_marked_unsent
  - tests/unit/client/test_cancel_keeps_unique_claim.py::test_cancel_of_schedule_many_with_publishes_marked_unsent_releases_their_claims
  - tests/integration/broker/test_blocked_cancel_releases_claim.py::test_cancel_while_the_broker_blocks_releases_the_unique_claim
---

**Entscheidung.** Die Publish-Deadline (Default 120 s) deckelt den gesamten Publish-Vorgang samt Wiederholungen, nicht den einzelnen Versuch. Reißt sie, während der Broker die Nachricht schon haben kann, ist der Ausgang unbestimmt (`PublishIndeterminate`, Unterklasse von `BrokerConnectionError`): Der Aufrufer darf nicht von Nicht-Zustellung ausgehen. Scheitert so ein Retry- oder DLQ-Publish, bleibt die auslösende Nachricht unbestätigt und wird neu zugestellt. Im Client bleibt die Unique-Sperre bei unbestimmtem Ausgang stehen.

**Begründung.** Der Aufrufer bekommt eine planbare Obergrenze statt eines unbegrenzt hängenden Publish. Eine zusätzliche Ausführung ist billiger als ein stiller Verlust (AE2). Bei der Unique-Sperre ist Blockieren die richtige Richtung für ein Einmaligkeits-Feature.

**Bewusste Folgen.** Nach einem unbestimmten DLQ-Publish läuft der Task mindestens ein weiteres Mal, möglicherweise neben einem Eintrag in der DLQ. Ein Wiederholversuch von `schedule()` nach `PublishIndeterminate` wird bis zum Ablauf der Sperre als Duplikat abgewiesen. Beides ist kein Fehler.

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg): vier Pfade.** „Scheitert so ein Retry- oder DLQ-Publish, bleibt die auslösende Nachricht unbestätigt und wird neu zugestellt“ gilt für alle vier Kopie-Pfade des Workers: die Retry-Kopie (`retry_publish_failed`), die DLQ-Kopie (`dlq_publish_failed`), die Zurückstellung bei Backend-Ausfall (`task_defer_failed_redelivered`) und die Ablauf-DLQ (`task_expired_dlq_publish_failed`). Ein Zusatzlauf oder eine zusätzliche Kopie nach unbestimmtem Ausgang ist auf jedem dieser Pfade kein Fehler. Festgeschrieben für Retry, Zurückstellung und Ablauf-DLQ in den drei neuen Tests unter `tests:`, für die DLQ-Kopie im bestehenden Broker-Test.

**Überdenken nur mit neuem Argument.** Die Entscheidung trägt den Preis für eine Frist, die **mitten in einem Versuch** reißt. FS-02 Teil a ist behoben (U3, Plan 2026-10-14-0748): Ist die Frist schon **vor Beginn** verstrichen (systematisch am Drain-Ende, siehe E-001), sendet swarmq nichts mehr und meldet `BrokerConnectionError` statt `PublishIndeterminate` (E-042). Seit U8 (FS-08, Ergänzung a3) gilt das auch für eine Frist, die erst beim Kanalaufbau oder im Backoff reißt, solange noch kein Versuch gesendet haben kann (erster Versuch, oder alle vorigen nachweislich nicht gesendet). Reißt sie, nachdem ein Versuch seine Frames eingereiht hat, oder nach einem womöglich gesendeten (etwa nach einem negativen Confirm, auch wenn danach ein nachweislich nicht gesendeter Versuch folgte), bleibt es bei `PublishIndeterminate`. Steht der Versuch beim Reißen noch vor dem Einreihen (Warteschlange der Kanal-Sperre bei offener Verbindung), gilt er als nicht gesendet (Review U8, E-042). Festgehalten in `tests/unit/broker/test_publish_channel_closing.py` (ohne `entscheidung`-Marker).

**Nachtrag 2026-10-19 (0.13.1; Georg, Entscheidungsvorlage 0.13.0, Punkt 6, Option C): Abbruch im Publish.** „Im Client bleibt die Unique-Sperre bei unbestimmtem Ausgang stehen“ gilt auch für einen Abbruch (`asyncio.CancelledError`, etwa Timeout oder `task.cancel()` beim Aufrufer) während des Publish im sofortigen Pfad von `schedule()` und in `schedule_many()`: aio-pika kann die Frames schon geschrieben haben, der Ausgang ist so unbestimmt wie bei `PublishIndeterminate`. Bis 0.13.0 gab ein roher Abbruch im sofortigen Pfad die Sperre frei. Der `CancelledError` wird weitergeworfen, nie verschluckt; in `schedule_many` zählt ein Publish, der mit `CancelledError` endet, wie ein unbestimmter Eintrag (seine Sperre bleibt, der Batch wirft den `CancelledError` vor einem bestimmten Fehler). Ein Abbruch in `pre_enqueue`, also vor dem Publish, gibt weiter frei: Dann ist sicher nichts gesendet. Für den verzögerten Pfad und die CLI regelt E-059 (Nachtrag 0.13.1) dasselbe, für den Abbruch beim Setzen des Claims E-052.

**Bewusste Folge (Review 0.13.1, Georg 2026-10-19): Abbruch bei blockiertem Broker.** Der Broker wartet vor jedem Senden, solange die Publish-Verbindung blockiert ist (`connection.blocked`, Ressourcenalarm; `_wait_while_blocked`, `broker/rabbitmq/delivery.py:332`). Ein Abbruch des Aufrufers in dieser Wartezeit hält den Claim, obwohl nachweislich nichts gesendet wurde; der Client sieht nur den `CancelledError` aus `broker.publish` und kann die Phase nicht unterscheiden. Ohne Abbruch endet derselbe Fall nach der Publish-Frist als `BrokerPublishBlocked` und gibt die Sperre frei. Das ist Option C und kein Fehler: Ein Wiederholversuch wird bis zum Ablauf der Sperre als Duplikat abgewiesen.

**Überdenken für 0.14 (neues Argument, Review 0.13.1).** Die Lücke zwischen „abgebrochen, nichts gesendet“ und `BrokerPublishBlocked` ließe sich schließen, wenn der Broker beim eigenen Abbruch die Phase „nichts gesendet“ mitreicht (etwa als Attribut am weitergeworfenen `CancelledError` oder als eigene Rückmeldung), so dass der Client in diesem Fall freigibt. Bis dahin gilt die Regel oben.

**Nachtrag 2026-10-19 (0.14, Plan U4; löst „Bewusste Folge (Review 0.13.1)“ und „Überdenken für 0.14“ auf): Abbruch, bevor etwas gesendet wurde.** Bricht der Aufrufer den Publish ab, bevor ein Versuch gesendet haben kann, gibt der Client die Unique-Sperre frei. Maßgeblich ist dasselbe Kriterium wie für die Frist nach E-042 (`maybe_sent`, offene Frage 4 mit (b) entschieden): das Blocked-Warten (`_wait_while_blocked`), der Kanalaufbau, die Warteschlange der Kanal-Sperre und der Backoff nach nachweislich nicht gesendeten Versuchen. Der RabbitMQ-Broker meldet diesen Fall als `PublishCancelledUnsent` (`swarmq.exceptions`, nicht in `swarmq.__all__`), eine Unterklasse von `asyncio.CancelledError`. Der Abbruch bleibt damit ein Abbruch: `task.cancelled()` ist `True`, `asyncio.timeout` beim Aufrufer macht daraus `TimeoutError`, und nichts wird verschluckt. Der Client prüft den Ausgang mit `is_publish_outcome_unknown` (`swarmq.exceptions`) und gibt im sofortigen Pfad und in `schedule_many` frei, auch wenn der Aufrufer `schedule_many` abbricht. `gather` ersetzt den Abbruch eines Kinds durch einen neuen `CancelledError`, deshalb hält der Client die Markierung je Eintrag selbst fest. Die Freigabe läuft im abgebrochenen Task. Ein zweiter Abbruch während der Freigabe lässt die Sperre konservativ stehen und geht weiter.

Unverändert bleibt die Sperre stehen:
- nach einem Abbruch, nachdem ein Versuch seine Frames eingereiht hat;
- nach einem Abbruch nach einem womöglich gesendeten Versuch, etwa nach einem negativen Confirm;
- bei einem fremden `CancelledError` aus aio-pika und bei einem Handler, den der Drain schon abgebrochen hat (KTD9);
- bei Publishern ohne die Markierung: Ein unmarkierter Abbruch gilt weiter als unbestimmt (Vertrag `Publisher.publish`).

Reißt die eigene Publish-Frist im Blocked-Warten, bleibt es bei `BrokerPublishBlocked` (E-025). Der verzögerte Pfad und die CLI senden nicht an den Broker; für sie gilt weiter E-059, Nachtrag 0.13.1.
