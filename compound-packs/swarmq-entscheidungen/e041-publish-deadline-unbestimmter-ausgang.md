---
title: Die Publish-Deadline deckelt den ganzen Aufruf; ein unbestimmter Ausgang mit Zusatzlauf wird bewusst getragen
applies_when:
  - Änderungen an der Publish-Deadline, an PublishIndeterminate oder an der Behandlung gescheiterter Retry- und DLQ-Publishes
  - eine Fehlersuche meldet einen Task, der nach einem gescheiterten DLQ-Publish erneut läuft und zugleich in der DLQ liegt
  - eine Fehlersuche meldet, dass schedule() nach PublishIndeterminate die unique-Sperre nicht freigibt
tags: [broker, publish, deadline, dlq, unique, at-least-once]
id: E-041
datum: 2026-08-29
status: gilt
quelle:
  - docs/plans/2026-08-28-001-fix-publish-deadline-port-guard-logfix-plan.md:48 (KD2, user-directed), :61 (R4), :63 (R11, Entscheidung Georg im Code-Review 2026-08-29), :82 (AE2), :179 (KTD1), :182 (KTD4)
  - website/reference/errors.md:26 und :443 (PublishIndeterminate)
tests:
  - tests/unit/client/test_publish_deadline_rollback.py::TestDeadlineErrorReachesTheCaller::test_unique_claim_survives_an_indeterminate_publish
  - tests/unit/broker/test_publish_connection_loss.py::test_dead_letter_losing_the_copy_confirm_is_indeterminate_and_keeps_the_original
---

**Entscheidung.** Die Publish-Deadline (Default 120 s) deckelt den gesamten Publish-Vorgang samt Wiederholungen, nicht den einzelnen Versuch. Reißt sie, während der Broker die Nachricht schon haben kann, ist der Ausgang unbestimmt (`PublishIndeterminate`, Unterklasse von `BrokerConnectionError`): Der Aufrufer darf nicht von Nicht-Zustellung ausgehen. Scheitert so ein Retry- oder DLQ-Publish, bleibt die auslösende Nachricht unbestätigt und wird neu zugestellt. Im Client bleibt die Unique-Sperre bei unbestimmtem Ausgang stehen.

**Begründung.** Der Aufrufer bekommt eine planbare Obergrenze statt eines unbegrenzt hängenden Publish. Eine zusätzliche Ausführung ist billiger als ein stiller Verlust (AE2). Bei der Unique-Sperre ist Blockieren die richtige Richtung für ein Einmaligkeits-Feature.

**Bewusste Folgen.** Nach einem unbestimmten DLQ-Publish läuft der Task mindestens ein weiteres Mal, möglicherweise neben einem Eintrag in der DLQ. Ein Wiederholversuch von `schedule()` nach `PublishIndeterminate` wird bis zum Ablauf der Sperre als Duplikat abgewiesen. Beides ist kein Fehler.

**Überdenken nur mit neuem Argument.** Die Entscheidung trägt den Preis für eine Frist, die **mitten in einem Versuch** reißt. FS-02 Teil a ist behoben (U3, Plan 2026-10-14-0748): Ist die Frist schon **vor Beginn** verstrichen (systematisch am Drain-Ende, siehe E-001), sendet swarmq nichts mehr und meldet `BrokerConnectionError` statt `PublishIndeterminate` (E-042). Seit U8 (FS-08, Ergänzung a3) gilt das auch für eine Frist, die erst beim Kanalaufbau oder im Backoff reißt, solange noch kein Versuch gesendet haben kann (erster Versuch, oder alle vorigen nachweislich nicht gesendet). Reißt sie mitten in einem Versuch oder nach einem womöglich gesendeten (etwa nach einem negativen Confirm), bleibt es bei `PublishIndeterminate`. Festgehalten in `tests/unit/broker/test_publish_channel_closing.py` (ohne `entscheidung`-Marker).
