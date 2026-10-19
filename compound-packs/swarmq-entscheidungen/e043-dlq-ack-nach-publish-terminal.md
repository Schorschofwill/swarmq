---
title: Liegt die DLQ-Kopie sicher in der XQ und scheitert nur das Ack des Originals, gilt das als terminal wie ein Erfolg
applies_when:
  - Änderungen an dead_letter_copy, an Gates mit DLQ-Pfad (Expiry, Lock, Delivery-Limit) oder an SettleFailedAfterPublish
  - eine Fehlersuche meldet Unique-Freigabe oder TASK_DLQ trotz gescheitertem Ack nach dem DLQ-Publish
  - ein neues Gate leitet Nachrichten in die DLQ
  - Änderungen an der Reihenfolge von Delivery-Count-Gate und Ausführungsschutz (pipeline.py _pre_pipeline_gates, gates/delivery_count.py)
  - eine Fehlersuche meldet retry_publish_failed auf ERROR, obwohl die Retry-Kopie liegt
  - Änderungen an einer Rückstellungs-Kopie (retry_later mit gleichem Retry-Zähler) eines Gates oder an publish_deferral_copy
  - eine Fehlersuche meldet rate_limit_requeue_failed, lock_conflict_requeue_failed oder ein *_defer_failed_redelivered, obwohl die Kopie liegt
tags: [dlq, ack, execution-guard, unique, broker]
id: E-043
datum: 2026-10-09
status: gilt
quelle:
  - Commit 417df44 (Dead-Letter mit gescheitertem Ack nach dem Publish gilt als terminal)
  - Commit 9cf72c7 (Ausführungsschutz U8, SettleFailedAfterPublish)
  - src/swarmq/worker/gates/__init__.py:304-308 (dead_letter_copy)
  - website/explanation/delivery-semantics.md:316-320
  - 'Commit b69dd89 (U16, FS-17: Expiry-Gate nutzt dead_letter_copy)'
  - Georg 2026-10-17 (Fehlersuche Runde 4, F4-02, F4-15)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (FEHLER F4-02, F4-15; Vor dem Fix Punkte 2 und 13)
  - Georg 2026-10-19 (Entscheidungsvorlage 0.13.0, Punkt 5, Option B; FEHLER F4-17, F4-18)
tests:
  - tests/unit/worker/test_retry.py::test_dead_letter_with_failed_ack_after_publish_is_terminal
  - tests/unit/worker/test_retry.py::test_retry_copy_with_failed_ack_after_publish_counts_as_retried
  - tests/unit/worker/gates/test_lock_dlq_ack_failed.py::test_render_error_with_failed_ack_after_publish_is_terminal
  - tests/unit/worker/gates/test_lock_dlq_ack_failed.py::test_wait_limit_with_failed_ack_after_publish_is_terminal
  - tests/unit/worker/gates/test_befund_fs17_expiry_ack_failed.py::test_fs17_expired_copy_with_failed_ack_is_terminal
  - tests/unit/worker/test_befund_f4_02_delivery_limit_before_guard.py::test_completed_execution_redelivered_at_the_limit_stays_a_duplicate
  - tests/unit/worker/test_befund_f4_02_delivery_limit_before_guard.py::test_superseded_attempt_redelivered_at_the_limit_stays_a_duplicate
  - tests/unit/worker/test_befund_f4_15_settle_failed_after_publish_log_level.py::test_retry_copy_published_but_ack_failed_is_not_reported_as_failed_publish
  - tests/unit/worker/gates/test_execution_guard_skip_if_settled.py::test_an_expired_done_marker_leaves_the_dead_letter
  - tests/unit/worker/gates/test_befund_f4_17_deferral_copy_ack_failed_log_level.py::test_deferral_copy_published_but_ack_failed_logs_no_error
  - tests/unit/worker/gates/test_deferral_copy_ack_failed.py::test_published_deferral_copy_with_failed_ack_counts_as_deferred
  - tests/unit/worker/gates/test_unknown_task_grace.py::test_published_copy_with_failed_ack_counts_as_deferred
  - tests/unit/worker/gates/test_schedfire_defer.py::test_published_deferral_with_failed_ack_counts_as_deferred
  - tests/unit/worker/gates/test_redeliver_in_place.py::test_copy_published_with_failed_ack_is_not_nacked
---

**Entscheidung.** Hat der Broker die DLQ-Kopie bestätigt und scheitert danach nur das Ack des Originals (`SettleFailedAfterPublish`, etwa bis RabbitMQ 4.2 nach einem Consumer-Timeout), behandelt der Worker das wie einen erfolgreichen Dead-Letter: Warnung `dlq_ack_failed_after_publish`, Unique-Freigabe und `TASK_DLQ`. Die Neuzustellung des Originals überspringt der Ausführungsschutz als `done`.

**Begründung.** Die Kopie liegt in der DLQ; eine Behandlung als gescheiterter Publish hätte das Unique-Flag bis zur TTL stehen lassen, weil die übersprungene Neuzustellung die Freigabe nie mehr erreicht.

**Bewusste Folgen.** Ein `TASK_DLQ` und eine Unique-Freigabe trotz gescheitertem Ack sind kein Fehler. Mit `SWARMQ_EXECUTION_GUARD=false` kann die Neuzustellung erneut verarbeitet werden.

**Nachtrag 2026-10-14 (FS-17, U16).** Behoben. Das Expiry-Gate publiziert die DLQ-Kopie seit U16 über `dead_letter_copy` (`src/swarmq/worker/gates/expiry.py`) und wertet `SettleFailedAfterPublish` damit wie die übrigen Gates als terminal: Warnung `dlq_ack_failed_after_publish`, Unique-Freigabe, kein `redeliver`; die Neuzustellung des Originals überspringt der Ausführungsschutz. Festgeschrieben in `tests/unit/worker/gates/test_befund_fs17_expiry_ack_failed.py::test_fs17_expired_copy_with_failed_ack_is_terminal`.

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg; F4-02, F4-15).** (1) *Done/Superseded geht dem Zustelllimit vor (F4-02, R4Z-04).* Erreicht die Neuzustellung einer erledigten (Done-Marker) oder überholten (Superseded-Marker) Ausführung das Zustelllimit, liest das Delivery-Count-Gate vor dem Dead-Letter den Zustand des Ausführungsschutzes: `done`/`superseded` → bestätigen und überspringen (`duplicate_delivery_skipped`), sonst DLQ wie bisher. Kein Fehlerergebnis über dem Erfolg, kein `TASK_DLQ`, keine Unique-Freigabe. Doku: Route 3 in `delivery-semantics.md:642-645`; nach dem Fix entfallen die Klammer `RetryChainMachine.deliverable` und die K7-Ausnahme. (2) *Retry-Kopie entsprechend (F4-15, R4I-03).* Hat der Broker die Retry-Kopie bestätigt und scheitert nur das Ack des Originals (`SettleFailedAfterPublish` aus `retry_later`), gilt das wie bei der DLQ-Kopie als erledigt: WARNING `retry_ack_failed_after_publish`, `task_retrying` wird geloggt, kein `redeliver_in_place`, kein Requeue; die Neuzustellung des Originals überspringt der Ausführungsschutz als `superseded`. Katalogzeile EN + DE. Festgeschrieben mit den Nachweisen F4-02 und F4-15 (Marker E-043), seit 0.13.0 ohne `xfail` (U12, U9); die Klammer `deliverable` ist entfallen, die K7-Ausnahme bleibt (Nachtrag 2026-10-18).

**Nachtrag 2026-10-18 (0.13.0, U12; F4-02 behoben, kleine Variante KD5).** Das Delivery-Count-Gate liest am Zustelllimit vor dem Dead-Letter den Zustand des Ausführungsschutzes (`ExecutionGuardStore.peek`, nur `HMGET`, kein Claim, kein Lua) und bestätigt erledigte oder überholte Ausführungen wie der Ausführungsschutz (`duplicate_delivery_skipped`). Ohne Marker, mit `SWARMQ_EXECUTION_GUARD=false`, bei FIRE_AND_FORGET und wenn das Lesen scheitert (WARNING `execution_guard_peek_failed`), bleibt es beim Dead-Letter. Zwei bekannte Grenzen bleiben bewusst bestehen: (a) Die Zustelllimit-DLQ setzt weiterhin keinen Done-Marker; geht ihr Ack verloren, geht die Neuzustellung erneut in die DLQ (doppelte DLQ-Kopie), und eine Kopie derselben Identität ohne Broker-Zähler läuft danach; die K7-Ausnahme der Retry-Maschine bleibt deshalb bestehen, nur die Klammer `deliverable` entfällt. (b) Ist der Done-Marker bei Erreichen des Limits schon abgelaufen (`execution_guard_done_ttl_s`), liefert `peek` nichts, und die Limit-DLQ überschreibt das Ergebnis wie vor dem Fix. Festgeschrieben in `test_an_expired_done_marker_leaves_the_dead_letter`; die große Variante (besitzerloser Done-Marker nach dem Dead-Letter) ist zurückgestellt.

**Nachtrag 2026-10-19 (Georg, Entscheidungsvorlage 0.13.0 Punkt 5, Option B; F4-17, 0.13.0rc2).** *Gilt für alle Rückstellungs-Kopien.* Hat der Broker die Kopie einer Rückstellung bestätigt (`retry_later` mit gleichem Retry-Zähler: Rate-Limit, Lock-Konflikt, Ausführungsschutz, Backend-Zurückstellung, dazu unbekannter Task, Schedfire-Zurückstellung und die Claim-Kopie aus `redeliver_in_place`) und scheitert nur das Ack des Originals (`SettleFailedAfterPublish`), ist die Rückstellung erfolgt: WARNING `deferral_ack_failed_after_publish` (Feld `deferral`), die übliche Zeile des Gates, Outcome `Redelivered`, kein `redeliver_in_place`, das Unique-Flag bleibt. Umgesetzt im Helfer `publish_deferral_copy` (`src/swarmq/worker/gates/__init__.py`). *Ohne Superseded-Marker.* Anders als bei Retry- und DLQ-Kopie setzt `GuardedBroker.retry_later` bei Rückstellungen keinen Marker (gleicher Retry-Zähler; Running wird vor dem Publish freigegeben). Der Broker stellt das Original neu zu; Kopie und Original laufen beide durch die Gates, der Ausführungsschutz serialisiert sie (die zweite Zustellung derselben Identität wird zurückgestellt oder läuft nach der ersten). *Begründung:* Die Kopie liegt; ein Requeue auf dem toten Kanal ändert nichts (der Broker stellt das Original ohnehin neu zu), und ERROR widerspräche der Level-Regel aus 0.8.0 (selbstheilend → WARNING) wie bei F4-15. Ein Superseded-Marker wäre falsch: die Rückstellung ist keine neue Ausführung, sondern dieselbe, und ein Marker ließe das Original überspringen, obwohl die Kopie noch durch dieselben Gates muss. *Bewusste Folgen:* Mit `SWARMQ_EXECUTION_GUARD=false` können Kopie und Original beide laufen (wie bisher, „Bewusste Folgen“ oben; E-041 Nachtrag). Ein echter Publish-Fehler nimmt weiter den alten Weg (Zeile des Gates, `redeliver_in_place`). Zusammen mit F4-18: Scheitert danach das `nack` an Kanal- oder Verbindungsverlust, gilt die Rückgabe als erfolgt (WARNING `delivery_settle_failed`), das Unique-Flag bleibt. Festgeschrieben in den oben ergänzten Tests (Marker E-043).

**Überdenken nur mit neuem Argument.** Bis U16 hielt sich das Expiry-Gate nicht an diese Regel (FS-17): Es wertete den Fall als gescheiterten Publish, stellte das Original per `redeliver` zurück, und das Unique-Flag hing bis `unique_for`. Das war ein Fehler gegen diese Entscheidung, keine offene Frage. Keine offenen Einwände bekannt.
