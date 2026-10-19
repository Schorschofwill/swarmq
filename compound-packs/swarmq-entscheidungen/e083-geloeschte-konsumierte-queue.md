---
title: Wird eine konsumierte Queue bei laufendem Worker gelöscht, grenzt der Consumer-Wächter den Schaden auf diese Queue ein, meldet ERROR consumer_queue_missing und hält /ready rot; kein automatisches Herunterfahren, keine stille Neuanlage
applies_when:
  - Änderungen am Consumer-Wächter (broker/rabbitmq/consumer_watch.py), an release_queue/readmit_queue (broker/rabbitmq/robust_channels.py) oder an _consume_queue_exists (broker/rabbitmq/connection.py)
  - Änderungen am Consumer-Check von /ready (health.consumer_check, worker/lifecycle.py _readiness_checks)
  - eine Fehlersuche meldet, dass ein Worker eine gelöschte Queue nicht selbst neu anlegt oder sich nach dem Löschen nicht beendet
  - eine Fehlersuche meldet, dass /ready nach dem Löschen einer konsumierten Queue sofort rot ist, ohne SWARMQ_CONSUMER_READY_GRACE_S
  - eine Fehlersuche meldet eine NOT_FOUND-Schleife im Reconnect, weil eine konsumierte Queue gelöscht wurde, während die Verbindung schon getrennt war
  - ein Plan will die gelöschte Queue selbst neu deklarieren (Option (c)) oder den Worker beim Fehlen einer Queue herunterfahren
tags: [broker, consumer, rabbitmq, queue, readiness, reconnect, aio-pika, pf-20]
id: E-083
datum: 2026-10-19
status: gilt
quelle:
  - Georg 2026-10-19 (PF-20, Option (b) „eingrenzen und laut scheitern“; Teilfrage „kein automatisches Herunterfahren“ vom Orchestrator entschieden, Georg kann widersprechen)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (PF-20, Verdacht D der Gegenprüfung 0.14, Proben probe_d_separate und probe_d_shared)
  - Commit 64a2f9e (0.14.0, Umsetzung mit Tests)
  - src/swarmq/broker/rabbitmq/consumer_watch.py (_queue_present, missing_queues)
  - src/swarmq/broker/rabbitmq/robust_channels.py (release_queue, readmit_queue)
  - src/swarmq/broker/rabbitmq/connection.py (_consume_queue_exists)
  - src/swarmq/health.py (consumer_check, Parameter missing)
  - website/explanation/delivery-semantics.md (Deleting a consumed queue)
tests:
  - tests/unit/broker/test_consumer_watch_missing_queue.py::test_missing_queue_gets_no_consume_and_one_error
  - tests/unit/broker/test_consumer_watch_missing_queue.py::test_missing_queue_leaves_the_restore_list_of_the_channel
  - tests/unit/broker/test_consumer_watch_missing_queue.py::test_a_recreated_queue_is_readmitted_and_consumed_again
  - tests/unit/broker/test_consumer_watch_missing_queue.py::test_missing_queues_name_the_deleted_queue_until_it_returns
  - tests/unit/broker/test_consumer_watch_missing_queue.py::test_the_broker_checks_passively_on_a_throwaway_channel
  - tests/unit/broker/test_robust_channels.py::test_a_released_queue_leaves_the_restore_list_and_the_others_stay
  - tests/unit/broker/test_robust_channels.py::test_a_missing_queue_registry_fails_open_with_one_warning_per_channel
  - tests/unit/broker/test_robust_channels.py::test_a_readmitted_queue_is_back_in_the_restore_list
  - tests/unit/test_health.py::TestConsumerCheck::test_a_queue_missing_at_the_broker_is_not_ready_at_once_and_named
  - tests/unit/worker/test_consumer_watch_wiring.py::test_ready_names_a_consumed_queue_missing_at_the_broker
  - tests/integration/broker/test_consumed_queue_deleted.py::test_a_deleted_queue_is_fenced_and_the_other_queue_keeps_consuming
  - tests/integration/broker/test_consumed_queue_deleted.py::test_after_a_deleted_queue_a_reconnect_succeeds_without_a_not_found_loop
  - tests/integration/broker/test_consumed_queue_deleted.py::test_a_recreated_queue_is_consumed_again
---

**Entscheidung.** Bevor der Consumer-Wächter einen Consumer neu anmeldet (`basic.consume`), prüft er passiv auf einem Wegwerf-Kanal (`open_throwaway_channel`, nicht robust) der Consume-Verbindung, ob die Queue existiert. Die Prüfung ist wie der Kanalbezug auf `_RPC_TIMEOUT_S` begrenzt, das anschließende Schließen des Wegwerf-Kanals (nach einem Abbruch im Öffnen samt dem Ende des Öffnens) noch einmal auf `_RPC_TIMEOUT_S`; ein Takt hängt an einer Queue also höchstens zweimal `_RPC_TIMEOUT_S`. Ein Abbruch trifft nur den Wegwerf-Kanal (E-053), nie das Öffnen selbst: aiormq behielte einen mitten im Öffnen abgebrochenen Kanal halb offen. Fehlt die Queue (NOT_FOUND):

- loggt der Worker einmal je Verlust ERROR `consumer_queue_missing` mit `queue`;
- unterbleibt `basic.consume`, und der Wächter nimmt die `RobustQueue` aus der Restore-Liste ihres `RobustChannel` (`_queues`, `release_queue`), damit ein Reconnect sie nicht passiv neu deklariert;
- meldet `/ready` sofort, ohne `SWARMQ_CONSUMER_READY_GRACE_S`, `consumed queue missing at the broker: <queue>`.

Die übrigen Queues des Workers, der Consume-Kanal und die Verbindung bleiben heil, in beiden Modi von `SWARMQ_BROKER_PUBLISH_CONNECTION`; Publishes laufen weiter. Der Worker fährt nicht von selbst herunter und legt die Queue nicht neu an. Der Wächter prüft in jedem Takt erneut. Legt jemand die Queue neu an, kommt sie zurück in die Restore-Liste (`readmit_queue`), und der Wächter meldet den Consumer wieder an (WARNING `consumer_reregistered`, `reason=queue_recreated`, zählt in `swarmq_consumer_reregistrations_total`); `/ready` wird wieder grün.

Ist `_queues` nicht lesbar (aio-pika geändert), gilt fail-open: einmal je Kanal WARNING `robust_queue_release_unavailable`, ein Reconnect scheitert dann wie vor 0.14.0 an NOT_FOUND, bis der Worker neu startet.

**Begründung.** Vor 0.14.0 scheiterte `basic.consume` an NOT_FOUND und schloss den gemeinsamen Consume-Kanal (E-019): alle Queues des Workers standen, in jedem Takt kam `consumer_watch_tick_failed`, `/ready` nannte nur die gelöschte Queue, und ein Reconnect lief in eine NOT_FOUND-Schleife alle 5 s, mit `shared` samt Publishes (PF-20). Option (b) hält den Schaden auf die gelöschte Queue begrenzt, macht ihn sichtbar und bricht keine Betreiberabsicht: Eine bewusst gelöschte Queue wird nicht still neu angelegt wie bei Option (c). Ein automatisches Herunterfahren hätte einen Worker mit weiteren, gesunden Queues beendet; der Neustart, der die Queue neu deklariert, bleibt eine Entscheidung des Betreibers, `/ready` und das ERROR machen sie fällig.

**Bewusste Folgen.**
- Der Worker läuft mit einer Queue weniger weiter, bis jemand die Queue neu anlegt oder den Worker neu startet; kein Fehler. Ein Neustart deklariert die Queue mit allen swarmq-Argumenten neu. Eine von Hand neu angelegte Queue konsumiert der Wächter so, wie sie angelegt wurde.
- Startet ein anderer Worker mit derselben Queue neu (etwa eine Replik), deklariert er sie mit allen Argumenten; die laufenden Worker melden ihren Consumer danach im nächsten Takt wieder an, ohne eigenen Neustart.
- Zwischen Prüfung und `basic.consume` bleibt ein Fenster: Wird die Queue genau darin gelöscht, scheitert `basic.consume` wie vor 0.14.0. Ebenso, wenn die Queue gelöscht wird, während die Verbindung schon getrennt ist, oder ein Reconnect vor dem nächsten Takt kommt: aio-pikas Restore deklariert sie dann noch passiv und scheitert an NOT_FOUND, der Wächter kommt nicht dazwischen (er braucht eine bereite Verbindung). Dann hilft nur der Neustart. Kein Fehler.
- `/ready` wird beim Fehlen sofort rot, nicht erst nach der Gnadenfrist; die Frist gilt nur für einen Consumer, der kurz fehlt und neu angemeldet wird.
- Je Takt öffnet der Wächter für jede fehlende Queue einen Wegwerf-Kanal (passives Declare, dann Schließen).

**Überdenken nur mit neuem Argument.** Georg kann der Teilfrage „kein automatisches Herunterfahren“ widersprechen (PF-20). Eingetragen am 2026-10-19 (0.14.0).
