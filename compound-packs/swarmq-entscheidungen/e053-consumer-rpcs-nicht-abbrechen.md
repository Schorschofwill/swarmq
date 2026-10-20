---
title: Consumer-RPCs (basic.cancel, basic.consume) werden nie abgebrochen; Stoppen koppelt ab, begrenzt wird nur der Kanalbezug, Notbremse 60 s
applies_when:
  - Änderungen am Consumer-Wächter (broker/rabbitmq/consumer_watch.py), an cancel_consume oder am Shutdown-Schritt cancel_consume
  - eine Fehlersuche meldet, dass ein hängendes basic.cancel oder basic.consume nicht per Timeout abgebrochen wird
  - eine Fehlersuche meldet, dass stop() einen laufenden Wächter-Takt nicht abbricht oder cancel_consume nach dem Shutdown weiterläuft
  - ein Plan will ein wait_for um Consumer-RPCs legen
tags: [broker, consumer, rabbitmq, aiormq, shutdown]
id: E-053
datum: 2026-10-14
status: gilt
quelle:
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:175-179 (KTD16, Consumer-Wächter bricht keine RPCs ab)
  - Commit 7643879 (U10, FS-09) und Review 3348b96 (cancel_consume folgt dem SIGTERM-Anker, kein Consume nach stop(), Kanalbezug begrenzt)
  - src/swarmq/broker/rabbitmq/consumer_watch.py:40-59 (_RPC_TIMEOUT_S, _RPC_EMERGENCY_TIMEOUT_S, _STOP_TIMEOUT_S), :306-322 (späte consume-Antwort)
  - src/swarmq/worker/shutdown.py:125-153 (_cancel_consume)
tests:
  - tests/unit/broker/test_befund_fs09_consumer_watch_rpc_abort.py::test_fs09_slow_basic_cancel_does_not_close_the_consume_channel
  - tests/unit/broker/test_befund_fs09_consumer_watch_rpc_abort.py::test_fs09_slow_basic_consume_does_not_close_the_consume_channel
  - tests/unit/broker/test_consumer_watch_shutdown.py::test_stop_during_a_running_rpc_sends_no_channel_close
  - tests/unit/broker/test_consumer_watch_shutdown.py::test_a_late_consume_answer_after_cancel_all_is_cancelled_at_once
  - tests/unit/broker/test_consumer_watch_shutdown.py::test_cancel_all_gives_the_rpc_the_emergency_timeout
  - tests/unit/worker/test_shutdown_budget_steps.py::test_hanging_cancel_consume_is_bounded_without_aborting_it
  - tests/unit/broker/test_consumer_watch_shutdown.py::test_a_late_consume_answer_after_stop_is_cancelled_although_the_entry_is_still_listed
---

**Entscheidung.** Der Consumer-Wächter bricht `basic.cancel` und `basic.consume` nie per `wait_for` ab. Begrenzt wird nur der Kanalbezug (`_RPC_TIMEOUT_S`, 2 s, im Takt und vor jedem RPC); das RPC selbst hat nur aiormqs eigene Frist als Notbremse (`_RPC_EMERGENCY_TIMEOUT_S`, 60 s). `stop()` wartet höchstens `_STOP_TIMEOUT_S` (5 s) auf einen laufenden Takt und koppelt ihn danach ab, statt ihn abzubrechen. Nach `stop()` bzw. `cancel_all` übernimmt der Wächter keine Neuanmeldung mehr; kommt eine `consume`-Antwort danach, wird der Tag sofort wieder abgemeldet. Im Shutdown wird `cancel_consume` bis zum Drain-Ende (mindestens `_SHUTDOWN_STEP_MIN_S`, bei SIGTERM im Recycling-Drain bis zum Anker) abgewartet, aber nicht abgebrochen; es läuft danach im Hintergrund weiter, bis der Broker antwortet, die Notbremse greift oder `broker.close()` den Kanal schließt.

**Begründung.** Ein abgebrochenes RPC beantwortet aiormq mit `Channel.Close(504)`. Auf diesem Kanal liegen alle Consumer des Workers (E-019); alle unbestätigten Zustellungen kämen zurück, während ihre Handler noch laufen (FS-09). Die Notbremse schließt den Kanal erst, wenn der Broker so lange stumm war, dass er ohnehin verloren ist.

**Bewusste Folgen.** Ein `basic.cancel`, das länger als das Drain-Ende braucht, ist kein Fehler; der Shutdown loggt `shutdown_step_timeout` (`step=cancel_consume`) und läuft weiter, ein späterer Fehler erscheint als `cancel_consume_partial_failure` mit `late=True`. Ein abgekoppelter Takt kann bis zur Notbremse weiterlaufen. Zustellungen, die bis zur Antwort des Brokers noch ankommen, gehören zum Drain (E-050).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
