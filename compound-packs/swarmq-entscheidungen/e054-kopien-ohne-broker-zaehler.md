---
title: Kopien, die swarmq publiziert, tragen keine Broker-Zustellzähler; nur Broker-DLX-Kopien behalten sie
applies_when:
  - Änderungen an identity.copy_headers, BROKER_DELIVERY_COUNTERS oder an Pfaden, die eine Kopie publizieren (Retry, Zurückstellung, DLQ-Stempel, dlq retry)
  - eine Fehlersuche meldet, dass ein DLQ-Eintrag die Zustellzahl vor dem Dead-Lettern nicht mehr zeigt
  - eine Fehlersuche meldet, dass ein Replay einer Broker-DLX-Kopie sofort wieder in die DLQ geht
tags: [dlq, header, replay, delivery-limit, broker]
id: E-054
datum: 2026-10-14
status: gilt
quelle:
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:172 (KTD13, Broker-Zähler zentral aus Kopien entfernen)
  - Commit b69dd89 (U16, FS-10) und Review 7a50258 (Grenztest Broker-DLX-Kopie)
  - src/swarmq/headers.py:132-139 (BROKER_DELIVERY_COUNTERS)
  - src/swarmq/identity.py:84-100 (copy_headers)
tests:
  - tests/unit/worker/test_befund_fs10_dlq_copy_counters.py::test_fs10_delivery_limit_dlq_copy_drops_broker_counters
  - tests/unit/worker/test_befund_fs10_dlq_copy_counters.py::test_fs10_retry_exhausted_dlq_copy_drops_broker_counters
  - tests/unit/worker/test_befund_fs10_dlq_copy_counters.py::test_fs10_expired_dlq_copy_drops_broker_counters
  - tests/unit/worker/test_befund_fs10_dlq_copy_counters.py::test_fs10_rejected_dlq_copy_drops_broker_counters
  - tests/integration/broker/test_befund_fs10_dlq_replay_counters.py::test_fs10_shovel_replay_of_a_delivery_limit_dead_letter_runs_the_task
---

**Entscheidung.** Keine Kopie, die swarmq selbst publiziert (Retry, Zurückstellung, DLQ-Stempel, `swarmq dlq retry`), trägt `x-delivery-count` oder `x-acquired-count` weiter. `identity.copy_headers` entfernt sie zentral; eine Kopie ist eine neue Nachricht mit eigenem Zustellbudget. Replay-ID und Generation der Ausführungsidentität bleiben bitgleich (E-009). Kopien, die der Broker selbst per DLX erzeugt, behalten den Zähler; das ist eine Grenze, die swarmq nicht beeinflusst.

**Begründung.** RabbitMQ 4 behält einen vom Publisher gesetzten `x-delivery-count` bei der ersten Zustellung bei. Mitkopiert hielt das Delivery-Count-Gate die Kopie für hängend, und ein Replay per UI, Shovel oder swarmv landete sofort wieder in der DLQ (FS-10, BZ-08 nur teilweise behoben).

**Bewusste Folgen.** Ein DLQ-Eintrag zeigt die Zustellzahl vor dem Dead-Lettern nicht mehr; das ist kein Fehler, die Diagnose geht über die Logs des Workers. Ein Replay einer Broker-DLX-Kopie (Delivery-Limit des Brokers, nicht der Worker-Pfad) geht weiter sofort wieder in die DLQ; der Nachweis `tests/integration/broker/test_befund_fs10_dlq_replay_counters.py::test_fs10_shovel_replay_of_a_broker_dlx_copy_with_counters` bleibt bewusst xfail (Grenze). Weil Retry-Kopien keine Broker-Zähler tragen, gelten sie im Schedfire-Gate nicht als Neuzustellung (E-046, Nachtrag Retry-Kopie).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
