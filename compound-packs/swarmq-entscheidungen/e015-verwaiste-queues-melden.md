---
title: Queues ohne lebenden Consumer mit Inhalt werden über Valkey-Meldungen erkannt und gemeldet, ohne Management-API
applies_when:
  - eine Queue wurde umbenannt oder verschoben und enthält noch Nachrichten
  - Änderungen an scheduler/orphaned_queues.py, sq:{sched}:report:queues oder swarmq check (queues)
  - jemand schlägt vor, die RabbitMQ-Management-API zu nutzen
  - eine Fehlersuche meldet, dass eine nie von einem Worker gemeldete Queue nicht als verwaist erkannt wird
tags: [broker, queue, verwaist, monitoring, bz-14]
id: E-015
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:132 (Entscheidungen, BZ-14)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:611 (MR 18, BZ-14)
tests:
  - tests/integration/broker/test_befund_bz14_orphaned_queue.py::test_backlog_in_abandoned_queue_is_reported
  - tests/unit/scheduler/test_orphaned_queues.py::test_second_check_in_the_same_state_is_silent
  - tests/unit/scheduler/test_orphaned_queues.py::test_missing_or_empty_queues_unreported_for_7_days_are_forgotten
  - tests/unit/test_cli_env_check_queues.py::test_unreported_queue_with_backlog_and_no_consumer_is_orphaned
---

**Entscheidung.** Worker tragen ihre Queues mit Heartbeat (Valkey-`TIME`) in `sq:{sched}:report:queues` ein. Der Leader prüft per passivem Declare `consumer_count == 0` und `message_count > 0` und meldet Übergänge (Log/Signal `SCHEDULER_QUEUE_ORPHANED`, Gauge). `swarmq check` listet sie. Einträge, die NOT_FOUND oder leer und älter als 7 d sind, werden entfernt. Keine Management-API.

**Begründung.** Umbenannte Queues blieben samt Inhalt ohne Warnung liegen (BZ-14). Die Management-API ist nicht überall erreichbar und bräuchte eigene Zugangsdaten.

**Bewusste Folgen.** Nur Queues, die irgendwann ein Worker gemeldet hat, werden überwacht; fremde oder nie konsumierte Queues nicht. Die Meldung kommt nur beim Übergang, nicht in jedem Takt.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
