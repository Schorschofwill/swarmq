---
title: Neue Quorum-Queues werden at-least-once mit reject-publish angelegt, bestehende behalten ihre Argumente (Modus auto)
applies_when:
  - Queues werden deklariert (neu oder Bestand aus 0.11)
  - Änderungen an SWARMQ_QUEUE_DEAD_LETTER_STRATEGY oder declarations.py
  - eine Fehlersuche meldet, dass eine Bestandsqueue at-most-once bleibt (Warnung queue_dead_letter_at_most_once)
  - eine Fehlersuche meldet Publish-Fehler bei voller Queue (reject-publish mit max-length-Policy)
tags: [broker, quorum, dead-letter, at-least-once, bz-19]
id: E-017
datum: 2026-10-10
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:135 (Entscheidungen, BZ-19)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:52 (Abschnitt 1, BZ-19 auto)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:68 (Nachtrag SWARMQ_QUEUE_DEAD_LETTER_STRATEGY, Georg 2026-10-10)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:710 (Abschnitt 7, BZ-19)
tests:
  - tests/unit/broker/test_befund_bz19_dead_letter_strategy.py::test_quorum_dead_lettering_queues_request_at_least_once
  - tests/integration/broker/test_declare_chain.py::test_existing_0_11_queue_keeps_its_arguments_and_warns_once
  - tests/integration/broker/test_declare_chain.py::test_new_queue_is_created_at_least_once
  - tests/unit/broker/test_declare_chain.py::test_strict_mode_refuses_an_existing_at_most_once_queue
  - tests/unit/broker/test_declare_chain.py::test_legacy_mode_creates_queues_with_0_11_arguments
  - tests/unit/broker/test_declare_chain.py::test_legacy_mode_does_not_warn_about_at_most_once_queues
  - tests/unit/test_config_queue_dead_letter_strategy.py::test_default_is_auto
---

**Entscheidung.** `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=auto` (Default): Neue Queues bekommen `x-dead-letter-strategy=at-least-once` und `x-overflow=reject-publish` (nicht `.XQ`); Bestandsqueues behalten ihre Argumente, dazu höchstens eine WARNING `queue_dead_letter_at_most_once` je Prozess. `at-least-once`: wie `auto`, aber eine Bestandsqueue ohne at-least-once führt zu `QueueArgumentsMismatch` (Exit 78). `legacy`: Argumente aus 0.11, keine Warnung.

**Begründung.** Dead-Lettering in Quorum-Queues ist standardmäßig at-most-once; bei Leader-Wechsel gingen DQ- und XQ-Übergänge verloren (BZ-19). Bestandsqueues umzustellen hieße löschen und neu anlegen; das bleibt eine bewusste Betreiberentscheidung (Policy oder strenger Modus).

**Bewusste Folgen.** Bestandsqueues bleiben at-most-once, bis der Betreiber eine Policy setzt. Duplikate laufen doppelt, wenn kein Ausführungsschutz greift. Rollback nur auf ≥ 0.11.1. `reject-publish` zusammen mit einer `max-length`-Policy führt zu Publish-Fehlern statt drop-head.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
