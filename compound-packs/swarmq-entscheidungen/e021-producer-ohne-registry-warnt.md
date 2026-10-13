---
title: Ein Producer ohne Registry warnt einmal je Task-Name, dass Task-Defaults nicht aufgelöst werden; ohne Abschalter
applies_when:
  - ein Client ohne Registry sendet oder plant Tasks
  - Änderungen an client/schedule_resolve.py
  - eine Fehlersuche meldet, dass ein Producer ohne Registry Dedup und Priorität verliert
  - jemand schlägt einen Abschalter für client_task_defaults_unresolved vor
tags: [client, registry, logging, bz-24]
id: E-021
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:138 (Entscheidungen, BZ-24)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:285 (MR 2, BZ-24)
tests:
  - tests/unit/client/test_befund_bz24_registry_less_producer.py::test_registry_less_schedule_warns_that_task_defaults_are_unresolved
  - tests/unit/client/test_task_defaults_unresolved.py::test_each_task_name_and_each_client_instance_warns_once
  - tests/unit/client/test_task_defaults_unresolved.py::test_reported_task_names_are_capped_at_1000
  - tests/unit/client/test_task_defaults_unresolved.py::test_client_with_registry_does_not_warn
---

**Entscheidung.** Sendet ein Producer ohne Registry (also ohne TaskInfo), loggt er einmal je Task-Name und Client-Instanz die WARNING `client_task_defaults_unresolved`; die Menge gemeldeter Namen ist auf 1000 gedeckelt. Es gibt keinen Abschalter. Die Doku erklärt die Abhängigkeit von der Registry. Das Verhalten selbst (kein Dedup, keine Priorität aus dem Task) bleibt.

**Begründung.** Der stille Verlust von Dedup und Priorität (BZ-24) soll sichtbar werden, ohne Producer ohne Registry zu verbieten.

**Bewusste Folgen.** Ein Producer ohne Registry verliert weiterhin `unique`/Priorität der Task-Definition; das ist kein Fehler, solange die Warnung kommt. Ein Client mit Registry, dem der Name fehlt, warnt nicht, sondern wirft `TaskNotFoundError`.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
