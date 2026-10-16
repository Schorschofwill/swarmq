---
title: Das Speichern eines Oneshots aus schedule(delay=/eta=) ist gegen Wiederholung idempotent; swarmq schedule add mit gleichem Namen meldet weiter already exists
applies_when:
  - Änderungen an ScheduleStore.add, Client._schedule_deferred oder swarmq schedule add
  - eine Fehlersuche meldet, dass ein byte-gleicher bestehender Oneshot-Eintrag als eigener Insert gilt
  - eine Fehlersuche meldet, dass swarmq schedule add nach verlorener Antwort „already exists“ meldet
tags: [scheduler, oneshot, valkey, transport-retry, idempotenz, f3-01]
id: E-058
datum: 2026-10-15
status: gilt
quelle:
  - Georg 2026-10-15 (Fehlersuche Runde 3, F3-01)
  - docs/befunde/fehlersuche-runde-3-2026-10.md (F3-01; Vor dem Fix zu entscheiden, Punkt 4)
  - docs/befunde/fehlersuche-runde-2-recherche/2-absturz.md:137 (K-3, alle Lua-Skripte und NX-Befehle gegen Wiederholung prüfen)
tests:
  - tests/integration/backend/test_befund_f3_01_schedule_add_lost_reply.py::test_deferred_oneshot_is_indexed_after_lost_hsetnx_reply
  - tests/integration/backend/test_befund_f3_01_schedule_add_lost_reply.py::test_cli_cron_is_indexed_after_lost_reply_and_reports_already_exists
  - tests/integration/backend/test_befund_f3_01_schedule_add_lost_reply.py::test_identical_oneshot_without_fire_index_gets_indexed
  - tests/unit/scheduler/test_definition.py::TestScheduleStoreAddOneshotIdempotent::test_identical_existing_oneshot_counts_as_own_insert
  - tests/unit/scheduler/test_definition.py::TestScheduleStoreAddOneshotIdempotent::test_identical_existing_oneshot_without_score_gets_its_fire_index
  - tests/unit/scheduler/test_definition.py::TestScheduleStoreAddOneshotIdempotent::test_identical_existing_oneshot_as_str_counts_as_own_insert
  - tests/unit/scheduler/test_definition.py::TestScheduleStoreAddOneshotIdempotent::test_differing_existing_oneshot_is_a_conflict
  - tests/unit/scheduler/test_definition.py::TestScheduleStoreAddOneshotIdempotent::test_vanished_existing_oneshot_is_a_conflict
  - tests/unit/scheduler/test_definition.py::TestScheduleStoreAddOneshotIdempotent::test_cli_at_definition_with_same_name_stays_already_exists
---

**Entscheidung.** Findet das `HSETNX` beim Speichern eines Oneshots (Name mit frischer UUID, aus `schedule(delay=…/eta=…)`) einen bestehenden Eintrag, der byte-gleich ist, gilt das als eigener Insert: Der Fire-Index (`ZADD`) wird ergänzt, und `schedule()` gibt die ID zurück. Ein abweichender Eintrag bleibt ein Konflikt. Für `swarmq schedule add` gilt das nicht: Ein bestehender Eintrag mit gleichem Namen meldet weiter „already exists“, auch bei gleichem Inhalt.

**Begründung.** valkey-py wiederholt einen Befehl nach verlorener Antwort; die Wiederholung meldet `False`, und der Oneshot lief nie (F3-01). Bei einem Oneshot kann ein byte-gleicher Eintrag wegen der frischen UUID nur der eigene sein. Bei `schedule add` ist der Name frei gewählt; ein zweiter gleicher Aufruf kann ein echter Zweitaufruf sein.

**Bewusste Folgen.** Geht bei `swarmq schedule add` die Antwort verloren, meldet die CLI „already exists“, obwohl der eigene Insert durchging. Das ist kein Fehler; der Bediener prüft mit `swarmq schedule list`.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt. Verwandte Idempotenz nach Transport-Retry: E-011 und E-022 (Nachtrag FS-06b).

**Nachtrag 2026-10-15 (F3-01 behoben in 0.12.3, U3, KTD4).** `ScheduleStore.add` schreibt Definition und Fire-Index in einem Schritt (`sched_add.lua`: HSETNX, bei Erfolg ZADD). Nach einer verlorenen Antwort ist der Eintrag damit immer vollständig, auch bei `swarmq schedule add`: Die CLI meldet „already exists“, und der Eintrag feuert trotzdem. Die Idempotenz prüft die Definitionsschicht (`ScheduleStore.add_schedule`): Nur ein Oneshot mit `task_id`, ohne Cron und mit Namen `oneshot:<task_id>` gilt bei byte-gleichem Eintrag als eigener Insert; ein fehlender Index wird bedingt ergänzt (`replan_if_unchanged`, `sched_replan.lua`: nur solange die Definition byte-gleich ist und keinen Score hat; Review U3). Feuert und löscht der Leader den Oneshot zwischen dem Lesen und der Ergänzung, wird er nicht neu angelegt. Ein Eintrag, der zwischen HSETNX und dem Lesen verschwindet, gilt als Konflikt (konservativ; die Regel regelte den Fall nicht). Grenze: Producer und CLI 0.12.0–0.12.2 schreiben weiter in zwei Befehlen (HSETNX, dann ZADD); ihre Einträge können nach verlorener Antwort ohne Fire-Index bleiben (Rolling Deploy). Weder der Leader (die Sync-Heilung gilt nur für `source="static"`) noch ein 0.12.3-Producer ergänzt diesen Index: Jeder `schedule()`-Aufruf erzeugt eine frische `task_id`, der Reparaturzweig greift also nur bei der Wiederholung desselben Aufrufs.
