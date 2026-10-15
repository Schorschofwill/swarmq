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
tests: []
ohne_test: Test entsteht mit dem Fix in 0.12.3 (F3-01, tests/integration/backend/test_befund_f3_01_schedule_add_lost_reply.py).
---

**Entscheidung.** Findet das `HSETNX` beim Speichern eines Oneshots (Name mit frischer UUID, aus `schedule(delay=…/eta=…)`) einen bestehenden Eintrag, der byte-gleich ist, gilt das als eigener Insert: Der Fire-Index (`ZADD`) wird ergänzt, und `schedule()` gibt die ID zurück. Ein abweichender Eintrag bleibt ein Konflikt. Für `swarmq schedule add` gilt das nicht: Ein bestehender Eintrag mit gleichem Namen meldet weiter „already exists“, auch bei gleichem Inhalt.

**Begründung.** valkey-py wiederholt einen Befehl nach verlorener Antwort; die Wiederholung meldet `False`, und der Oneshot lief nie (F3-01). Bei einem Oneshot kann ein byte-gleicher Eintrag wegen der frischen UUID nur der eigene sein. Bei `schedule add` ist der Name frei gewählt; ein zweiter gleicher Aufruf kann ein echter Zweitaufruf sein.

**Bewusste Folgen.** Geht bei `swarmq schedule add` die Antwort verloren, meldet die CLI „already exists“, obwohl der eigene Insert durchging. Das ist kein Fehler; der Bediener prüft mit `swarmq schedule list`.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt. Verwandte Idempotenz nach Transport-Retry: E-011 und E-022 (Nachtrag FS-06b).
