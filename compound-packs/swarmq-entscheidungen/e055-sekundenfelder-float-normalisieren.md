---
title: Ganzzahlige Sekundenfelder (result_ttl, unique_for) nehmen float an; ganzzahlige Werte werden still zu int, echte Brüche aufgerundet
applies_when:
  - Änderungen an TaskInfo.__post_init__, an der Validierung von SwarmQConfig oder am Parameter unique_for von Client.schedule/schedule_many
  - eine Fehlersuche meldet, dass result_ttl=1.5 oder unique_for=0.2 still auf die nächste ganze Sekunde aufgerundet wird
  - ein Plan will float in diesen Feldern stattdessen mit ValueError oder ConfigurationError ablehnen
tags: [konfiguration, taskinfo, unique, result-ttl, validierung, f3-31]
id: E-055
datum: 2026-10-15
status: gilt
quelle:
  - Georg 2026-10-15 (Fehlersuche Runde 3, F3-31)
  - docs/befunde/fehlersuche-runde-3-2026-10.md (LÜCKE F3-31, Option a; Bezug F3-07)
tests:
  - tests/unit/test_befund_f3_31_float_ttl_normalized.py::test_task_info_result_ttl_is_normalized_to_int
  - tests/unit/test_befund_f3_31_float_ttl_normalized.py::test_task_info_unique_for_is_normalized_to_int
  - tests/unit/test_befund_f3_31_float_ttl_normalized.py::test_config_result_ttl_is_normalized_to_int
  - tests/unit/client/test_schedule_resolve.py::test_schedule_float_unique_for_claims_whole_seconds
  - tests/unit/client/test_schedule_resolve.py::test_deferred_float_unique_for_claims_whole_seconds_plus_delay
  - tests/unit/client/test_schedule_resolve.py::test_schedule_many_float_unique_for_claims_whole_seconds
---

**Entscheidung.** `result_ttl` und `unique_for` in `TaskInfo` sowie `result_ttl` in `SwarmQConfig` (ein Feld `unique_for` hat die Konfiguration nicht) nehmen auch `float` an, ebenso der Aufrufparameter `unique_for` von `schedule()`/`schedule_many()` (F3-07). Umgesetzt in 0.12.3 (F3-31 behoben, `swarmq.seconds`). Ein ganzzahliger `float` wie `timedelta(hours=1).total_seconds() == 3600.0` wird still zu `int`. Ein echter Bruch wird auf die nächste ganze Sekunde aufgerundet (`ceil`). Die übrigen Bereichsregeln bleiben: `unique_for=0` (auch `0.0`) ist ein Fehler (E-056), `None` schaltet ab (E-033).

**Begründung.** `timedelta(...).total_seconds()` liefert immer `float`. Ungeschützt ließ ein `float` valkey-py beim `EX`/`PX` mit `DataError` scheitern; jede Nachricht blieb unbestätigt, der Prefetch lief voll, nach `consumer_timeout` drohte Doppelausführung, und ein Cron feuerte nie (F3-31). Die Typannotation `int` allein schützt zur Laufzeit nicht. Aufrunden stellt sicher, dass eine TTL oder Sperre nie kürzer wird als verlangt.

**Bewusste Folgen.** `result_ttl=0.5` bewahrt das Ergebnis eine Sekunde, `unique_for=0.2` sperrt eine Sekunde. Ein `float` erzeugt weder Fehler noch Warnung. Das ist kein Fehler.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt. Verworfen: `isinstance(…, int)` mit `ValueError` (Option b) und „nichts tun“ (Option c).
