---
title: delay und eta sind endlich und höchstens MAX_DEADLINE_S voraus; sonst ValueError vor jeder Nebenwirkung
applies_when:
  - Änderungen an Client.schedule (Parameter delay oder eta), an _schedule_deferred oder an swarmq schedule add --at
  - ein neuer öffentlicher Weg, eine Verzögerung oder einen Fire-Zeitpunkt zu setzen, kommt dazu
  - eine Fehlersuche meldet, dass schedule(delay=nan), delay=inf, ein eta mehr als zehn Jahre voraus oder ein solches --at abgelehnt wird
  - Änderungen an MAX_DEADLINE_S (E-063)
tags: [client, schedule, delay, eta, cli, validierung, fristen]
id: E-080
datum: 2026-10-19
status: gilt
quelle:
  - 'Georg 2026-10-19 (Gegenprüfung 0.13.1, Punkt B: delay und eta endlich und begrenzt)'
  - docs/befunde/fehlersuche-runde-4-2026-10.md (Deferred und Verdachte, delay/eta)
  - src/swarmq/client/schedule.py (Client.schedule, _schedule_deferred)
  - src/swarmq/cli/schedule_cmd.py (_resolve_fire_time)
tests:
  - tests/unit/client/test_schedule_delay_eta_bounds.py::test_nan_delay_is_rejected_before_any_side_effect
  - tests/unit/client/test_schedule_delay_eta_bounds.py::test_delay_outside_the_bound_is_rejected_before_any_side_effect
  - tests/unit/client/test_schedule_delay_eta_bounds.py::test_delay_at_the_bound_is_stored
  - tests/unit/client/test_schedule_delay_eta_bounds.py::test_eta_beyond_the_bound_is_rejected_before_any_side_effect
  - tests/unit/client/test_schedule_delay_eta_bounds.py::test_eta_just_inside_the_bound_is_stored
  - tests/unit/client/test_schedule_delay_eta_bounds.py::test_naive_eta_is_read_as_local_time_and_bounded_alike
  - tests/unit/client/test_schedule_delay_eta_bounds.py::test_naive_datetime_max_eta_names_the_field
  - tests/unit/test_cli.py::TestScheduleHandlerValidation::test_schedule_add_rejects_at_beyond_the_deadline_bound
---

**Entscheidung.** `schedule(delay=…)` verlangt ein endliches `delay` von höchstens `MAX_DEADLINE_S` (315 360 000 s, zehn Jahre, E-063); `delay < 0` bleibt wie bisher ein Fehler („delay must be >= 0“). `schedule(eta=…)` verlangt ein `eta` höchstens `MAX_DEADLINE_S` nach jetzt; ein `eta` in der Vergangenheit bleibt mit 1 s Toleranz ein Fehler. Verstöße ergeben `ValueError` mit dem Namen `delay` bzw. `eta`, und zwar vor jeder Nebenwirkung: vor `pre_enqueue`, Claim, Publish und Speichern (wie `unique_for`, E-056). Der genaue Grenzwert ist erlaubt. Ein naives `eta` liest der Client wie bisher als Ortszeit (`eta.timestamp()`); die Grenze gilt auf demselben Zeitstempel. Ein als Ortszeit nicht darstellbares naives `eta` (`datetime.max`) ergibt denselben `ValueError` statt des rohen Jahresfehlers. `swarmq schedule add --at` folgt derselben Grenze (`ConfigurationError`, Exit 1, vor dem Aufbau des Backends; ohne Zone als UTC gelesen wie bisher).

**Begründung.** Bis 0.13.0 veröffentlichte `delay=nan` still sofort, weil `nan < 0` und `nan > 0` beide falsch sind; `delay=inf` warf beim Umrechnen in `fire_ts_ms` einen rohen `OverflowError`; ein fernes `eta` (oder großes `delay`) ließ sich speichern, danach scheiterte `swarmq schedule list` an `datetime.fromtimestamp`. Keiner dieser Werte tat, was der Aufrufer meinte. Die Grenze ist dieselbe wie für alle Fristen (E-063), damit es nur eine Zahl gibt.

**Ort.** Eigene Regel statt Nachtrag: E-063 regelt Fristen und TTLs (Werte, die als `PX`/`EX` oder Expiration enden), E-056 die Form von `unique_for`. `delay` und `eta` sind Fire-Zeitpunkte des Schedule-Stores und haben mit `eta` einen eigenen, von „jetzt“ abhängigen Fall; die Regel übernimmt die Grenze von E-063 und das Muster „Fehler vor jeder Nebenwirkung“ von E-056.

**Geprüfte Eingänge.** `Client.schedule` (`delay`, `eta`; über `SwarmQ` dieselbe Methode) und `swarmq schedule add --at`. `schedule_many()` nimmt weder `delay` noch `eta`, Workflows haben keine Verzögerung, die CLI hat kein `--delay`. Nicht von dieser Regel erfasst: `Retry(delay=…)` (E-060: endlich, negativ wird 0; eine Obergrenze ist dort nicht festgelegt) und der Low-Level-Vertrag `ScheduleStore.add_schedule` mit fertigem `fire_ts_ms`.

**Bewusste Folgen.** `delay=float("inf")` als „nie“ wird abgelehnt; einen Task, der nie laufen soll, plant man nicht ein. Ein `eta` genau zehn Jahre voraus kann je nach Laufzeit zwischen Prüfung und Umrechnung um Millisekunden über der Grenze liegen und abgelehnt werden; das ist kein Fehler.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
