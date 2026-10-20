---
title: Der Autoskalierer zählt und wählt drainende Worker nicht mit; für MIN, MAX und die Schwelle zählen nur Slots mit einem nicht drainenden Prozess, hochskaliert wird nur in freie Slots
applies_when:
  - Änderungen an process_manager/autoscale.py (tick, scale_down_one) oder an heading_slots/drain_candidates in process_manager/slots.py
  - eine Fehlersuche meldet, dass der Autoskalierer während eines Drains hochskaliert, obwohl MAX Prozesse laufen
  - eine Fehlersuche meldet, dass mit --max-processes während eines Drains mehr als MAX Worker-Prozesse laufen
  - eine Fehlersuche meldet, dass der Autoskalierer bei MAX - 1 aktiven Workern und einem drainenden ohne Spielraum nicht hochskaliert
  - ein Plan will drainende Worker für den Autoskalierer wieder als aktiv zählen (Option (a)) oder den Autoskalierer anders zählen lassen als den Sollwert-Durchlauf
tags: [process-manager, autoscale, drain, slots, setpoint, f4-25, f4-26]
id: E-084
datum: 2026-10-20
status: gilt
quelle:
  - "Orchestrator 2026-10-20 nach Gegenprüfung V-E, Option (b) auf Georgs ‚mache autonom weiter‘ übernommen, Georg kann widersprechen"
  - docs/befunde/fehlersuche-runde-4-2026-10.md (F4-25, F4-26; Verdacht V-E der Test-Runde 2026-10-20)
  - docs/plans/2026-10-20-2100-fix-swarmq-0-14-2-plan.md (KTD1–KTD4, R1–R4)
  - src/swarmq/process_manager/autoscale.py (tick, scale_down_one)
  - src/swarmq/process_manager/slots.py (heading_slots, drain_candidates)
  - src/swarmq/process_manager/setpoint.py (reconcile zählt seit dem Kontrollkanal ebenso)
  - tests/property/stateful/autoscale_machine.py (Invarianten A1–A4, ohne Marker)
tests:
  - tests/unit/process_manager/test_befund_f4_25_26_autoscale_drain.py::test_a_down_tick_during_a_long_drain_leaves_the_draining_worker_alone
  - tests/unit/process_manager/test_befund_f4_25_26_autoscale_drain.py::test_a_draining_slot_does_not_count_towards_the_lower_bound
  - tests/unit/process_manager/test_autoscale.py::test_a_draining_worker_does_not_count_towards_the_scale_up_threshold
  - tests/unit/process_manager/test_autoscale.py::test_without_headroom_a_draining_worker_holds_its_slot_until_it_exits
  - tests/unit/process_manager/test_autoscale.py::test_with_headroom_a_drain_lets_one_worker_start_in_a_headroom_slot
  - tests/unit/process_manager/test_autoscale.py::test_a_slot_in_transition_during_a_drain_counts_once_and_its_replacement_stays
  - tests/unit/process_manager/test_autoscale.py::test_an_exhausted_hang_budget_locks_the_slot_and_its_draining_worker_does_not_count
  - tests/unit/process_manager/test_setpoint.py::TestAutoscalerDuringDrain::test_raised_lower_bound_during_a_drain_is_not_undone_by_the_autoscaler
---

**Entscheidung.** Der Autoskalierer zählt als aktiv nur Slots mit einem Prozess, der nicht zum sanften Ende markiert ist (`expected_shutdown`, `heading_slots`). Diese Zahl gilt für alle drei Prüfungen: die Obergrenze `MAX`, die Schwelle `depth > aktiv × SCALE_UP_THRESHOLD` und die Untergrenze `MIN`. Ein Slot im Übergang (Recycling- oder Hang-Ersatz) zählt über den Ersatz genau einmal. Ein Abbau wählt nur Worker, die nicht drainen, nicht im Übergang liegen und mindestens `SCALE_MIN_AGE_S` alt sind (`drain_candidates`); ein drainender Worker bekommt vom Autoskalierer nie ein zweites SIGTERM. Findet ein Abbau-Takt keinen Kandidaten, ist er nicht verbraucht: Abkühlzeit und Leerlauf bleiben, der nächste Abtast-Takt versucht es erneut. Hochskaliert wird nur in einen freien Slot (`smallest_free()`); ein drainender Prozess hält seinen Slot bis zum Exit.

**Begründung.** Vor 0.14.2 zählte der Autoskalierer belegte Slots (`occupied()`), drainende eingeschlossen, und wählte drainende Worker erneut: ein zweites SIGTERM, ein verbrauchter Takt (F4-25) und nicht drainende Worker unter `MIN` (F4-26). Der Sollwert-Durchlauf des Kontrollkanals (`setpoint.reconcile`) zählte schon so; mit Option (b) zählen beide im selben Supervisor gleich. Option (a), drainende Worker beim Hochskalieren weiter als aktiv zu zählen, hätte zwei Zählweisen nebeneinander gelassen.

**Bewusste Folgen.**
- Während eines Drains skaliert der Autoskalierer früher hoch: Die Schwelle rechnet mit weniger aktiven Workern.
- Ohne `--max-processes` hat die Slot-Tabelle `MAX` Einträge. Belegt ein drainender Prozess einen davon, wächst die Gruppe bis zu seinem Exit nicht über `MAX − drainend` aktive Worker, auch bei Rückstau. Ein Abbau-Drain ist durch `SWARMQ_SHUTDOWN_TIMEOUT` begrenzt; ein Recycling-Drain nicht (E-018).
- Mit `--max-processes H` > `MAX` kann der Autoskalierer während eines Drains in einen Spielraum-Slot hochskalieren. Aktiv bleiben ≤ `MAX`, Prozesse ≤ `H` (plus Slots im Übergang); bis zu `H − MAX` drainende Prozesse laufen zusätzlich, mit ihrem Speicher, und `workers` im Status zählt sie mit. `H` ist unter `--autoscale` nur gegen die Portbereiche geprüft, nicht gegen CPUs (die CPU-Prüfung gilt nur für `--pool cpu`, das `--autoscale` ausschließt).
- Fallen die nicht drainenden Worker ohne Abbau unter `MIN` (gesperrter Slot, sauberes Ende), stellt der Autoskalierer die Untergrenze nicht aktiv wieder her; das tun neuer Rückstau oder der Sollwert-Durchlauf.
- Unter `--autoscale` startet das Nachfüllen des Monitor-Loops nichts; Ersatz nach einem Absturz kommt allein aus `_maybe_restart`.

**Überdenken nur mit neuem Argument.** Eingetragen am 2026-10-20 (0.14.2).
