---
title: Ein Worker-Slot wird nach erschöpftem Crash- oder Hang-Budget für 3600 s gesperrt, nicht dauerhaft
applies_when:
  - ein Worker-Prozess stürzt wiederholt ab oder hängt wiederholt
  - Änderungen an process_manager/restart_budget.py, SlotTable oder SupervisorChannel.locked_slots
  - eine Fehlersuche meldet, dass ein gesperrter Slot nach einer Stunde wieder befüllt wird
tags: [supervisor, slot, crash, hang, bz-27]
id: E-024
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:141 (Entscheidungen, BZ-27)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:597 (MR 17, BZ-27)
tests:
  - tests/unit/process_manager/test_befund_bz27_slot_lock.py::test_locked_slot_is_refilled_after_the_restart_window
  - tests/unit/process_manager/test_slot_cooldown.py::test_crash_budget_locks_for_the_cooldown
  - tests/unit/process_manager/test_slot_cooldown.py::test_hang_budget_locks_for_the_cooldown_and_the_predecessor_keeps_the_slot
  - tests/unit/process_manager/test_slot_cooldown.py::test_unlock_and_lock_events_name_the_cooldown
  - tests/unit/process_manager/test_slot_cooldown.py::test_channels_report_the_locked_slots
  - tests/unit/worker/test_locked_slots_metric.py::test_sampler_reports_the_locked_slots_of_the_supervisor
---

**Entscheidung.** Ist das Crash-Budget (5 Neustarts je Stunde) oder das Hang-Budget eines Slots erschöpft, wird der Slot für eine Abkühlzeit von 3600 s gesperrt statt dauerhaft. Sperre und Freigabe erzeugen ein Event (`worker_restart_limit_exceeded`/`hang_replacement_budget_exhausted`, `worker_slot_unlocked`), die Gauge `swarmq_supervisor_locked_slots` zeigt gesperrte Slots.

**Begründung.** Eine Dauersperre ließ einen Worker nach einem vorübergehenden Problem (Broker-Ausfall, defektes Deploy) dauerhaft mit weniger Slots laufen (BZ-27).

**Bewusste Folgen.** Ein dauerhaft defekter Task bringt den Slot jede Stunde erneut zum Absturz; das ist gewollt und über Event und Gauge sichtbar.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
