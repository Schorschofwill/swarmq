---
title: Ein unreapbares Rechenkind sperrt seinen Slot dauerhaft, die Nachricht wird trotzdem bewertet
applies_when:
  - Änderungen am SlotPool (worker/compute/pool.py), am Kill-Ablauf oder an der Slot-Vergabe
  - eine Fehlersuche meldet, dass ein gesperrter Slot nie wieder freigegeben wird
  - ein Plan will gesperrte Rechen-Slots nach einer Abkühlzeit wieder öffnen
tags: [cpu-modus, compute, pool, kill, slot]
id: E-035
datum: 2026-09-03
status: gilt
quelle:
  - docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:259 (KTD3, „Bleibt das Kind unreapbar (D-State), bleibt der Slot blockiert“)
  - docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:388 (Risikotabelle)
  - src/swarmq/worker/compute/pool.py:626-634 (Hinweis „slot stays blocked“, Log compute_child_unreaped)
  - 'docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:180-183 (KTD17, FS-23: gesperrten Index verwerfen)'
  - Commit 13472d9 (U19, FS-23) und Review 30e536c
tests:
  - tests/unit/worker/compute/test_pool.py::test_immortal_child_blocks_the_slot
  - tests/unit/worker/compute/test_befund_fs23_blocked_slot.py::test_a_slot_blocked_by_background_recycling_is_never_handed_out_again
---

**Entscheidung.** Überlebt ein Rechenkind auch SIGKILL und die Sentinel-Wartezeit (etwa im D-State), bleibt sein Slot für die Lebensdauer des Pools gesperrt. Der Pool loggt `compute_child_unreaped`. Die Nachricht wird trotzdem bewertet, also nicht durch den hängenden Slot blockiert. Sind alle Slots gesperrt, gilt der Pool als ungesund.

**Begründung.** Ein Slot mit einem noch lebenden, nicht reapbaren Prozess darf keinen neuen Job bekommen, weil Speicher und CPU des alten Prozesses noch belegt sein können. Der D-State ist selten; die Erholung übernimmt der Neustart des Worker-Prozesses.

**Bewusste Folgen.** Dass eine Sperre nie aufgehoben wird, ist kein Fehler. BZ-27 („Abkühlzeit statt Dauersperre“) betrifft die Supervisor-Slots im `process_manager`, nicht den Rechen-Pool.

**Nachtrag 2026-10-14 (FS-23, U19).** Behoben. `SlotPool.run` verwirft einen Index, dessen Slot nach `_free.get()` als gesperrt gilt, statt ihn zurückzulegen; die Sperre selbst bleibt. Damit wird auch ein Slot, den ein Kill im Hintergrund (Recycling, `aclose`) erst nach der Rückgabe des Index gesperrt hat, nicht mehr vergeben. Sind alle Slots gesperrt, endet `run` mit `SlotUnavailable`, auch ein schon wartender Aufruf. Festgeschrieben in `tests/unit/worker/compute/test_befund_fs23_blocked_slot.py`.

**Überdenken nur mit neuem Argument.** Bis U19 blieb ein im Hintergrund gesperrter Index in der Free-Queue, und der gesperrte Slot wurde weiter vergeben (FS-23). Das war ein Fehler gegen diese Entscheidung, keine offene Frage. Keine offenen Einwände bekannt.
