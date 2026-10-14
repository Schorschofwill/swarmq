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
  - src/swarmq/worker/compute/pool.py:602 (Hinweis „slot stays blocked“, Log compute_child_unreaped)
tests:
  - tests/unit/worker/compute/test_pool.py::test_immortal_child_blocks_the_slot
---

**Entscheidung.** Überlebt ein Rechenkind auch SIGKILL und die Sentinel-Wartezeit (etwa im D-State), bleibt sein Slot für die Lebensdauer des Pools gesperrt. Der Pool loggt `compute_child_unreaped`. Die Nachricht wird trotzdem bewertet, also nicht durch den hängenden Slot blockiert. Sind alle Slots gesperrt, gilt der Pool als ungesund.

**Begründung.** Ein Slot mit einem noch lebenden, nicht reapbaren Prozess darf keinen neuen Job bekommen, weil Speicher und CPU des alten Prozesses noch belegt sein können. Der D-State ist selten; die Erholung übernimmt der Neustart des Worker-Prozesses.

**Bewusste Folgen.** Dass eine Sperre nie aufgehoben wird, ist kein Fehler. BZ-27 („Abkühlzeit statt Dauersperre“) betrifft die Supervisor-Slots im `process_manager`, nicht den Rechen-Pool.

**Überdenken nur mit neuem Argument.** Offen: FS-23. Wird der Kill im Hintergrund ausgelöst (Recycling, `aclose`), nachdem `run()` den Index schon in die Free-Queue zurückgegeben hat, bleibt der Index dort, und der gesperrte Slot wird weiter vergeben. Das widerspricht dieser Entscheidung und ist ein Fehler. Ein Nachweis soll nur „Index nicht mehr frei“ prüfen, nicht die Aufhebung der Sperre.
