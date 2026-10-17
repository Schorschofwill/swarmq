---
title: Ein gerenderter Lock-Name, der mit {sched} beginnt oder einem internen Lock-Namen gleicht, geht wie ein Render-Fehler in die DLQ
applies_when:
  - Änderungen an render_lock_name oder LockGate.apply (worker/gates/lock.py) oder an keyspace.lock/scheduler_lock
  - ein neuer interner Lock-Name kommt dazu (dann prüfen, ob getrennte Präfixe nötig werden)
  - eine Fehlersuche meldet, dass ein Nutzer-Lock den Scheduler-Leader blockieren kann
tags: [lock, scheduler, leader, nutzdaten, dlq, pf-14]
id: E-074
datum: 2026-10-17
status: gilt
quelle:
  - Georg 2026-10-17 (Fehlersuche Runde 4, Produktfrage C, PF-14, Option b)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (LÜCKE PF-14, Verdacht R4F-09)
  - src/swarmq/keyspace.py:121-125 (lock, scheduler_lock mit {sched}:leader)
  - src/swarmq/worker/gates/lock.py:229-250 (Leerprüfung, Muster für die Prüfung)
ohne_test: Test folgt mit dem Fix (PF-14, Fix-Plan 0.12.4). Heute prüft swarmq gerenderte Lock-Namen nicht gegen reservierte Namen; ein grüner Test ist erst nach dem Fix möglich.
---

**Entscheidung.** Ein gerenderter Lock-Name, der mit `{sched}` beginnt oder einem internen Lock-Namen gleicht (heute `{sched}:leader`, `keyspace.scheduler_lock`), wird wie ein Render-Fehler behandelt: Dead-Letter mit `x-swarmq-lock-render-error`, kein Requeue (Weg wie F4-10 und die Leerprüfung). Der Guide `locking-and-dedup.md` bekommt den Satz: Lock-Templates beginnen mit einem Literal (`"order:{order_id}"`, nicht `"{order_id}"`). Die Präfixe bleiben gemeinsam (`sq:lock:`); getrennte Präfixe (Option a, BREAKING) erst, wenn weitere interne Lock-Namen dazukommen.

**Begründung.** `lock="{resource}"` mit `resource="{sched}:leader"` aus Nutzdaten ergibt den Key des Scheduler-Leaders; ist er frei, kann für die Laufzeit des Tasks kein Leader gewählt werden (R4F-09). Die Prüfung ist klein, nicht BREAKING und folgt dem Muster der Leerprüfung. E-012 trennt nur Apps, ein Vertrauensmodell für Nutzdaten ist nicht dokumentiert.

**Bewusste Folgen.** Ein Task, dessen Lock-Name aus Nutzdaten einen reservierten Namen ergibt, landet in der DLQ und läuft nicht. Ein Template, das mit einem Literal beginnt, kann keinen reservierten Namen ergeben.

**Überdenken nur mit neuem Argument.** Option a (getrennte Präfixe `sq:lock:u:`) wieder aufnehmen, sobald ein weiterer interner Lock-Name entsteht.
