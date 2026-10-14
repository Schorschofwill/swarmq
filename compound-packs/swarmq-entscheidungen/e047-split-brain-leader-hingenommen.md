---
title: Ein kurzzeitig doppelter Scheduler-Leader wird hingenommen; Doppel-Publishes fängt die Fire-Dedup, Commits schützt der Owner-Check
applies_when:
  - Änderungen an Leader-Wahl, Lock-TTL, sched_commit.lua oder an der Fire-Dedup
  - eine Fehlersuche meldet, dass nach GC-Pause, Lock-Verlust oder Netztrennung zwei Leader kurz gleichzeitig feuern
  - ein Plan will Split-Brain durch Fencing oder Konsens vollständig ausschließen
tags: [scheduler, leader, split-brain, dedup, akzeptiertes-risiko]
id: E-047
datum: 2026-04-17
status: gilt
quelle:
  - docs/plans/2026-04-17-002-feat-phase3c-scheduler-plan.md:1388-1391 (Risks & Dependencies, Split-Brain und GC-Pause: gering/niedrig, Abmilderung Consumer-Dedup und Owner-Check)
tests:
  - tests/unit/scheduler/test_loop.py::TestPollLoop::test_commit_owner_mismatch_transitions_to_standby
  - tests/unit/backend/test_valkey_schedules.py::TestScripts::test_commit_fire_rejected_by_owner_check
---

**Entscheidung.** Die Leader-Wahl über ein Valkey-Lock mit TTL (3 × Heartbeat) schließt kurze Doppel-Leader nicht aus (Lock-TTL-Race, GC-Pause über die TTL, Absturz zwischen Publish und Commit). swarmq nimmt das als akzeptiertes Risiko hin. Abgemildert wird es an zwei Stellen: Ein doppelt veröffentlichtes Fire fängt die Fire-Dedup im Worker ab; einen Commit des alten Leaders lehnt der Owner-Check in `sched_commit.lua` ab, worauf dieser in den Standby geht.

**Begründung.** Wahrscheinlichkeit gering, Wirkung niedrig, solange die Dedup im Worker greift. Echtes Fencing bräuchte einen Konsensdienst, den swarmq nicht voraussetzt.

**Bewusste Folgen.** Ein doppelter Publish desselben Fires durch zwei Leader ist für sich kein Fehler. Ein Fehler ist erst, wenn das Duplikat tatsächlich zweimal ausgeführt wird oder ein Commit des alten Leaders den Zustand des neuen überschreibt.

**Nachtrag 2026-10-14 (FS-06b, U11).** Ein Standby, dessen eigener Token noch im Leader-Key steht, wird seit FS-06b sofort wieder Leader (Nachtrag in E-048). Solange der Key den eigenen Token trägt, kann kein anderer ihn halten. Ein Restfenster bleibt, wenn die Verlängerung direkt nach der Wiederwahl wirft: Der Key lebt dann nur noch seine Rest-TTL, und läuft sie ab, kann ein anderer ihn erwerben, während dieser Prozess noch Leader ist. Das Fenster ist auf höchstens einen Heartbeat begrenzt, weil der nächste Renew-Fehler sofort zum Rücktritt führt und ein Renew gegen einen fremden Token `False` liefert (E-048). Die Wirkung fällt unter dieses Risiko.

**Überdenken nur mit neuem Argument.** FS-31 (Leader-Renew ohne `op_timeout`) fällt in der Wirkung unter dieses Risiko, verletzt aber die Zeitgrenze aus E-048.
