---
title: Scheitert der Lock-Renew wiederholt, tritt der Leader zurück, bevor seine Lock-TTL sicher abgelaufen ist
applies_when:
  - Änderungen am Election-Loop, am Renew des Scheduler-Locks oder an Backend-Zeitlimits für den Renew
  - eine Fehlersuche meldet, dass ein Leader bei Valkey-Ausfall weiterfeuert, während ein Standby übernimmt
  - ein Plan ändert Heartbeat-Intervall oder Lock-TTL
tags: [scheduler, leader, renew, split-brain, zusage]
id: E-048
datum: 2026-06-03
status: gilt
quelle:
  - docs/plans/2026-06-03-001-fix-hardening-round-2-plan.md:96-102 (P1-5, Step-down nach N = max(1, lock_ttl//heartbeat - 1) Fehlern, „bevor die Lock-TTL sicher abgelaufen ist“)
  - src/swarmq/scheduler/loop.py:187-194
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:154-158 (KTD7, Nachtrag Wiederwahl mit eigenem Token, FS-06b, U11)
tests:
  - tests/unit/scheduler/test_loop.py::TestElectionLoop::test_renew_exception_steps_down_after_threshold
  - tests/unit/scheduler/test_loop.py::TestElectionLoop::test_renew_failure_limit_derived_from_ttl_and_heartbeat
  - tests/unit/scheduler/test_loop.py::TestStandbyWithOwnTokenInLock::test_standby_reacquires_its_own_lock_at_once
  - tests/unit/scheduler/test_loop.py::TestStandbyWithOwnTokenInLock::test_reacquiring_its_own_lock_extends_it_at_once
  - tests/unit/scheduler/test_loop.py::TestStandbyWithOwnTokenInLock::test_failed_renew_after_reacquiring_steps_down_on_the_next_failure
---

**Entscheidung.** Ein Renew, der `False` liefert, lässt den Leader sofort in den Standby gehen. Ein Renew, der wirft (Valkey nicht erreichbar), zählt als Fehler; nach `N = max(1, lock_ttl // heartbeat - 1)` aufeinanderfolgenden Fehlern tritt der Leader proaktiv zurück (`SCHEDULER_STANDBY`), einen Heartbeat bevor die Lock-TTL abgelaufen sein kann. Ein Erfolg setzt den Zähler zurück.

**Begründung.** Ohne Rücktritt feuerte der alte Leader während des ganzen Ausfalls weiter, während ein Standby nach TTL-Ablauf übernahm: Split-Brain für die Dauer des Ausfalls.

**Bewusste Folgen.** Ein Leader, der bei kurzer Valkey-Störung zurücktritt und neu gewählt werden muss, ist kein Fehler.

**Nachtrag 2026-10-14 (FS-06b, U11).** `lock_acquire.lua` wertet seit FS-06b den eigenen Token im Key als erworben (Mutex: `GET == owner`, Semaphor: lebender eigener Slot), ohne die TTL anzufassen. Damit übersteht ein Erwerb die Wiederholung, die valkey-py nach verlorener Antwort sendet. Für die Leader-Wahl mit ihrem stabilen `owner_token` je `SchedulerLoop` heißt das: Trägt der Key nach einem Rücktritt ohne Löschen (Renew-Fehler nach dieser Regel) noch den eigenen Token, wird der Standby beim nächsten Wahl-Takt sofort wieder Leader, statt den TTL-Ablauf abzuwarten; direkt danach verlängert er wie bei jeder Übernahme (BZ-07) auf die volle TTL. Wirft diese Verlängerung, bleibt er Leader (BZ-07), aber mit verbrauchtem Renew-Budget: Der nächste Renew-Fehler lässt ihn sofort zurücktreten, statt `N` weitere abzuwarten, denn der Key kann nur noch die Rest-TTL leben, die der Selbsttreffer stehen lässt. Das gilt für jede Übernahme mit gescheiterter Verlängerung, auch nach frischem Erwerb. Das ist gewollt und kein Split-Brain, denn solange der Key den eigenen Token trägt, hält ihn kein anderer (E-047 unberührt). Der Rücktritt nach Publish-Fehlern (E-013) löscht den Key ohnehin, und seine Abkühlzeit verhindert die Kandidatur. Festgehalten in `tests/unit/scheduler/test_loop.py::TestStandbyWithOwnTokenInLock` (Wiederwahl, sofortige Verlängerung, Rücktritt beim nächsten Fehler).

**Überdenken nur mit neuem Argument.** Heute in der Zeitgrenze verletzt: FS-31 (Fehlersuche Runde 2, P3). Der Renew läuft ohne `backend_op_timeout`; bei vollständig hängendem Valkey dauert ein einzelner Renew durch Socket-Timeout und valkey-py-Retries bis etwa 65 s, länger als die TTL. Die Wirkung fällt unter das akzeptierte Risiko E-047. Der Rücktritt nach Publish-Fehlern und die Abkühlzeit (BZ-10) sind gesondert geregelt.
