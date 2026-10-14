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
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:170 (KTD11, Nachtrag zeitbasierter Auslöser, FS-31, U14)
tests:
  - tests/unit/scheduler/test_loop.py::TestElectionLoop::test_renew_exception_steps_down_after_threshold
  - tests/unit/scheduler/test_loop.py::TestElectionLoop::test_renew_failure_limit_derived_from_ttl_and_heartbeat
  - tests/unit/scheduler/test_loop.py::TestStandbyWithOwnTokenInLock::test_standby_reacquires_its_own_lock_at_once
  - tests/unit/scheduler/test_loop.py::TestStandbyWithOwnTokenInLock::test_reacquiring_its_own_lock_extends_it_at_once
  - tests/unit/scheduler/test_loop.py::TestStandbyWithOwnTokenInLock::test_failed_renew_after_reacquiring_steps_down_on_the_next_failure
  - tests/unit/scheduler/test_loop_renew_deadline.py::test_hanging_renews_after_a_success_step_down_before_the_lock_ttl
  - tests/unit/scheduler/test_loop_renew_deadline.py::test_hanging_renews_after_a_takeover_step_down_before_the_lock_ttl
---

**Entscheidung.** Ein Renew, der `False` liefert, lässt den Leader sofort in den Standby gehen. Ein Renew, der wirft (Valkey nicht erreichbar), zählt als Fehler; nach `N = max(1, lock_ttl // heartbeat - 1)` aufeinanderfolgenden Fehlern tritt der Leader proaktiv zurück (`SCHEDULER_STANDBY`), einen Heartbeat bevor die Lock-TTL abgelaufen sein kann. Ein Erfolg setzt den Zähler zurück.

**Begründung.** Ohne Rücktritt feuerte der alte Leader während des ganzen Ausfalls weiter, während ein Standby nach TTL-Ablauf übernahm: Split-Brain für die Dauer des Ausfalls.

**Bewusste Folgen.** Ein Leader, der bei kurzer Valkey-Störung zurücktritt und neu gewählt werden muss, ist kein Fehler.

**Nachtrag 2026-10-14 (FS-06b, U11).** `lock_acquire.lua` wertet seit FS-06b den eigenen Token im Key als erworben (Mutex: `GET == owner`, Semaphor: lebender eigener Slot), ohne die TTL anzufassen. Damit übersteht ein Erwerb die Wiederholung, die valkey-py nach verlorener Antwort sendet. Für die Leader-Wahl mit ihrem stabilen `owner_token` je `SchedulerLoop` heißt das: Trägt der Key nach einem Rücktritt ohne Löschen (Renew-Fehler nach dieser Regel) noch den eigenen Token, wird der Standby beim nächsten Wahl-Takt sofort wieder Leader, statt den TTL-Ablauf abzuwarten; direkt danach verlängert er wie bei jeder Übernahme (BZ-07) auf die volle TTL. Wirft diese Verlängerung, bleibt er Leader (BZ-07), aber mit verbrauchtem Renew-Budget: Der nächste Renew-Fehler lässt ihn sofort zurücktreten, statt `N` weitere abzuwarten, denn der Key kann nur noch die Rest-TTL leben, die der Selbsttreffer stehen lässt. Das gilt für jede Übernahme mit gescheiterter Verlängerung, auch nach frischem Erwerb. Das ist gewollt und kein Split-Brain, denn solange der Key den eigenen Token trägt, hält ihn kein anderer (E-047 unberührt). Der Rücktritt nach Publish-Fehlern (E-013) löscht den Key ohnehin, und seine Abkühlzeit verhindert die Kandidatur. Festgehalten in `tests/unit/scheduler/test_loop.py::TestStandbyWithOwnTokenInLock` (Wiederwahl, sofortige Verlängerung, Rücktritt beim nächsten Fehler).

**Nachtrag 2026-10-14 (FS-31, KTD11, U14): zeitbasierter Auslöser.** Die Zählregel allein hielt die Zusage nicht ein: Ein Renew lief ohne Zeitlimit (bei hängendem Valkey bis etwa 65 s), und selbst mit `backend_op_timeout` fiel der Rücktritt mit den Defaults (TTL 30 s, Heartbeat 10 s, `op_timeout` 5 s) genau auf den TTL-Ablauf. Deshalb gilt zusätzlich: Der Leader merkt sich den Start des letzten erfolgreichen Renews (bzw. des Erwerbs) auf der Uhr der Event-Loop. Die Zeitgrenze liegt bei `Start + lock_ttl − Marge`, die Marge ist `backend_op_timeout`, höchstens aber `(lock_ttl − heartbeat) / 2` (nie negativ). Jeder Renew, auch der direkt nach der Übernahme (BZ-07), ist auf `min(backend_op_timeout, Restzeit bis zur Grenze)` begrenzt; ein Timeout zählt als Renew-Fehler (`scheduler_renew_error`, `error_type=TimeoutError`). Ist die Grenze nach einem Fehler erreicht, tritt der Leader sofort zurück (`scheduler_renew_deadline_stepdown`, `SCHEDULER_STANDBY`), unabhängig vom Zähler; ohne Löschen des Keys und ohne Abkühlzeit. Die Zählregel und der Nachtrag FS-06b bleiben unverändert: Scheitert die Verlängerung nach der Übernahme, gilt die Grenze ab dem Start des Erwerbs, und der nächste Fehler tritt nach der Zählregel zurück. Die Kappung der Marge weicht bewusst von KTD11 (`ttl − backend_op_timeout`) ab: Ist `backend_op_timeout` größer als die Spanne zwischen Heartbeat und TTL (etwa TTL 1 s bei `op_timeout` 5 s), läge die Grenze sonst vor dem nächsten regulären Renew, und ein gesunder Leader träte bei jedem Takt zurück. Mit den Defaults ist die Marge genau `backend_op_timeout`. Langsame, aber erfolgreiche Renews (unter `op_timeout`) schieben die Grenze und lösen keinen Rücktritt aus. Festgehalten in `tests/unit/scheduler/test_loop_renew_deadline.py` und im Nachweis `tests/unit/scheduler/test_befund_fs31_leader_renew_timeout.py`.

**Überdenken nur mit neuem Argument.** Die Zeitgrenze misst mit der lokalen Uhr ab dem Start des Renew-Aufrufs; ein Valkey, dessen Uhr deutlich schneller läuft, könnte den Key früher verfallen lassen. Ein Abbruch mitten im Renew trennt die Verbindung; der Renew kann serverseitig gelungen sein, dann tritt der Leader zurück, obwohl er den Key hält, und wird über den Selbsttreffer (FS-06b) beim nächsten Takt wieder Leader. Kein Split-Brain. Bis 2026-10-14 in der Zeitgrenze verletzt (FS-31, behoben in U14). Der Rücktritt nach Publish-Fehlern und die Abkühlzeit (BZ-10) sind gesondert geregelt.
