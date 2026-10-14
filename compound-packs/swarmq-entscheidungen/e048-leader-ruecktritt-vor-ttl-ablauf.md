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
tests:
  - tests/unit/scheduler/test_loop.py::TestElectionLoop::test_renew_exception_steps_down_after_threshold
  - tests/unit/scheduler/test_loop.py::TestElectionLoop::test_renew_failure_limit_derived_from_ttl_and_heartbeat
---

**Entscheidung.** Ein Renew, der `False` liefert, lässt den Leader sofort in den Standby gehen. Ein Renew, der wirft (Valkey nicht erreichbar), zählt als Fehler; nach `N = max(1, lock_ttl // heartbeat - 1)` aufeinanderfolgenden Fehlern tritt der Leader proaktiv zurück (`SCHEDULER_STANDBY`), einen Heartbeat bevor die Lock-TTL abgelaufen sein kann. Ein Erfolg setzt den Zähler zurück.

**Begründung.** Ohne Rücktritt feuerte der alte Leader während des ganzen Ausfalls weiter, während ein Standby nach TTL-Ablauf übernahm: Split-Brain für die Dauer des Ausfalls.

**Bewusste Folgen.** Ein Leader, der bei kurzer Valkey-Störung zurücktritt und neu gewählt werden muss, ist kein Fehler.

**Überdenken nur mit neuem Argument.** Heute in der Zeitgrenze verletzt: FS-31 (Fehlersuche Runde 2, P3). Der Renew läuft ohne `backend_op_timeout`; bei vollständig hängendem Valkey dauert ein einzelner Renew durch Socket-Timeout und valkey-py-Retries bis etwa 65 s, länger als die TTL. Die Wirkung fällt unter das akzeptierte Risiko E-047. Der Rücktritt nach Publish-Fehlern und die Abkühlzeit (BZ-10) sind gesondert geregelt.
