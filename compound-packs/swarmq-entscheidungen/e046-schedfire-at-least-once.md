---
title: Ein Scheduler-Fire wird at-least-once ausgeführt; stürzt der Claim-Gewinner vor dem Ack ab, läuft das Fire erneut
applies_when:
  - Änderungen am Schedfire-Gate (worker/gates/schedfire.py), an sched_fire_claim.lua oder an der Pipeline vor dem try/finally
  - eine Fehlersuche meldet einen verlorenen Cron- oder Delay-Fire nach Absturz, SIGKILL oder Shutdown eines Workers
  - ein Plan ändert die Behandlung einer Broker-Neuzustellung mit fremdem oder altem Claim
tags: [scheduler, schedfire, dedup, at-least-once, zusage]
id: E-046
datum: 2026-04-17
status: gilt
quelle:
  - docs/plans/2026-04-17-003-fix-phase3c-review-followup-plan.md:27-32 (H-2, „At-most-once-Hole in einem at-least-once-System“), :103-125 (Unit 1, Ziel at-least-once bei SETNX-winner-crash-before-ack), :414-416 (E2E-Test mit simuliertem Crash Pflicht)
  - docs/solutions/logic-errors/schedfire-retry-dedup-collision-silent-drop-2026-06-02.md (stiller Fire-Drop ist kritisch)
  - website/explanation/delivery-semantics.md:176-183 (Fire-Verlust nur, wenn Freigabe und Broker-Kopie scheitern)
ohne_test: verletzt, Nachweis FS-03 (xfail, Branch test/fehlersuche-runde-2); der in Plan H-2 geforderte Crash-Test fehlt in tests/integration/test_worker_schedfire_dedup.py
---

**Entscheidung.** Ein Schedfire (Cron-, Delay- oder ETA-Fire) wird mindestens einmal ausgeführt. Der Fire-Claim schützt nur gegen parallele Ausführung während eines aktiven Laufs. Stürzt der Worker, der den Claim gewonnen hat, vor dem Ack ab, muss die Neuzustellung das Fire nach Ablauf bzw. Übernahme des Claims erneut ausführen, statt es als Duplikat zu bestätigen.

**Begründung.** swarmq ist at-least-once. Ein Claim, der nach einem Absturz jede Neuzustellung als Duplikat verwirft, ist ein Loch mit at-most-once-Semantik über den ganzen Dedup-Horizont (H-2). Ein stiller Fire-Drop gilt in der Lösungsdoku als kritisch.

**Bewusste Folgen.** Ein Fire geht nur im dokumentierten Doppelausfall verloren: Die Freigabe des Claims in Valkey scheitert **und** der Broker nimmt auch die Ersatzkopie nicht an; dann wird die requeuete Nachricht bis zum Ablauf des Claims als Duplikat bestätigt. Das ist kein Fehler.

**Überdenken nur mit neuem Argument.** Heute verletzt: FS-03 (Fehlersuche Runde 2). Das Gate bestätigt eine Broker-Neuzustellung mit fremdem Claim (Verlust nach SIGKILL/OOM), und das Warten auf den Semaphor liegt außerhalb des try/finally (Verlust bei Shutdown mit vollen Slots). Der Gate-Docstring `schedfire.py:113-116` und `test_schedfire.py::test_duplicate_when_inflight_claim_lost` beruhen auf der falschen Prämisse „the next redelivery re-runs“. Gegenläufig sagt `sched_fire_claim.lua:9-11` (d93d3ab, 0.9.0), eine Broker-Redelivery derselben Kopie bleibe Duplikat; das schützt vor Parallelausführung bei Kanalverlust und muss ein Fix erhalten (Zurückstellen statt Bestätigen).
