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
  - Commit 06958c8 (Review U5: Retry-Kopie ohne Schedfire-Defer-Zähler)
  - src/swarmq/worker/retry.py:157-160
tests:
  - tests/e2e/test_schedfire_worker_crash.py::test_sigkill_mid_cron_fire_runs_the_fire_exactly_once_after_the_claim_expired
  - tests/unit/worker/test_retry.py::test_retry_copy_drops_schedfire_defer_count
---

**Entscheidung.** Ein Schedfire (Cron-, Delay- oder ETA-Fire) wird mindestens einmal ausgeführt. Der Fire-Claim schützt nur gegen parallele Ausführung während eines aktiven Laufs. Stürzt der Worker, der den Claim gewonnen hat, vor dem Ack ab, muss die Neuzustellung das Fire nach Ablauf bzw. Übernahme des Claims erneut ausführen, statt es als Duplikat zu bestätigen.

**Begründung.** swarmq ist at-least-once. Ein Claim, der nach einem Absturz jede Neuzustellung als Duplikat verwirft, ist ein Loch mit at-most-once-Semantik über den ganzen Dedup-Horizont (H-2). Ein stiller Fire-Drop gilt in der Lösungsdoku als kritisch.

**Bewusste Folgen.** Ein Fire geht nur im dokumentierten Doppelausfall verloren: Die Freigabe des Claims in Valkey scheitert **und** der Broker nimmt auch die Ersatzkopie nicht an; dann wird die requeuete Nachricht bis zum Ablauf des Claims als Duplikat bestätigt. Das ist kein Fehler.

**Nachtrag 2026-10-14 (FS-03 Teil 1, 0.12.1).** Eine Broker-Neuzustellung (`x-delivery-count`/`x-acquired-count` > 0) oder eine schon zurückgestellte Kopie (`x-swarmq-schedfire-defer-count`) wird bei fremdem Claim per `retry_later` mit wachsender Wartezeit zurückgestellt, bis der Completed-Marker steht (Ack) oder der Claim abläuft (Lauf). Frische Leader-Duplikate werden weiter bestätigt (E-047). Eine Backend-Ausfall-Kopie einer Neuzustellung trägt den Defer-Header (bei der ersten mit 0), damit sie ohne Broker-Zähler als Neuzustellung erkennbar bleibt. Die Zurückstellung ist gedeckelt auf `max(completed_ttl, inflight_ttl + backend_defer_max_delay)` (`completed_ttl` ohne das Alter der Kopie: `scheduler_dedup_ttl` bzw. `max_age + 300`), gemessen an der Summe der Wartezeiten aus dem Defer-Zähler; danach `schedfire_defer_stalled` und Ack. Grenzen: Klassische Queues führen keine Broker-Zähler, dort bleibt die erste Neuzustellung ein Duplikat wie bisher. Ein 0.12.0-Worker bestätigt eine zurückgestellte Kopie bei fremdem Claim weiter (Rolling Deploy: Surge). Den Semaphor-Teil (Teil 2) schließt U4. Festgeschrieben im e2e-Test unter `tests:`.

**Nachtrag 2026-10-14 (Review U5, Commit 06958c8): Retry-Kopie eines Fires.** Die fachliche Retry-Kopie eines gelaufenen Fires (`RetryHandler`, Task scheiterte mit Retry-Budget) verliert `x-swarmq-schedfire-defer-count`, und wie jede swarmq-Kopie die Broker-Zähler (E-054). Sie ist keine Neuzustellung im Sinne des Nachtrags FS-03: Trifft sie einen fremden Claim, wird sie als Duplikat bestätigt statt zurückgestellt. Der Header `x-swarmq-schedfire` bleibt. Grund: Mit dem Defer-Zähler würde die Kopie bei fremdem Claim zurückgestellt, und der Deckel der Zurückstellung startete mit dem alten Stand. Festgeschrieben in `tests/unit/worker/test_retry.py::test_retry_copy_drops_schedfire_defer_count`.

**Überdenken nur mit neuem Argument.** FS-03 (Fehlersuche Runde 2) ist in 0.12.1 behoben: Teil 2 (Abbruch beim Warten auf den Semaphor gibt den Fire-Claim frei) durch U4, Teil 1 (Neuzustellung mit fremdem Claim wird zurückgestellt statt bestätigt) durch U5, siehe Nachtrag. Die frühere Prämisse „the next redelivery re-runs“ im Gate-Docstring und in `test_schedfire.py::test_duplicate_when_inflight_claim_lost` ist ersetzt; `sched_fire_claim.lua` bleibt dabei: eine Broker-Redelivery derselben Kopie trifft einen fremden Claim, das Gate stellt sie jetzt zurück statt sie zu bestätigen (Schutz vor Parallelausführung bei Kanalverlust bleibt). Grenzen: klassische Queues (keine Broker-Zähler, die erste Neuzustellung bleibt ein Duplikat), Mischbetrieb mit 0.12.0-Workern (bestätigen die Kopie weiter), der Deckel (eine Kopie, deren Claim ein lebender Lauf länger als `max(completed_ttl, inflight_ttl + backend_defer_max_delay)` hält, wird mit `schedfire_defer_stalled` bestätigt; stirbt dieser Lauf danach, ist das Fire verloren) und der dokumentierte Doppelausfall oben. Läuft ein Task ohne `hard_timeout` länger als `scheduler_inflight_ttl`, kann eine zurückgestellte Kopie den abgelaufenen Claim gewinnen; den Parallellauf verhindert dann nur der Ausführungsschutz (`execution_guard`, Default an) -- und der nur für Kopien derselben Nachricht (gleiche `task_id`, dieselbe Ausführungsidentität). Restfall: Hält ein verspätetes Leader-Doppel (E-047, eigene `task_id`) den Claim und läuft ohne `hard_timeout` länger als `scheduler_inflight_ttl`, kann eine zurückgestellte Neuzustellung nach Claim-Ablauf daneben laufen. Das setzt Absturz, verspätetes Doppel und Langläufer ohne `hard_timeout` zugleich voraus. Empfehlung: Cron-Tasks mit `hard_timeout` (der Claim lebt dann `hard_timeout + 30` s).
