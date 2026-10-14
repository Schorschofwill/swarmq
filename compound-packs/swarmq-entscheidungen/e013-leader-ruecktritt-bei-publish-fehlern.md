---
title: Der Leader tritt nach wiederholten Broker-Verbindungsfehlern zurück und kühlt 2 × Lock-TTL ab; queue-bezogene Publish-Fehler zählen nicht
applies_when:
  - der Leader kann Cron-Fires wegen Broker-Problemen nicht publizieren
  - Änderungen an scheduler/stepdown.py, _NOT_COUNTED oder scheduler_publish_stepdown_s
  - eine Fehlersuche meldet, dass der Leader bei Broker-Alarm, unroutbarem Ziel oder ausbleibenden Confirms nicht zurücktritt
  - eine Fehlersuche meldet, dass ein zurückgetretener Worker sich 2 × Lock-TTL nicht bewirbt
tags: [scheduler, leader, ruecktritt, broker, bz-10]
id: E-013
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:130 (Entscheidungen, BZ-10: Leader tritt nach wiederholten Publish-Fehlern zurück)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:570 (MR 15, Rücktritt mit Abkühlzeit 2 × Lock-TTL)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:74 (Nachtrag Rücktritt nur bei Verbindungsfehlern)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:75 (Nachtrag PublishIndeterminate zählt nicht, Georg 2026-10-11)
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:184 (KTD18, Nachtrag nicht gesendet nach Endfrist zählt, 2026-10-14)
tests:
  - tests/unit/scheduler/test_stepdown.py::test_leader_steps_down_after_repeated_publish_failures
  - tests/unit/scheduler/test_stepdown.py::test_no_candidacy_during_the_cooldown
  - tests/unit/scheduler/test_stepdown.py::test_errors_that_hit_every_candidate_are_not_counted
  - tests/unit/scheduler/test_stepdown.py::test_window_zero_disables_the_stepdown
  - tests/unit/scheduler/test_stepdown_series.py::test_a_gap_longer_than_the_window_starts_a_new_series
  - tests/unit/scheduler/test_stepdown_series.py::test_a_new_term_starts_a_new_series
  - tests/unit/scheduler/test_stepdown_series.py::test_a_single_cron_without_confirms_does_not_step_down
  - tests/unit/test_config_stepdown.py::test_default_is_the_scheduler_lock_ttl
---

**Entscheidung.** Der Leader tritt zurück, wenn mindestens 2 Publish-Fehler über mindestens `scheduler_publish_stepdown_s` (Default `scheduler_lock_ttl`, 0 schaltet ab) auftreten. Danach gibt er den Lock ohne Handover-Marker frei und bewirbt sich 2 × Lock-TTL nicht. Gezählt werden nur `BrokerConnectionError`, nicht `UnroutableMessage`, `MessageHeadersTooLarge`, `BrokerPublishBlocked`, `PublishRejected` und `PublishIndeterminate`. Die Serie beginnt neu nach einer Lücke länger als die Frist und mit jeder Amtszeit. `ACCESS_REFUSED` beim Publish zählt.

**Begründung.** Ein Leader mit defektem Broker hielt alle Crons aller Apps an (BZ-10). Fehler, die alle Kandidaten gleich treffen (Alarm) oder an einer Queue hängen, würden die Führung nur rotieren lassen; ein einzelner Cron ohne Confirms soll nicht die Führung kosten.

**Bewusste Folgen.** Ein toter TCP ohne RST zählt erst, wenn der Heartbeat den Verlust erkennt. Solange Apps < 0.12 auf dem Valkey laufen, empfiehlt die Doku `SCHEDULER_PUBLISH_STEPDOWN_S=0`, sonst wandert die Führung zu einer 0.11-App.

**Nachtrag 2026-10-14 (FS-02a, KTD18).** Ein Publish nach verstrichener Shutdown-Publish-Endfrist geht nicht raus und endet als einfacher `BrokerConnectionError` („not sent“), nicht mehr als `PublishIndeterminate`. Er zählt deshalb für den Rücktritt; eine eigene, nicht gezählte Unterklasse gibt es bewusst nicht. Ein Leader im Shutdown, der zwischen `drain_until` und `scheduler.stop()` feuert, zählt also. Ein Rücktritt braucht trotzdem ≥ 2 Fehler über ≥ `scheduler_publish_stepdown_s`; im Shutdown-Fenster ist das höchstens kosmetisch (FAILOVER- statt LEADER-Signal). Festgehalten in `tests/unit/scheduler/test_fire_after_publish_deadline.py` (ohne `entscheidung`-Marker).

**Überdenken nur mit neuem Argument.** Offen (Gegenprüfung 2026-10-13, nur Code-Analyse): Ein echter 403 beim Publish kommt als Channel-Close nach dem Senden und wird über `_raise_if_sent` (`broker/rabbitmq/delivery.py:82-91`) vermutlich zu `PublishIndeterminate` – dann zählt `ACCESS_REFUSED` entgegen dem Nachtrag nicht. Die Tests prüfen nur einen gemockten `BrokerConnectionError("ACCESS_REFUSED")`.
