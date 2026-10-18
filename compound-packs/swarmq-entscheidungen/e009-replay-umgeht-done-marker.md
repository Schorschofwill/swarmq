---
title: Ein DLQ-Replay ist eine neue Ausführung und umgeht den Done-Marker; eine Neuzustellung bleibt Duplikat
applies_when:
  - eine Nachricht aus der DLQ wird erneut eingespielt (dlq retry, RabbitMQ-UI „Move messages“, Shovel, Broker-DLX)
  - ein Cron-Lauf wird aus der DLQ erneut eingespielt
  - Änderungen an identity.py, execution_guard, schedfire-Gate oder dlq retry
  - eine Fehlersuche meldet, dass ein Replay trotz Done-Marker läuft oder ein Cron-Replay trotz Completed-Marker
  - eine Fehlersuche meldet, dass ein zweites dlq retry derselben XQ-Nachricht verworfen wird
tags: [replay, dlq, execution-guard, identitaet, cron, bz-08]
id: E-009
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:128 (Entscheidungen, BZ-08/BZ-12)
  - 'docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:46 (Abschnitt 1, BZ-08 Cron: Replay eines Cron-Laufs läuft)'
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:461 (MR 12, Ausführungsidentität)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:701 (Abschnitt 7, BZ-08 im Mischbetrieb)
tests:
  - tests/integration/test_befund_bz08_replay_without_id.py::test_replay_without_replay_id_runs_the_task_again
  - tests/unit/test_identity_sequences.py::test_at_least_once_duplicate_is_skipped_also_for_a_replay
  - tests/unit/test_identity_sequences.py::test_publish_indeterminate_double_copy_replays_once
  - tests/unit/worker/gates/test_schedfire_replay.py::test_replay_of_a_completed_fire_runs_under_its_own_key
  - tests/unit/worker/gates/test_schedfire_replay.py::test_a_plain_duplicate_fire_keeps_the_key_and_is_skipped
  - tests/unit/worker/gates/test_execution_guard_skip_if_settled.py::test_a_replay_at_the_limit_reads_its_own_identity
---

**Entscheidung.** Die Ausführungsidentität ist `task_id|H#n` (H = `x-swarmq-replay-id`, n = `x-swarmq-xdeath-base` + `x-death`-Zähler außerhalb von Delay-Queues). Ein Replay aus der DLQ hat eine neue Identität und läuft, auch wenn der Done-Marker der ursprünglichen Ausführung steht. Eine normale Neuzustellung derselben Ausführung (gleiche Identität) wird weiter als Duplikat verworfen. Ein Cron-Replay läuft unter dem Schedfire-Key `K|<Identität>`; ein normaler Doppel-Fire behält K und wird verworfen. Wiederholtes `dlq retry` derselben XQ-Nachricht ergibt dieselbe Identität, der zweite Lauf wird verworfen.

**Begründung.** 0.11-Worker verwarfen ein Replay ohne `x-swarmq-replay-id` still (BZ-08), obwohl ein Replay bewusst ist. Gleichzeitig darf at-least-once-Zustellung nicht zu Doppelausführung führen.

**Bewusste Folgen.** Grenzen laut Plan Abschnitt 7: Ein Broker-DLX- oder Cron-Replay an einen 0.11-Worker wird verworfen; ein Replay in-flight beim Versionswechsel kann doppelt laufen; wiederholte UI-Moves nach erneutem Broker-Dead-Lettering werden übersprungen (`dlq retry` verwenden); UI-Replays mit `max_age` können sofort ablaufen; nach einem UI-Move zählt die erste Ausführung das rohe `x-death`. Mit `dead_letter_ttl_s = none` sind Replay-Identitäten 7 d geschützt.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
