---
title: unique_for/unique_until eines Crons reisen mit der Definition; der Leader nutzt sie statt seiner Registry
applies_when:
  - ein Cron mit unique_for wird von einem Leader gefeuert, der den Task nicht oder anders registriert hat
  - Änderungen am Cron-Report, ScheduleDefinition-Fingerabdruck oder an der Unique-Freigabe in scheduler/fire.py
  - eine Fehlersuche meldet, dass die Unique-Sperre bei PublishIndeterminate oder bei fremdem cli_unique-Flag nicht freigegeben wird
tags: [scheduler, cron, unique, bz-09]
id: E-011
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:129 (Entscheidungen, BZ-09)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:410 (MR 9)
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:154-158 (KTD7, Selbsttreffer nach Transport-Retry, FS-06b, U11)
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:159-163 (KTD8, Claim-Freigabe nach Timeout, FS-07, U12)
tests:
  - tests/unit/scheduler/test_fire.py::test_old_definition_takes_both_unique_values_from_the_registry
  - tests/unit/scheduler/test_fire.py::test_definition_with_unique_for_beats_the_registry
  - tests/unit/scheduler/test_fire.py::test_publish_failure_releases_the_own_claim_so_the_next_poll_fires
  - tests/unit/scheduler/test_fire.py::test_indeterminate_publish_keeps_the_claim
  - tests/unit/scheduler/test_fire.py::test_publish_failure_keeps_a_cli_unique_claim_of_the_producer
  - tests/unit/scheduler/test_reports.py::TestFingerprint::test_fingerprint_without_unique_for_is_unchanged_since_0_11
  - tests/unit/scheduler/test_befund_bz02_bz07_bz09_bz28_scheduler.py::test_bz09_cron_unique_for_holds_on_leader_without_task_in_registry
  - tests/unit/scheduler/test_fire.py::test_oneshot_fire_stays_duplicate_when_the_claim_holds_its_task_id
  - tests/unit/scheduler/test_fire.py::test_oneshot_without_stable_id_publishes_when_the_claim_holds_its_own_task_id
  - tests/unit/scheduler/test_fire.py::test_oneshot_claim_timeout_keeps_a_flag_of_its_stable_task_id
---

**Entscheidung.** Cron-Report und `ScheduleDefinition` führen `unique_for` und `unique_until` mit. Ist `definition.unique_for` gesetzt, kommen beide Werte aus der Definition, sonst beide aus der Registry des Leaders (nie gemischt). Der Fingerabdruck enthält sie nur bei gesetztem `unique_for` (sonst byte-gleich zu 0.11). Bei einem Publish-Fehler gibt der Leader die Unique-Sperre nur frei, wenn er sie in genau diesem Fire beansprucht hat; nicht bei fremden `cli_unique`-Flags und nicht bei `PublishIndeterminate`.

**Begründung.** Vorher hing `unique_for` davon ab, welcher Worker gerade Leader war; mit anderer App oder anderem `--module` überlappten lange Cron-Läufe (BZ-09). Bei `PublishIndeterminate` kann der Lauf schon unterwegs sein.

**Bewusste Folgen.** Nach `PublishIndeterminate` feuert der nächste Poll den Cron nicht, bis die Sperre abläuft. Wirkt erst mit einem Leader ≥ 0.12 (Plan Abschnitt 7).

**Nachtrag 2026-10-14 (FS-06b, U11, KTD7).** Meldet `claim_unique` beim Fire die eigene `task_id` als „existing“, wertet der Leader das nur als eigenen Claim, wenn `_fire_task_id` die ID für dieses Fire frisch erzeugt hat (Cron, Oneshot ohne `task_id`-Feld und ohne `oneshot:`-Präfix, etwa CLI `--at`); dann war es die Transport-Wiederholung nach verlorener Antwort, und das Fire wird publiziert. Unter stabiler ID (Oneshot aus `schedule()`, Feld `task_id` oder `oneshot:<id>`) ist der Selbsttreffer nicht von einem echten Duplikat zu unterscheiden, denn auch der Folge-Poll nach `PublishIndeterminate` trifft den eigenen Claim; er bleibt Duplikat. Folge: Geht beim ersten Fire eines solchen Oneshots die Antwort auf den Claim verloren, endet das Fire als Duplikat und wird festgeschrieben, ohne zu publizieren. Das ist hingenommen, weil die Alternative bei `PublishIndeterminate` doppelt publizierte.

**Nachtrag 2026-10-14 (FS-07, U12, KTD8).** Läuft `claim_unique` beim Fire in einen Ausfall (Timeout, Verbindung), kann Valkey das Flag trotzdem gesetzt haben. Unter frisch erzeugter `task_id` (Cron, Oneshot ohne stabile ID) gibt der Leader es danach owner-geprüft frei; der nächste Poll feuert. Unter stabiler ID (Oneshot aus `schedule()`, Feld `task_id` oder `oneshot:<id>`) gibt er nach einem Claim-Timeout nichts frei: das Flag kann aus einem früheren Poll mit `PublishIndeterminate` stammen, und eine Freigabe ließe den Folge-Poll ein zweites Mal publizieren. Folge: Setzte der verlorene Claim das Flag, wertet der nächste Poll den Oneshot als Duplikat (wie im Nachtrag FS-06b). Eine Fehlerantwort (`BackendResponseError`) gibt in keinem Fall frei, der Claim lief dann nicht.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
