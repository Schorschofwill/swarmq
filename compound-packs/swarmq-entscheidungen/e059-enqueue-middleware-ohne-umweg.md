---
title: Kein Enqueue über Client oder Workflow umgeht pre_enqueue und TASK_ENQUEUED; bei schedule(delay=/eta=) läuft pre_enqueue vor dem Speichern, TASK_ENQUEUED danach
applies_when:
  - Änderungen an Client.schedule, _schedule_deferred, schedule_many, WorkflowAdvancer oder an der Persistenz verzögerter Schedules
  - ein Plan führt einen neuen Publish- oder Speicherpfad für Tasks ein
  - eine Fehlersuche meldet, dass TASK_ENQUEUED bei delay/eta beim Speichern statt beim Fire feuert
  - eine Fehlersuche meldet, dass ein Veto aus pre_enqueue einen verzögerten Task schon vor dem Speichern verhindert
  - eine Fehlersuche meldet, dass Cron-Fires des Leaders die Enqueue-Middleware umgehen
tags: [middleware, signals, client, scheduler, workflow, b9, f3-02]
id: E-059
datum: 2026-10-15
status: gilt
quelle:
  - Georg 2026-10-15 (Fehlersuche Runde 3, F3-02)
  - docs/plans/2026-08-30-001-fix-inventar-debug-backlog.md:215 (B9, Workflow-Folgeschritte umgehen Middleware und TASK_ENQUEUED)
  - docs/plans/2026-09-08-001-feat-debug-backlog-loadsuite-plan.md:16, 92 (B9 in 0.6.0 behoben)
  - website/reference/changelog.md:2468 (0.6.0, Workflow steps run through the enqueue middleware)
  - website/guides/middleware.md:12, 27 (every task; pre_enqueue aborts the enqueue)
tests:
  - tests/unit/worker/test_pipeline.py::test_workflow_step_publisher_runs_middleware_then_signals
  - tests/unit/client/test_befund_f3_02_deferred_pre_enqueue.py::test_pre_enqueue_veto_blocks_deferred_schedule
  - tests/unit/client/test_befund_f3_02_deferred_pre_enqueue.py::test_pre_enqueue_veto_precedes_unique_claim_on_deferred_schedule
  - tests/unit/client/test_befund_f3_02_deferred_pre_enqueue.py::test_task_enqueued_fires_after_store_on_deferred_schedule
  - tests/unit/client/test_befund_f3_02_deferred_pre_enqueue.py::test_middleware_kwargs_change_is_stored_with_deferred_definition
  - tests/unit/client/test_schedule.py::TestClientDeferredEnqueueMiddleware::test_post_enqueue_runs_once_after_store_with_the_same_message
  - tests/unit/client/test_schedule.py::TestClientDeferredEnqueueMiddleware::test_dedup_hash_comes_from_the_arguments_before_the_middleware
  - tests/unit/client/test_schedule.py::TestClientDeferredEnqueueMiddleware::test_duplicate_sees_pre_enqueue_without_post_enqueue_or_signal
  - tests/unit/client/test_schedule.py::TestClientDeferredEnqueueMiddleware::test_veto_skips_post_enqueue
  - tests/unit/client/test_schedule.py::TestClientDeferredEnqueueMiddleware::test_validation_errors_come_before_pre_enqueue
  - tests/unit/client/test_schedule.py::TestClientDeferredEnqueueMiddleware::test_middleware_change_to_args_is_not_stored
  - tests/unit/client/test_schedule.py::TestClientDeferredEnqueueMiddleware::test_store_conflict_without_unique_for_raises_without_release
  - tests/unit/client/test_schedule.py::TestClientDeferredEnqueueMiddleware::test_store_conflict_raises_and_keeps_the_own_claim
  - tests/unit/client/test_schedule.py::TestClientDeferredEnqueueMiddleware::test_store_outage_keeps_the_claim
  - tests/unit/client/test_schedule.py::TestClientDeferredEnqueueMiddleware::test_store_error_reply_skips_post_enqueue_and_signal_and_releases_the_claim
---

**Entscheidung.** Jeder Enqueue über den Client (`schedule`, `schedule_many`, `apply`) und jeder Workflow-Folgeschritt läuft durch `pre_enqueue` und löst `TASK_ENQUEUED` aus. Bei `schedule(delay=…/eta=…)` läuft `pre_enqueue` im Producer vor dem Speichern im Scheduler-Store und vor dem Unique-Claim; ein Veto verhindert beides. `TASK_ENQUEUED` feuert nach dem Speichern, nicht erst beim Fire. Header- und Message-Änderungen der Middleware werden mit der Definition gespeichert, und der Leader publiziert sie beim Fire.

**Begründung.** Middleware und Signale versprechen „every task“; ein Umweg an ihnen vorbei war schon bei den Workflow-Schritten ein Fehler (B9, 0.6.0). Beim Speichern ist der Enqueue aus Sicht des Aufrufers erledigt; der Fire liegt in einem anderen Prozess (Leader), der den Producer-Kontext nicht kennt.

**Bewusste Folgen.** Ein Veto wirkt zum Zeitpunkt von `schedule()`, nicht zum Fire-Zeitpunkt. Middleware, die beim Fire neu entscheiden will, sieht den Fire nicht. Statische Crons und CLI-Crons, die der Leader feuert, laufen bis zur Klärung von F3-36 ohne Enqueue-Middleware; das ist kein Fehler gegen diese Regel.

**Überdenken nur mit neuem Argument.** Offen: F3-36 (sollen Leader-Fires durch `pre_enqueue` und `TASK_ENQUEUED` laufen?). Dass `schedule(delay=/eta=)` bis 0.12.2 vor `pre_enqueue` zurückkehrte, war der Befund F3-02 (behoben in 0.12.3).

**Nachtrag 2026-10-15 (F3-02 behoben in 0.12.3, U15, KTD5).** Reihenfolge im verzögerten Pfad: Prüfungen (`delay`/`eta`, `unique_for`, `eta` in der Vergangenheit) laufen vor der Middleware und scheitern ohne `pre_enqueue`. Danach `task_id`, Dedup-Hash, `pre_enqueue`, Unique-Claim, Speichern, `post_enqueue`, `TASK_ENQUEUED`.
- **`post_enqueue`** läuft nach dem Speichern genau einmal mit demselben Kontext wie `pre_enqueue`, nicht beim Veto, nicht bei einem Fehler oder Konflikt beim Speichern und nicht im Duplikatpfad. Bewusste Folge: Anders als im sofortigen Pfad (Claim vor `pre_enqueue`) sieht die Middleware bei einem verzögerten Duplikat `pre_enqueue` ohne `post_enqueue`.
- **Dedup-Hash** stammt aus den Argumenten vor der Middleware, wie im sofortigen Pfad. Eine Middleware, die je Aufruf `kwargs` ergänzt (etwa eine `correlation_id`), hebelt die Dedup verzögerter Aufrufe damit nicht aus, und gleiche sofortige und verzögerte Aufrufe kollidieren weiter. Grenze: Bei `source="cli"` (Producer ohne Registry und ohne `unique_for`, E-032) hasht der Leader beim Fire die gespeicherten `kwargs` samt Ergänzung.
- **Header und Nachricht:** `MessageContext` hat kein Header-Feld, eine Middleware kann heute keine Header setzen. Gespeichert werden wie im sofortigen Pfad die In-place-Änderungen an `kwargs`; Änderungen an `args` oder neu zugewiesene Schlüssel wirken nicht. Header kommen erst mit einer `MessageContext`-Erweiterung in einer Minor-Version (ein Feld in `ScheduleDefinition` würden Leader 0.12.0–0.12.2 still verwerfen).
- **Konflikt beim Speichern:** Meldet `add_schedule` `False` (seit F3-01, E-058, nur noch ein abweichender oder verschwundener Eintrag), wirft `schedule()` `BackendResponseError`. Der eigene Unique-Claim bleibt stehen (Review U15, siehe unten).
- **Claim nach einem Fehler beim Speichern (Review U15).** Das Speichern entscheidet, ob der eigene Claim freigegeben wird:
  - *Konflikt* (`False`): Bei einem Oneshot mit frischer UUID kann das nach verlorener Antwort nur der eigene, vom Leader schon gefeuerte und gelöschte Eintrag sein (E-058, „verschwundener Eintrag gilt als Konflikt“). Der Task kann also schon laufen; der Claim bleibt stehen, damit ein Wiederholversuch des Aufrufers nicht doppelt ausführt.
  - *Backend-Ausfall* (Timeout, Verbindung; `is_backend_outage`): Unbestimmt, denn `sched_add.lua` speichert Definition und Fire-Index in einem Schritt, der Eintrag kann gespeichert sein und feuern. Der Claim bleibt stehen wie im sofortigen Pfad nach `PublishIndeterminate` (E-041). Ein Wiederholversuch wird bis zum Ablauf der Sperre (`unique_for` plus Verzögerung) als Duplikat abgewiesen; das ist kein Fehler, Blockieren ist für ein Einmaligkeits-Feature die richtige Richtung.
  - *Fehlerantwort* (`BackendResponseError` aus dem Store, nichts gespeichert) und *jeder andere Fehler*, auch ein Abbruch: Der eigene Claim wird owner-geprüft freigegeben.
  - Verträglichkeit: E-052 regelt den Ausfall beim **Claim selbst** (dort Freigabe, weil noch nichts gespeichert ist) und bleibt unberührt; hier geht es um das Speichern danach. E-058 bleibt unverändert, die Regel legt nur die Folge des Konflikts für den Claim fest.
