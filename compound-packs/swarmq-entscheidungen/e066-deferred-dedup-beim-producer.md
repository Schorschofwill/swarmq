---
title: Bei schedule(delay=/eta=) beansprucht ein Producer mit Registry den Unique-Claim sofort mit TTL unique_for plus Verzögerung; der Leader setzt dann keinen eigenen
applies_when:
  - Änderungen an Client._schedule_deferred, an der Unique-Auflösung im Producer oder an scheduler/fire.py (source cli_unique)
  - eine Fehlersuche meldet, dass ein verzögerter Task schon bei schedule() als Duplikat gilt, nicht erst beim Fire
  - eine Fehlersuche meldet, dass das Unique-Flag eines verzögerten Tasks länger lebt als unique_for
tags: [unique, dedup, scheduler, client, f3-29]
id: E-066
datum: 2026-04-17
status: gilt
quelle:
  - docs/plans/2026-04-17-004-feat-phase3-5-unique-tasks-plan.md:56 (R10, Deferred-Pfad-SETNX-Timing, origin CD11), :694-760 (Unit 7, Dedup vor dem ScheduleStore-Insert)
  - src/swarmq/client/schedule.py:589-597 (The producer here is authoritative)
tests:
  - tests/unit/client/test_schedule.py::TestClientUniqueDeferred::test_deferred_with_unique_for_sets_flag_and_persists_hash
  - tests/unit/client/test_schedule.py::TestClientUniqueDeferred::test_deferred_duplicate_returns_existing_and_skips_store
  - tests/unit/client/test_schedule.py::TestClientUniqueDeferred::test_deferred_with_eta_ttl_covers_fire_plus_unique_for
  - tests/unit/client/test_schedule.py::TestClientDeferredEnqueueMiddleware::test_veto_on_a_deferred_duplicate_propagates_instead_of_the_existing_id
---

**Entscheidung.** Löst ein Producer bei `schedule(delay=…/eta=…)` ein `unique_for` auf (Aufrufparameter oder `TaskInfo`-Default aus seiner Registry), beansprucht er den Unique-Claim vor dem Speichern im Scheduler-Store mit TTL `(unique_for + Verzögerung)`. Die Definition wird als `cli_unique` gespeichert; der Leader setzt beim Fire keinen eigenen Claim. Ein Duplikat gibt die bestehende `task_id` zurück und feuert `TASK_DUPLICATE_DETECTED`, ohne zu speichern. Nur ein Producer ohne Registry und ohne ausdrückliches `unique_for` überlässt die Entscheidung dem Leader (E-032).

**Begründung.** Ein Claim beim Fire käme zu spät: Zwei Producer könnten denselben verzögerten Task zweimal speichern. Ein Claim an zwei Stellen erzeugte ein Doppel-Claim-Race (Unit 7).

**Bewusste Folgen.** Das Flag lebt ab `schedule()` bis Fire plus `unique_for`; ein gleicher Aufruf in dieser Zeit ist ein Duplikat, auch lange vor dem Fire. Das ist kein Fehler.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt. `scheduling-and-retries.md:52-54` (+ DE) widersprach sich dazu selbst; korrigiert in 0.12.3 (F3-29, Doku-Fix: Guide und API-Referenz unterscheiden Producer mit und ohne Registry).

**Nachtrag 2026-10-15 (F3-02, U15, E-059).** `pre_enqueue` läuft im verzögerten Pfad vor dem Claim (E-059 hat Vorrang). Ein Veto aus `pre_enqueue` bei einem Aufruf, der ein Duplikat wäre, propagiert darum das Veto, statt die bestehende `task_id` zurückzugeben (bis 0.12.2 kam die `task_id`). Im sofortigen Pfad liegt der Claim vor `pre_enqueue`, dort gibt ein Duplikat weiter die bestehende `task_id` zurück, ohne die Middleware zu fragen. Das ist kein Fehler. Upgrade-Hinweis im Changelog (U17).
