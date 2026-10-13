---
title: unique_for=None schaltet die Deduplizierung für genau diesen Aufruf ab, auch gegen einen TaskInfo-Default
applies_when:
  - Änderungen an Client.schedule, schedule_many oder an der Persistenz verzögerter Schedules (delay/eta)
  - Änderungen an der Unique-Auflösung im Scheduler-Fire
  - eine Fehlersuche oder ein Plan stellt den Per-Call-Opt-out in Frage
tags: [client, unique, dedup, scheduler, zusage]
id: E-033
datum: 2026-04-17
status: gilt
quelle:
  - docs/brainstorms/2026-04-17-phase3-5-unique-tasks-requirements.md:70-75 (Per-Call-Override, unique_for=None deaktiviert Dedup)
  - website/reference/api.md:123 („Pass None to disable dedup for this call.“)
  - website/guides/locking-and-dedup.md:244-256
  - src/swarmq/client/schedule_resolve.py:120-124 (_resolve_unique)
tests:
  - tests/unit/client/test_schedule.py::TestClientUniqueSchedule::test_schedule_per_call_override_none_disables_dedup
  - tests/unit/client/test_schedule.py::TestClientUniqueScheduleMany::test_schedule_many_batch_override_none_disables_dedup
---

**Entscheidung.** Ein ausdrücklich übergebenes `unique_for=None` an `Client.schedule()` oder `schedule_many()` schaltet die Deduplizierung für diesen Aufruf ab, auch wenn die `TaskInfo` einen `unique_for`-Default setzt. Ein weggelassenes Argument nutzt den Default. Die Zusage gilt für sofortige und für verzögerte Aufrufe (`delay`, `eta`).

**Begründung.** Der Opt-out dient dazu, trotz laufender oder wartender Duplikate bewusst einen weiteren Lauf zu erzwingen.

**Bewusste Folgen.** Zwei Läufe mit gleichen Argumenten trotz `unique_for` in der `TaskInfo` sind kein Fehler, wenn der Aufrufer `unique_for=None` übergeben hat.

**Überdenken nur mit neuem Argument.** Heute verletzt für verzögerte Aufrufe: FS-14 (Fehlersuche Runde 2, xfail-Nachweis im Branch `test/fehlersuche-runde-2`). `schedule(delay=…, unique_for=None)` speichert den Opt-out nicht in der Definition, der Leader fällt beim Fire nach E-032 auf seine TaskInfo zurück, und der erzwungene Lauf verschwindet still, sobald ein anderer Lauf das Flag hält. Die markierten Tests decken nur den sofortigen Pfad ab.
