---
title: "Pattern: cancel-safe Worker-Teardown für detachte Sub-Tasks (Scheduler-Orphan)"
date: 2026-06-01
category: patterns
module: src/swarmq/worker.py
problem_type: resource_leak
component: worker
symptoms:
  - "Nach Cancel der start_worker()-Task bleiben SchedulerLoop._election_loop/._poll_loop (+ aio_pika Connection-Tasks) in asyncio.all_tasks() PENDING"
  - "'Future exception was never retrieved: CancelledError'-Logzeilen in Tests"
  - "Zombie-Scheduler hält/re-acquired den Leader-Lock nach einem 'Crash' → nichtdeterministische Leader-Tests"
root_cause: resource_leak
resolution_type: code_fix
severity: medium
tags:
  - asyncio
  - task-leak
  - cancellation
  - shield
  - graceful-shutdown
  - scheduler
  - detached-task
  - idempotent-teardown
---

# Pattern: cancel-safe Worker-Teardown für detachte Sub-Tasks

## Problem

`Worker.start()` startete den Scheduler (`_election_task`/`_poll_task` als
detachte `asyncio.create_task`s) und blockierte auf
`await self._shutdown_event.wait()` — **ohne** umschließendes `try/finally`. Der
graceful Pfad (`_request_shutdown` → `_execute_shutdown` → `scheduler.stop()`)
räumte sauber auf, aber ein **Cancel** der `start_worker()`-Coroutine (Tests,
ProcessManager-`--reload`-execvp, eingebettete Nutzung) ließ `CancelledError`
rauspropagieren und übersprang `_execute_shutdown` → die detachten Scheduler-
Loops (und aio_pika-Connection-Tasks) leakten.

Empirisch belegt: nach `start_worker()`-Cancel + await blieben 8 Tasks PENDING,
darunter `SchedulerLoop._election_loop`/`._poll_loop`.

## Solution

Den Steady-State-Body in `try/finally` umschließen; das `finally` ruft ein
**idempotentes** `_teardown_on_exit`, das auf jedem Austrittspfad (graceful,
Exception, Cancel) läuft:

1. **Idempotenz-Guard** (`self._torn_down`): No-op, sobald `_execute_shutdown`
   schon lief (graceful Pfad setzt das Flag am Ende).
2. **Nur eigene Ressourcen** abräumen: Burst-Monitor canceln, `scheduler.abort()`
   (cancelt Sub-Tasks OHNE graceful Handover/Release — ein Crash macht keinen
   Handover, der Lock läuft per TTL ab), Signal-Handler entfernen.
3. **NICHT** Broker/Backend schließen — die besitzt der SwarmQ-Context-Manager
   (`__aexit__`); ein Caller, der `start_worker` cancelt, nutzt sie evtl. weiter
   (z. B. Cancel-Flag lesen). Sie hier zu schließen regression-te
   `test_worker_cancellation`. Die aio_pika-Tasks werden beim Context-Exit
   gereapt.

### Cancel-sicheres Awaiten (detached-shield)

Das Teardown läuft als geshieldete Kind-Task, damit es **vollendet, auch während
start() selbst gecancelt wird**:

```python
teardown = asyncio.create_task(self._abort_teardown())
try:
    await asyncio.shield(teardown)
except asyncio.CancelledError:
    await teardown   # 1. CancelledError ist konsumiert → nächstes await wartet
                     #    wirklich → Sub-Task-Ergebnisse werden abgeholt
                     #    (keine 'Future exception never retrieved')
    raise            # Cancel-Semantik bewahren
```

Schlüssel-Einsicht: Nach dem ersten in die Task geworfenen `CancelledError`
(im `except` gefangen) ist die Task nicht automatisch erneut gecancelt — das
folgende `await teardown` wartet daher tatsächlich auf Completion. Gleiches
Muster wie `_detached_releases` / der DI-Scope-Exit in `_handle_message`.

## Why This Works

`scheduler.abort()` ruft synchron `.cancel()` auf beiden Sub-Tasks (immer
ausgeführt) und awaitet sie mit Suppress — das holt deren CancelledError ab
(keine Noise) und beendet den Orphan/Zombie. Der Idempotenz-Guard verhindert
Doppel-Teardown gegenüber dem graceful Pfad.

## Prevention

- Detachte `create_task`-Sub-Tasks brauchen einen `try/finally`-Teardown auf
  JEDEM Austrittspfad ihres Owners — nicht nur im graceful Branch.
- Teardown nur für **selbst erzeugte** Ressourcen; geliehene Connections gehören
  dem Context-Manager/Caller (sonst Regression bei Cancel-und-weiterverwenden).
- Cleanup, das während Cancellation vollenden MUSS: detached-shield + im
  `except` die geshieldete Task awaiten, dann re-raise.

## Scope note

Produktions-`SIGTERM` ist bereits graceful (kein Orphan). Der Fix trifft den
Cancel-Pfad (Tests, ProcessManager-Reload, Embedding) und beseitigte die Wurzel
der Leader-Test-Fragilität.

## References

- `src/swarmq/worker.py::_teardown_on_exit` / `_abort_teardown`; `src/swarmq/scheduler.py::abort`
- Test: `tests/e2e/test_worker_cancel_teardown.py`
- Verwandt: [scheduler-cron-sync-clobber-test-flake-2026-06-01.md](../logic-errors/scheduler-cron-sync-clobber-test-flake-2026-06-01.md)
