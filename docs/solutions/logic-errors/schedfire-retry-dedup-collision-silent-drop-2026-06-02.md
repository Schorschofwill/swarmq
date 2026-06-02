---
title: "Scheduled-Task-Retry still gedroppt: completed-Marker im finally kollidiert mit Retry-Dedup"
date: 2026-06-02
category: logic-errors
module: src/swarmq/worker.py
problem_type: logic_error
component: scheduler
symptoms:
  - "Ein per Cron/ETA/Delay gefeuerter Task mit max_retries>0 schlägt beim ersten Versuch fehl und wird NIE wiederholt — kein Retry, kein DLQ-Eintrag, kein Fehlerresultat"
  - "Im Log: 'task_retrying' (delay_ms=…) gefolgt ~delay später von 'schedfire_completed_duplicate_acked' für denselben schedfire_key; die zweite Ausführung läuft nie"
  - "get_result() läuft in den Timeout statt einen TaskError zu liefern"
root_cause: logic_error
resolution_type: code_fix
severity: critical
tags:
  - scheduler
  - schedfire
  - dedup
  - retry
  - silent-drop
  - finally-block
  - at-least-once
  - production-safety
---

# Scheduled-Task-Retry still gedroppt: completed-Marker kollidiert mit Retry-Dedup

## Problem

Scheduled-Fire-Nachrichten (Cron/ETA/Delay) tragen einen `x-swarmq-schedfire`-Header und werden über zwei Marker dedupliziert: einen kurz-TTL **inflight**-Marker (SETNX beim Pickup) und einen lang-TTL **completed**-Marker (geschrieben im `finally` nach Abschluss). Ein fehlschlagender Task mit `max_retries>0` wurde beim **ersten** Fehlversuch **still gedroppt**: Der Retry-Republish behielt den `x-swarmq-schedfire`-Header, und der `finally`-Block schrieb **unbedingt** den completed-Marker. Wenn die redelivered Retry-Kopie ankam, sah `is_schedfire_completed()` den Marker → **ack + skip**. Die konfigurierten Retries liefen nie.

## Symptoms

- `task_retrying` (`delay_ms=500`) → ~500ms später `schedfire_completed_duplicate_acked` (gleicher `schedfire_key`) → `get_result()`-Timeout.
- Betraf **jeden** nicht-terminalen Pfad, nicht nur Retry: Rate-Limit-Requeue, Lock-Conflict-Requeue, Shutdown-nack, Result-Write-nack — alle laufen durch denselben `finally` und schrieben den completed-Marker, sodass die redelivered Kopie geskippt wurde.

## What Didn't Work

- **Annahme „Retry-Dedup und Fire-Dedup sind unabhängig".** Sie teilen sich dieselbe Fire-Identität (`schedule:score`). Der completed-Marker bedeutet de facto „diese Fire-Identität nie wieder ausführen" — aber der Retry teilt sich genau diese Identität.
- **Nur den completed-Marker unterdrücken (ohne den inflight-Marker zu behandeln).** Bei nack(requeue=True)-Pfaden (gleiche Nachricht redelivered) wäre der inflight-Marker (300s TTL) bei der Redelivery noch da → SETNX verliert → „another worker running" → erneut skip → der Drop bliebe.

## Solution

Ein `_schedfire_terminal`-Flag analog zum existierenden `_unique_release_pending`. Es startet `True` und wird in **exakt denselben** nicht-terminalen Zweigen auf `False` gesetzt, in denen auch `_unique_release_pending = False` gesetzt wird. Die Finalize-Methode verzweigt darauf:

```python
async def _finalize_schedfire(self, message, *, terminal: bool) -> None:
    schedfire_key = self._schedfire_inflight_claims.pop(id(message), None)
    if not schedfire_key:
        return
    if terminal:                       # success / error→DLQ / pre-start cancel
        await self._backend.set_schedfire_dedup(schedfire_key, dedup_ttl_ms)
    else:                              # retry / rate-limit / lock / shutdown requeue
        await self._backend.clear_schedfire_inflight(schedfire_key)
```

Bei nicht-terminalem Outcome wird also der **inflight-Marker gelöscht** (neue Backend-Methode `clear_schedfire_inflight`), statt den completed-Marker zu schreiben — so kann die redelivered/Retry-Kopie neu claimen und laufen.

## Why This Works

Der inflight-Marker (kurz-TTL) verhindert weiterhin gleichzeitige Doppelausführung **derselben** Fire; der completed-Marker (lang-TTL) verhindert weiterhin, dass ein **echtes** Duplikat-Fire (z. B. vom Scheduler bei Leader-Wechsel) eine bereits **abgeschlossene** Fire erneut ausführt. Der Bug war, beide Marker für **nicht abgeschlossene** (= neu zuzustellende) Fires zu setzen. Das Flag trennt sauber: terminal → completed schreiben; nicht-terminal → inflight freigeben. Die Richtung ist sicher gewählt — ein versehentliches `False` (über-freigeben) führt höchstens dazu, dass ein seltenes Duplikat-Fire läuft (at-least-once, tolerierbar), während ein versehentliches `True` den stillen Drop reproduzieren würde.

## Prevention

- **Empirisch reproduzieren vor dem Fix:** Ein integration-RED-Test (`tests/integration/test_worker_schedfire_dedup.py::test_scheduled_task_retry_is_not_dropped`) gegen echte RabbitMQ+Valkey, der den Drop über die Log-Sequenz nachweist (`task_retrying` → `schedfire_completed_duplicate_acked` → Timeout).
- **Regel für `finally`-Blöcke an Dedup-/Lock-Lebenszyklen:** Wenn ein `finally` einen „abgeschlossen"-Zustand persistiert, muss er zwischen **terminalen** und **nicht-terminalen** Austritten unterscheiden. Das Projekt hatte dieses Muster für `_unique_release_pending` bereits — der schedfire-Marker hat es nur nicht mitgenutzt.
- **Beim Spiegeln eines Flags alle Set-Stellen koppeln:** `_schedfire_terminal = False` steht jetzt neben **jedem** `_unique_release_pending = False` (8 Stellen); ein Konsistenz-Check (`grep -c`) hält beide Zähler gleich.

## Related Issues

- [[scheduler-cron-sync-clobber-test-flake-2026-06-01]] — selbes schedfire/Scheduler-Subsystem, ebenfalls last-/timing-abhängig
- [[nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16]] — DLQ-/Requeue-Semantik, die hier mit der Dedup kollidierte
- [[worker-pipeline-stage-ack-ordering-2026-04-16]] — ack-nach-Persist-Ordnung im selben `_handle_message`-finally
- Teil des Produktionssicherheits-Audits 2026-06-02 (P0-B), Plan `docs/plans/2026-06-02-001-fix-production-safety-hardening-plan.md`, MR !8
