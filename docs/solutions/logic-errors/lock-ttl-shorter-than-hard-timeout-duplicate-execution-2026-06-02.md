---
title: "Task-Lock-TTL kürzer als hard_timeout → Mutex bricht, Doppelausführung"
date: 2026-06-02
category: logic-errors
module: src/swarmq/locking.py
problem_type: logic_error
component: locking
symptoms:
  - "Ein @task(lock=...) mit max_concurrency=1 (Mutex) wird gelegentlich zweimal gleichzeitig ausgeführt, obwohl der Lock genau das verhindern soll"
  - "Tritt nur bei Tasks mit hard_timeout > 30s auf, die länger als ~0.25·hard_timeout laufen und für die eine zweite Nachricht (Redelivery / zweite Instanz) während der Ausführung ankommt"
  - "Kein Fehler im Log — der zweite Worker akquiriert den Lock 'legitim', weil der erste bereits per TTL abgelaufen ist"
root_cause: logic_error
resolution_type: code_fix
severity: critical
tags:
  - locking
  - mutex
  - ttl
  - lock-renewal
  - duplicate-execution
  - valkey
  - production-safety
---

# Task-Lock-TTL kürzer als hard_timeout → Mutex bricht, Doppelausführung

## Problem

Die Lock-TTL wurde als `max(30s, 0.25 · hard_timeout)` berechnet. Für **jedes** `hard_timeout > 30s` ist dieser Wert **kürzer** als die maximale erlaubte Laufzeit des Tasks. Da Task-Locks nie erneuert wurden, lief der Lock mitten in einer legitimen Ausführung ab → ein zweiter Worker konnte denselben `@task(lock=...)`-Mutex akquirieren und denselben kritischen Abschnitt **parallel** ausführen. Damit ist die Kern-Garantie von `lock=` (gegenseitiger Ausschluss) für lang laufende Tasks gebrochen.

## Symptoms

- Beispiel: `hard_timeout=600s` → TTL = `max(30, 150)` = **150s**. Ein Task, der legitim 200s läuft, verliert seinen Lock nach 150s. Eine zwischenzeitlich redelivered Nachricht (oder eine zweite geplante Instanz) akquiriert den Lock und läuft parallel.
- Schwelle ist `hard_timeout > 30s` (nicht 120s, wie zunächst vermutet): `max(30, 0.25·ht) ≥ ht` gilt nur für `ht ≤ 30`.
- Doppelausführung von Arbeit, die explizit serialisiert werden sollte (z. B. „nur ein Report-Build pro `user_id` gleichzeitig").

## What Didn't Work

- **Auf das `renew()` verlassen.** `LockManager.renew()` existiert, wurde aber ausschließlich für den **Scheduler-Leader-Lock** (`scheduler.py`) aufgerufen, nie für Task-Locks. Es gab also keinen Heartbeat, der den Task-Lock am Leben hielt.
- **Dem Docstring glauben.** Der Docstring von `compute_lock_ttl_ms` behauptete, der Watchdog canceltden Task „vor dem TTL-Ablauf" — die Logik war invertiert: Der Watchdog feuert bei `hard_timeout`, der Lock lief aber schon bei `0.25·hard_timeout` ab.

## Solution

TTL = `hard_timeout + Puffer` (30s), sodass sie die maximale Laufzeit **überdauert**:

```python
# src/swarmq/locking.py
_LOCK_TTL_BUFFER_SECONDS = 30

def compute_lock_ttl_ms(hard_timeout: int | None) -> int:
    if hard_timeout is None:
        raise ValueError("lock requires hard_timeout to be set; ...")
    seconds = hard_timeout + _LOCK_TTL_BUFFER_SECONDS   # war: max(30, 0.25*hard_timeout)
    return int(seconds * 1000)
```

Statische TTL statt Renewal-Heartbeat gewählt: kein Lost-Lock-Fencing-Problem (ein fehlschlagendes `renew()` während der Ausführung würde den Lock freigeben und Doppelausführung wieder ermöglichen), eine einzige Formeländerung, und dieselbe TTL regelt auch die Semaphore-Slot-Expiry — ein noch laufender Semaphore-Holder wird damit ebenfalls nicht mehr weggeswept.

## Why This Works

Die Lock-TTL ist ausschließlich eine **Crash-Recovery-Deadline**: Im Normalbetrieb cancelt der Watchdog den Task bei `hard_timeout` und der `finally`-Block gibt den Lock kooperativ frei — die TTL wird nie erreicht. Sie greift nur, wenn ein Holder hart stirbt (SIGKILL/OOM) ohne Freigabe. Damit sie in diesem Fall keinen noch laufenden Holder „überholt", muss sie länger sein als die längste legitime Laufzeit (`hard_timeout`). Da Locks server-seitige Valkey-`TIME` nutzen, gibt es keinen Inter-Worker-Clock-Skew zu budgetieren; der Puffer deckt nur Watchdog-Präzision und Release-Latenz.

Trade-off: Für ein langes `hard_timeout` ist das Recovery-Fenster entsprechend lang (ein 1h-Mutex-Task → ~1h Recovery nach hartem Crash). Das ist der korrekte Preis für gegenseitigen Ausschluss ohne Renewal-Heartbeat.

## Prevention

- **Invariante als Test kodieren:** `compute_lock_ttl_ms(ht) > ht*1000` für alle realistischen `ht` (parametrisierter Test in `tests/unit/test_locking.py`).
- **Faustregel:** Eine Lock-/Lease-TTL, die kürzer als die geschützte Operation sein kann, ist immer ein Bug — entweder TTL ≥ max. Operationsdauer, oder ein Renewal-Heartbeat MIT Lost-Lock-Abbruch (Fencing).
- **Docstrings über Timing-Garantien gegen die Formel prüfen:** Der alte Docstring behauptete eine Sicherheit, die die Formel für `ht > 30s` nicht hergab.

## Related Issues

- [[worker-cancel-safe-teardown-detached-tasks-2026-06-01]] — derselbe `finally`/Release-Pfad, der den Lock kooperativ freigibt
- [[scheduler-cron-sync-clobber-test-flake-2026-06-01]] — `renew()` wird für den Scheduler-Leader-Lock genutzt (der Pfad, der hier für Task-Locks fehlte)
- Teil des Produktionssicherheits-Audits 2026-06-02 (P0-A), Plan `docs/plans/2026-06-02-001-fix-production-safety-hardening-plan.md`, MR !8
