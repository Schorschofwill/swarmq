---
title: "SwarmQ Phase 0+1 Review: Async Deadlock and Error Handling Fixes"
date: 2026-04-15
category: logic-errors
module: swarmq
problem_type: logic_error
component: tooling
symptoms:
  - "Worker consumer loop stalls under concurrency — semaphore.acquire() blocks aio-pika dispatch callback"
  - "ValkeyBackend raises raw ValkeyError/OSError instead of BackendConnectionError — Worker error handler never fires"
  - "Double-connect on Broker/Backend leaks file descriptors and TCP connections"
  - "Partial connect failure in __aenter__ orphans broker connection"
  - "asyncio.get_event_loop() DeprecationWarning in Python 3.10+"
root_cause: async_timing
resolution_type: code_fix
severity: high
tags:
  - asyncio
  - semaphore
  - deadlock
  - aio-pika
  - valkey
  - error-wrapping
  - resource-leak
  - connect-guard
---

# SwarmQ Phase 0+1 Review: Async Deadlock and Error Handling Fixes

## Problem

A code review of SwarmQ's Phase 0+1 implementation found 7 defects (1 P0, 5 P1, 2 P2) across 7 files. The most critical was a semaphore deadlock in the aio-pika consumer callback that would have caused complete consumer freeze under any concurrency. Secondary issues included missing exception wrapping, resource leaks, and a deprecated API call.

## Symptoms

- **P0 — Consumer freeze:** aio-pika delivers messages by `await`-ing each consume callback. With `await semaphore.acquire()` inside the callback, the second message blocks waiting for the first to finish — a complete deadlock under any load above 1 concurrent task.
- **P1 — Unhandled Valkey exceptions:** Transient connection drops during `store_result`/`get_result`/`delete_result` surfaced as raw `ValkeyError`, bypassing the Worker's `except BackendConnectionError` handler. Messages left neither acked nor nacked.
- **P1 — Connection leak:** Calling `connect()` twice (e.g., `__aenter__` then `Worker.start()`) opened new connections without closing old ones, leaking file descriptors.
- **P1 — Partial connect leak:** If `backend.connect()` raised in `__aenter__`, the already-opened broker connection was orphaned (Python only calls `__aexit__` if `__aenter__` completed).
- **P1 — Deprecated API:** `asyncio.get_event_loop()` emits DeprecationWarning in Python 3.10+ and is removed in 3.14+.

## What Didn't Work

The research phase produced comprehensive aio-pika documentation (`docs/recherche-aio-pika-rabbitmq.md`, 1,376 lines) that covered consumer callback patterns. The planning flow-analyzer even flagged the semaphore concern. However, the implementation agent still wrote `await self._semaphore.acquire()` directly inside the callback. (session history)

**Blocking consume callback (before):**

```python
# worker.py — BEFORE
async def _on_message(self, message) -> None:
    # Full pipeline ran here — aio-pika awaited this entire coroutine
    try:
        payload = decode_json(message.body)
        ...
    await self._semaphore.acquire()  # blocks dispatch loop
    try:
        ...
    finally:
        self._semaphore.release()
```

**Bare Valkey I/O (before):**

```python
# backend/valkey.py — BEFORE
async def store_result(self, task_id, result_data, ttl_ms):
    await self._client.set(self._key(task_id), result_data, px=ttl_ms)
    # ValkeyError escapes — Worker catches BackendConnectionError, never fires
```

**No connect guard (before):**

```python
# broker/rabbitmq.py — BEFORE
async def connect(self):
    self._connection = await aio_pika.connect_robust(self._url)  # always opens new
```

## Solution

### P0 — Fire-and-forget dispatch with `asyncio.create_task`

```python
# worker.py — AFTER
async def _on_message(self, message) -> None:
    """Callback returns immediately — aio-pika dispatch loop is not blocked."""
    asyncio.create_task(self._handle_message(message))

async def _handle_message(self, message) -> None:
    """All logic moved here — runs as independent asyncio Task."""
    # deserialize, acquire semaphore, execute, store, ack/nack
    ...
```

### P1 — Wrap all Valkey I/O in BackendConnectionError

```python
# backend/valkey.py — AFTER
async def store_result(self, task_id, result_data, ttl_ms):
    try:
        await self._client.set(self._key(task_id), result_data, px=ttl_ms)
    except (ValkeyError, OSError) as exc:
        raise BackendConnectionError(str(exc)) from exc
```

Same pattern for `get_result()` and `delete_result()`.

### P1 — Idempotent connect guards

```python
# broker/rabbitmq.py — AFTER
async def connect(self):
    if self._connection is not None:
        return  # already connected
    ...

# backend/valkey.py — AFTER
async def connect(self):
    if self._client is not None:
        return  # already connected
    ...
```

### P1 — Partial connect cleanup in `__aenter__`

```python
# app.py — AFTER
async def __aenter__(self):
    await self.broker.connect()
    try:
        await self.backend.connect()
    except BaseException:
        await self.broker.close()
        raise
    return self
```

### P1 — `get_running_loop()` statt `get_event_loop()`

```python
# client.py — AFTER
deadline = asyncio.get_running_loop().time() + timeout
```

### P2 — Dead parameter entfernt, Guard hinzugefuegt

```python
# serialization.py: decode_result(data: bytes) — type_hint entfernt
# registry.py: get_entry() prüft jetzt auf Existenz → TaskNotFoundError
```

## Why This Works

**P0:** aio-pika's consume dispatch ist eine Single-Coroutine-Schleife die `await callback(message)` aufruft. `asyncio.create_task()` scheduled den Handler als unabhängigen Task auf dem Event-Loop; der Callback returned `None` sofort, aio-pika fährt mit dem nächsten Message fort. Die Semaphore throttled die Anzahl paralleler Tasks ohne den Dispatch-Loop zu berühren.

**P1 (Valkey wrapping):** valkey-py wirft `ValkeyError` (und Subklassen) für Protokoll-/Transportfehler und `OSError` für Socket-Fehler. Ohne Wrapping wurde der strukturierte Error-Pfad des Workers (`BackendConnectionError` -> nack mit requeue) komplett umgangen.

**P1 (Connect guards):** Idempotentes `connect()` ist essentiell weil (1) Tests setup/teardown mehrfach aufrufen können und (2) Reconnect-Logik `connect()` defensiv aufrufen können muss.

**P1 (`get_running_loop`):** `get_event_loop()` ist in async Code mehrdeutig — `get_running_loop()` gibt explizit den Loop zurück auf dem die aktuelle Coroutine läuft.

## Prevention

**Kernregel für async Library-Callbacks:** Jeder Callback der von einer event-driven Library ge-`await`-et wird, muss sofort returnen. Echte Arbeit in `asyncio.create_task()` auslagern. Nie eine Semaphore oder I/O im Callback-Frame halten.

**Checkliste für async Python Services:**

- Consume-Callbacks sind Dispatch-Hooks — Task feuern, returnen
- Jede `connect()` Methode muss idempotent sein — prüfe auf existierende Connection
- Jede in `__aenter__` geöffnete Resource muss bei partiellem Failure geschlossen werden — `try/except BaseException: cleanup; raise`
- Library-Exceptions an der Boundary wrappen — I/O Methoden dürfen nie library-spezifische Exceptions nach außen lassen
- In Coroutines immer `asyncio.get_running_loop()` verwenden, nie `get_event_loop()`
- Public API Signaturen sind Verträge — ungenutzte Parameter entfernen
- Domain Exception Hierarchien nur nützlich wenn jeder Code-Pfad sie wirft

## Related Issues

- Plan: `docs/plans/2026-04-15-001-feat-phase0-phase1-roundtrip-plan.md`
- Fix-Plan: `docs/plans/2026-04-15-002-fix-review-findings-plan.md`
- Recherche: `docs/recherche-aio-pika-rabbitmq.md` (aio-pika Consumer-Patterns)
- Recherche: `docs/recherche-valkey-py.md` (valkey-py Async-Client)
