---
title: "fix: Review-Findings P0+P1 aus Phase 0+1 Code Review"
type: fix
status: active
date: 2026-04-15
---

# fix: Review-Findings P0+P1 aus Phase 0+1 Code Review

## Overview

Das Code Review der Phase 0+1 Implementierung hat 1 P0 und 5 P1 Findings identifiziert, die vor dem Merge auf `main` behoben werden müssen. Zusätzlich werden 2 P2-Findings mitbehoben, die trivial und risikoarm sind.

## Problem Frame

Die Phase 0+1 Implementierung ist funktional korrekt (178 Tests grün, E2E-Roundtrip funktioniert), hat aber strukturelle Probleme bei Error-Handling, Resource-Management und Async-Patterns, die in Produktion zu Deadlocks, Resource-Leaks und stillen Fehlern führen würden.

## Requirements Trace

- F1. P0: Worker-Semaphore darf aio-pika Consume-Callback nicht blockieren
- F2. P1: ValkeyBackend-Methoden müssen Errors in BackendConnectionError wrappen
- F3. P1: Broker/Backend.connect() muss idempotent sein (Connect-Guard)
- F4. P1: app.__aenter__ muss Broker bei partiellem Connect-Failure schließen
- F5. P1: asyncio.get_event_loop() durch get_running_loop() ersetzen
- F6. P2: decode_result unused type_hint Parameter entfernen
- F7. P2: registry.get_entry() muss TaskNotFoundError werfen statt KeyError

## Scope Boundaries

- Nur P0+P1 Findings plus 2 triviale P2-Fixes
- Kein neues Feature, kein Retry-System, kein Graceful Shutdown
- P2-Findings wie Infinite-Requeue-Storm, post_execution-Result-Discard sind dokumentierte Phase-1-Limitationen und werden in Phase 2 adressiert

## Key Technical Decisions

- **asyncio.create_task() für Worker-Message-Handling:** Der Callback gibt sofort zurück, die eigentliche Arbeit läuft in einem gespawnten Task. Das Semaphore wird im gespawnten Task acquired, nicht im Callback. Dies ist das Standard-Pattern für aio-pika Consumer mit Concurrency-Control.
- **Connect-Guard statt Worker-Restructure:** Einfacher `if self._connection is not None: return` Guard in `connect()` statt Restructure der Worker/App-Beziehung. Minimaler Eingriff, löst das Double-Connect-Problem vollständig.

## Implementation Units

- [x] **Unit 1: Worker Semaphore-Deadlock beheben (P0)**

**Goal:** Worker._on_message darf nicht direkt im aio-pika Callback blockieren.

**Requirements:** F1

**Dependencies:** Keine

**Files:**
- Modify: `src/swarmq/worker.py`
- Test: `tests/unit/test_worker.py`

**Approach:**
- `_on_message(message)` wird zum dünnen Dispatcher: `asyncio.create_task(self._handle_message(message))`
- Neues `_handle_message(message)`: enthält die gesamte bisherige Logik (Deserialisierung, Semaphore, Task-Ausführung, Result-Storage, Ack/Nack)
- Semaphore wird in `_handle_message` acquired, nicht in `_on_message`
- Bestehende Tests müssen angepasst werden (Callback ist jetzt `_on_message`, aber die Logik läuft in `_handle_message`)

**Execution note:** Test-first — schreibe zuerst einen Test der verifiziert, dass _on_message sofort returned und die Arbeit asynchron passiert.

**Test scenarios:**
- Happy path: _on_message returned sofort, Message wird asynchron verarbeitet
- Happy path: Concurrency-Semaphore funktioniert weiterhin korrekt
- Edge case: Alle bestehenden Worker-Tests bestehen weiterhin

**Verification:**
- _on_message blockiert nicht den aio-pika Dispatch-Loop
- Alle 178 bestehenden Tests grün

---

- [x] **Unit 2: ValkeyBackend Error-Wrapping (P1)**

**Goal:** Alle I/O-Methoden in ValkeyBackend wrappen Valkey-Exceptions in BackendConnectionError.

**Requirements:** F2

**Dependencies:** Keine

**Files:**
- Modify: `src/swarmq/backend/valkey.py`
- Test: `tests/unit/backend/test_valkey.py`

**Approach:**
- `store_result()`, `get_result()`, `delete_result()` bekommen try/except das `ValkeyError` und `OSError` in `BackendConnectionError` wrapped
- Import `from valkey.exceptions import ValkeyError` als Basis-Exception für alle Valkey-Fehler
- Worker's `except BackendConnectionError` greift jetzt korrekt

**Test scenarios:**
- Error path: store_result() mit Valkey-Fehler → BackendConnectionError
- Error path: get_result() mit Valkey-Fehler → BackendConnectionError
- Error path: delete_result() mit Valkey-Fehler → BackendConnectionError

**Verification:**
- Worker's BackendConnectionError-Handler fängt Valkey-Fehler korrekt

---

- [x] **Unit 3: Connect-Guard für Broker und Backend (P1)**

**Goal:** `connect()` ist idempotent — mehrfaches Aufrufen leakt keine Connections.

**Requirements:** F3

**Dependencies:** Keine

**Files:**
- Modify: `src/swarmq/broker/rabbitmq.py`
- Modify: `src/swarmq/backend/valkey.py`
- Test: `tests/unit/broker/test_rabbitmq.py`
- Test: `tests/unit/backend/test_valkey.py`

**Approach:**
- `RabbitMQBroker.connect()`: `if self._connection is not None: return` am Anfang
- `ValkeyBackend.connect()`: `if self._client is not None: return` am Anfang
- Löst das Double-Connect-Problem von Worker.start() + __aenter__

**Test scenarios:**
- Edge case: Broker.connect() zweimal aufrufen → zweiter Aufruf ist no-op, eine Connection
- Edge case: Backend.connect() zweimal aufrufen → zweiter Aufruf ist no-op, ein Client

**Verification:**
- Worker.start() nach __aenter__ erzeugt keine doppelten Connections

---

- [x] **Unit 4: __aenter__ Cleanup bei partiellem Connect (P1)**

**Goal:** Wenn backend.connect() in __aenter__ fehlschlägt, wird die bereits geöffnete Broker-Connection geschlossen.

**Requirements:** F4

**Dependencies:** Keine

**Files:**
- Modify: `src/swarmq/app.py`
- Test: `tests/unit/test_app.py`

**Approach:**
- `__aenter__`: try/except um backend.connect() — im except: await self.broker.close(), dann re-raise

**Test scenarios:**
- Error path: backend.connect() raised → broker.close() wird aufgerufen, Exception propagiert
- Happy path: Beide connect erfolgreich → normaler Flow

**Verification:**
- Kein Resource-Leak bei partiellem Connect-Failure

---

- [x] **Unit 5: get_event_loop → get_running_loop (P1)**

**Goal:** Deprecated asyncio.get_event_loop() durch get_running_loop() ersetzen.

**Requirements:** F5

**Dependencies:** Keine

**Files:**
- Modify: `src/swarmq/client.py`

**Approach:**
- Zeile 86 und 91: `asyncio.get_event_loop().time()` → `asyncio.get_running_loop().time()`
- Triviale Änderung, keine neuen Tests nötig (bestehende Tests decken die Funktionalität ab)

**Test expectation: none -- triviale API-Ersetzung, bestehende Tests verifizieren die Funktionalität**

**Verification:**
- Keine DeprecationWarning mehr in Python 3.14
- Alle bestehenden Client-Tests grün

---

- [x] **Unit 6: decode_result type_hint entfernen + registry.get_entry() Guard (P2)**

**Goal:** Dead Parameter entfernen und fehlenden Error-Guard hinzufügen.

**Requirements:** F6, F7

**Dependencies:** Keine

**Files:**
- Modify: `src/swarmq/serialization.py`
- Modify: `src/swarmq/registry.py`
- Test: `tests/unit/test_registry.py`

**Approach:**
- `decode_result(data, type_hint=None)` → `decode_result(data)` (type_hint entfernen)
- Alle Aufrufer von decode_result anpassen (client.py)
- `registry.get_entry()`: Guard hinzufügen — `if name not in self._entries: raise TaskNotFoundError(name)`

**Test scenarios:**
- Error path: registry.get_entry("unknown") → TaskNotFoundError

**Verification:**
- Kein toter Parameter in der Public API
- get_entry() hat konsistentes Error-Handling wie get()

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Worker-Refactor (Unit 1) bricht bestehende Tests | Tests werden als Teil des Fixes angepasst. Alle 178 Tests müssen grün bleiben. |
| ValkeyBackend Error-Wrapping fängt zu breit | Nur ValkeyError + OSError wrappen, nicht alle Exceptions. |

## Sources & References

- **Code Review:** Findings aus ce:review Run vom 2026-04-15
- **Betroffener Code:** `src/swarmq/worker.py`, `src/swarmq/backend/valkey.py`, `src/swarmq/broker/rabbitmq.py`, `src/swarmq/app.py`, `src/swarmq/client.py`, `src/swarmq/serialization.py`, `src/swarmq/registry.py`
