---
title: "fix: Review-Findings aus Phase 3a Code Review"
type: fix
status: active
date: 2026-04-16
---

# fix: Review-Findings aus Phase 3a Code Review

## Overview

Das Code Review der Phase 3a (Middleware + Signals) hat 2 P1 und 6 P2 Findings identifiziert. Die 2 P1-Findings sind echte Bugs (Middleware-Snapshot im Client, Watchdog-Leak), die P2-Findings sind Schema-Inkonsistenzen und Code-Qualität.

## Problem Frame

Phase 3a ist funktional korrekt (370 Tests grün), aber das Review hat strukturelle Probleme gefunden: (1) Der Client bekommt einen gefrorenen MiddlewareChain-Snapshot bei `__init__`, sodass `add_middleware()` nach SwarmQ-Konstruktion für den Client wirkungslos ist. (2) Der Hard-Timeout-Watchdog wird bei Task-Exception nicht gecancelt. (3) message_dict Schema ist zwischen Worker und Client inkonsistent.

## Requirements Trace

- F1. P1: Client greift dynamisch auf middleware_chain zu (oder teilt mutable Referenz mit App)
- F2. P1: Watchdog wird bei ALLEN Task-Pfaden (Exception + Success) gecancelt
- F3. P1: `SwarmQ.on_signal` hat korrekte Return-Type-Annotation
- F4. P2: message_dict Schema im Client um `delivery_mode` erweitern (retry_count bleibt worker-only)
- F5. P2: `message_dict['retry_count']` wird vor `on_retry` auf retry_count+1 aktualisiert
- F6. P2: Middleware-Hook Signatur nutzt TypedDict für message (statt bare dict)
- F7. P2: `_evaluate_retry` bekommt Type-Annotations für message und task_info
- F8. P2: Dead Import `encode_result` in `_send_to_dlq` entfernen
- F9. P3: `publish_notification` bare except durch Warning-Log ersetzen

## Scope Boundaries

- Keine API-Änderungen an Public Middleware/Signal-Interface
- Keine neuen Features — nur Bug-Fixes und Code-Qualität
- AsyncHandler Protocol-Refactor (F6 Kieran-Python Finding) verschoben: zu invasiv, kein echter Bug

### Deferred to Separate Tasks

- SignalDispatcher `disconnect()` / `clear()` Methode: separates Task bei Bedarf
- schedule_many partial-failure API: design-decision verschoben
- SignalDispatcher BaseException Handling: residuelles Risiko, separates Hardening

## Context & Research

### Relevant Code and Patterns

- `src/swarmq/app.py` — Middleware-Snapshot-Bug (F1), on_signal Annotation (F3)
- `src/swarmq/worker.py` — Watchdog-Leak (F2), message_dict retry_count (F5), Type-Hints (F7), Dead Import (F8), publish_notification (F9)
- `src/swarmq/client.py` — message_dict Schema (F4)
- `src/swarmq/middleware.py` — TypedDict für Hook-Signatur (F6)

### Institutional Learnings

- `docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md` — Async-Cleanup in try/finally Pattern. Relevant für F2 (Watchdog-Cancel im äußeren finally).

## Key Technical Decisions

- **Client greift via App auf middleware_chain zu, nicht via Snapshot:** Einfachste Lösung für F1. Client speichert Referenz auf SwarmQ-Instanz (oder auf die live `_middlewares` Liste). Alternative: Client rebuilden nach `add_middleware()` — aber das verletzt das Single-Construction-Prinzip.
- **Watchdog-Cancel in äußerem try/finally:** F2. Watchdog-Referenz wird ausserhalb des run_task try/except gespeichert, Cancel im äußeren finally garantiert Cleanup bei allen Pfaden (Erfolg, Exception, Soft-Timeout, Hard-Timeout).
- **MessageContext als TypedDict (nicht Dataclass):** F6. TypedDict ist leichtgewichtiger, kompatibel mit bestehendem `dict`-API. Keine Breaking Change für Middleware-Autoren die bereits `message['task_id']` nutzen.
- **retry_count Update vor on_retry:** F5. message_dict wird vor dem on_retry Aufruf kopiert/aktualisiert. Parameter und dict stimmen überein.

## Open Questions

### Resolved During Planning

- **Client ↔ App Kopplung:** Einfachste Option — Client bekommt `middleware_chain` als Callable/Property statt als Snapshot. Kein zirkulärer Import, da Client nur Python-Closures nutzt.

### Deferred to Implementation

- Exakter MessageContext TypedDict-Name: könnte `MessageContext`, `MiddlewareMessage`, oder `TaskMessage` heißen. Trivial bei Implementation.

## Implementation Units

- [ ] **Unit 1: Middleware-Chain Live-Zugriff im Client (P1 F1)**

**Goal:** Client ruft die aktuelle Middleware-Chain dynamisch ab, statt einen Snapshot bei `__init__` zu halten.

**Requirements:** F1

**Dependencies:** Keine

**Files:**
- Modify: `src/swarmq/client.py`
- Modify: `src/swarmq/app.py`
- Test: `tests/unit/test_app.py`
- Test: `tests/unit/test_client.py`

**Approach:**
- Client akzeptiert `middleware_chain_provider: Callable[[], MiddlewareChain] | None = None` ODER eine Live-Referenz auf `app._middlewares` list
- Einfachster Ansatz: Client speichert `middleware_chain_factory` Callable. In `schedule()`/`schedule_many()` wird die Chain on-demand gebaut: `chain = self._middleware_chain_factory()`
- SwarmQ übergibt beim Client-Bau: `middleware_chain_factory=lambda: self.middleware_chain`
- Rückwärtskompatibilität: Wenn eine MiddlewareChain-Instanz direkt übergeben wird, wird sie in einen konstanten Factory gewickelt
- Alternativ einfacher: Client bekommt Reference auf die Live-Liste, baut Chain on-demand selbst

**Test scenarios:**
- Happy path: `app.add_middleware(mw)` nach `SwarmQ()` → nächster `app.schedule()` ruft mw.pre_enqueue auf
- Happy path: Middleware vor `SwarmQ()` registriert → funktioniert unverändert
- Edge case: Mehrere `add_middleware()` Calls → alle werden bei nächstem schedule() angewendet
- Integration: Register middleware, schedule, verify pre_enqueue + post_enqueue fired

**Verification:**
- Der Test `add_middleware after SwarmQ construction` ruft die Middleware wirklich auf (aktuell ein stiller Bug)

---

- [ ] **Unit 2: Watchdog-Cancel in äußerem try/finally (P1 F2)**

**Goal:** Hard-Timeout-Watchdog wird bei ALLEN Task-Ausgangswegen (Erfolg, Exception, Timeout, Cancel) gecancelt.

**Requirements:** F2

**Dependencies:** Keine

**Files:**
- Modify: `src/swarmq/worker.py`
- Test: `tests/unit/test_worker_timeouts.py`

**Approach:**
- Watchdog-Task-Referenz wird VOR dem try/except initialisiert (oder in einem äußeren try/finally wrapping)
- Aktueller Code: `watchdog.cancel()` im `finally` des `else`-Branchs — läuft nur bei Erfolg
- Fix: Watchdog-Variable außerhalb des try-Blocks, äußeres try/finally um die gesamte Execution, Cancel im äußeren finally
- Cancel ist idempotent (bereits abgeschlossene Tasks sind cancel-safe)

**Test scenarios:**
- Happy path: Task erfolgreich → Watchdog gecancelt (bestehend)
- Error path: Task wirft ValueError → Watchdog GECANCELT (aktuell nicht gecancelt — Bug)
- Error path: Task wirft asyncio.TimeoutError (soft_timeout) → Watchdog gecancelt
- Error path: Task wirft asyncio.CancelledError (hard_timeout oder shutdown) → Watchdog war selbst der Canceller, neuer Cancel ist no-op
- Edge case: Watchdog-Cancel auf bereits completed Task → no-op (kein Fehler)

**Verification:**
- Kein Watchdog-Leak bei Task-Exceptions
- `asyncio.all_tasks()` zeigt keine offenen Watchdog-Tasks nach Task-Abschluss

---

- [ ] **Unit 3: on_signal Return Type + message_dict Schema Fixes (P1 F3, P2 F4, F5)**

**Goal:** Type-Annotation korrigieren, Schema-Inkonsistenzen zwischen Client/Worker message_dict beheben.

**Requirements:** F3, F4, F5

**Dependencies:** Keine

**Files:**
- Modify: `src/swarmq/app.py` (F3 on_signal Annotation)
- Modify: `src/swarmq/client.py` (F4 delivery_mode in message_dict)
- Modify: `src/swarmq/worker.py` (F5 retry_count Update vor on_retry)
- Test: `tests/unit/test_client.py` (message_dict enthält delivery_mode)
- Test: `tests/unit/test_worker.py` (message_dict retry_count ist konsistent mit Parameter)

**Approach:**
- **F3:** `on_signal` Return-Type: `Callable[[AsyncHandler], AsyncHandler]`. Import AsyncHandler aus signals.py.
- **F4:** Client's message_dict bekommt `"delivery_mode": "at_least_once"` als Default (da Client Delivery Mode erst beim Enqueue noch nicht kennt — es steht in der Task). Alternative: `delivery_mode` aus Registry auslesen. Einfachster Ansatz: vom Registry holen wenn vorhanden, sonst `"at_least_once"`.
- **F5:** In `_evaluate_retry` vor dem `run_on_retry` Aufruf: `message_dict["retry_count"] = retry_count + 1`. Dann sind dict und Parameter synchron.

**Test scenarios:**
- Happy path F3: `help(app.on_signal)` zeigt korrekten Return-Type (mypy kompatibel)
- Happy path F4: `middleware.pre_enqueue` liest `message['delivery_mode']` ohne KeyError
- Happy path F5: `middleware.on_retry` sieht konsistenten retry_count in message vs Parameter
- Edge case F4: Client ohne Registry → delivery_mode Default ist korrekt

**Verification:**
- Keine KeyError mehr bei `message['delivery_mode']` im Client
- message_dict retry_count stimmt mit on_retry Parameter überein

---

- [ ] **Unit 4: MessageContext TypedDict + Type-Cleanup (P2 F6, F7, F8)**

**Goal:** Middleware-Hook-Signaturen nutzen TypedDict, `_evaluate_retry` bekommt Type-Annotations, Dead Import entfernen.

**Requirements:** F6, F7, F8

**Dependencies:** Keine (F4 sollte vorher passieren damit das Schema stabil ist)

**Files:**
- Modify: `src/swarmq/middleware.py` (TypedDict MessageContext)
- Modify: `src/swarmq/worker.py` (Type-Hints, Dead Import entfernen)

**Approach:**
- **F6:** Neuer TypedDict `MessageContext` in middleware.py:
  ```
  class MessageContext(TypedDict):
      task_id: str
      task_name: str
      queue: str
      args: list
      kwargs: dict
      delivery_mode: str
      retry_count: NotRequired[int]  # nur worker-seitig
  ```
  Hook-Signaturen: `message: MessageContext` statt `message: dict`. Rückwärtskompatibel da TypedDict ein dict IST.
- **F7:** `_evaluate_retry(self, message: AbstractIncomingMessage, task_info: TaskInfo, ...)` mit expliziten Typen
- **F8:** `encode_result` Import oben im worker.py ist bereits vorhanden — doppelten Import in `_send_to_dlq` entfernen

**Test scenarios:**
- Happy path F6: mypy strict läuft ohne neue Errors auf middleware.py
- Happy path F6: Bestehender Middleware-Code (der `message['task_id']` nutzt) funktioniert unverändert
- Happy path F7: mypy strict läuft ohne neue Errors auf worker.py _evaluate_retry

**Test expectation: none -- reine Type-Annotation und Dead-Code-Entfernung, bestehende Tests verifizieren die Funktionalität**

**Verification:**
- `mypy src/` läuft ohne neue Errors
- Bestehende 370 Tests laufen unverändert

---

- [ ] **Unit 5: publish_notification Logging (P3 F9)**

**Goal:** Bare `except Exception: pass` durch Warning-Log ersetzen für Operator-Sichtbarkeit.

**Requirements:** F9

**Dependencies:** Keine

**Files:**
- Modify: `src/swarmq/worker.py`

**Approach:**
- Im try/except um `publish_notification` in `_handle_message`: `except Exception as exc: logger.warning("publish_notification_failed", error=str(exc))`
- Kein Re-raise — Notification-Fehler dürfen den Worker-Flow nicht abbrechen

**Test expectation: none -- reine Logging-Ergänzung, bestehende Tests verifizieren die Funktionalität**

**Verification:**
- publish_notification Fehler sind in Logs sichtbar
- Worker-Flow wird nicht unterbrochen

## System-Wide Impact

- **Interaction graph:** Keine neuen Integrationen. Bestehende Client → Middleware Pfad wird dynamisch statt statisch.
- **Error propagation:** Unverändert. Watchdog-Cleanup wird robuster.
- **State lifecycle risks:** Watchdog-Leak (F2) wird behoben. Keine neuen Risiken.
- **API surface parity:** `message: dict` → `message: MessageContext` ist rückwärtskompatibel (TypedDict IS-A dict). Keine Breaking Change.
- **Unchanged invariants:** Middleware-Stack-Semantik, Signal-Dispatch-Reihenfolge, Retry/DLQ-Logik — alles unverändert.

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Client mit Live-Chain-Zugriff ist langsamer (Chain-Rebuild pro Call) | Chain-Build kostet <1μs. Bei Bedarf: Cache-Invalidation bei add_middleware. |
| TypedDict vs dict breaking für externe Middleware | TypedDict IS-A dict in Python typing. Bestehender dict-Access funktioniert. |
| F2 Watchdog-Refactor bricht bestehende Tests | Watchdog-Tests prüfen nur Verhalten, nicht Implementierung. Sollte transparent sein. |

## Sources & References

- **Code Review:** Findings aus ce:review Run vom 2026-04-16 (Kieran-Python + Correctness)
- **Phase 3a Plan:** [docs/plans/2026-04-16-001-feat-phase3a-middleware-signals-plan.md](docs/plans/2026-04-16-001-feat-phase3a-middleware-signals-plan.md)
- **Betroffener Code:** `src/swarmq/worker.py`, `src/swarmq/client.py`, `src/swarmq/app.py`, `src/swarmq/middleware.py`
