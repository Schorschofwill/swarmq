---
title: "feat: Workflow-Primitive (chain / group / chord + Signatures)"
type: feat
status: active
date: 2026-05-30
origin: docs/brainstorms/2026-05-30-workflow-primitives-requirements.md
---

# feat: Workflow-Primitive (chain / group / chord + Signatures)

## Summary

Net-new Workflow-Subsystem für SwarmQ: ein serialisierbares `Signature`-Objekt als
Baustein, die Primitive `chain` / `group` / `chord` mit Operator-Syntax (`|`), ein
fire-and-forget `.apply()` mit reattach-barem Handle, und worker-getriebene
Orchestrierung mit atomarer+idempotenter Chord-Completion über ein neues Lua-Script.
Der Plan setzt am bestehenden Worker-Completion-Pfad an (Advancement *vor* dem
AT_LEAST_ONCE-Ack) und an einem neuen `sq:wf:*`-Valkey-Namespace, ohne Refactoring
der bestehenden Task/Client/Worker-Kernpfade.

---

## Problem Frame

Jede Komposition mehrerer Tasks muss heute der Anwender selbst verdrahten (Ergebnis
weiterreichen, Fan-in zählen, Abbruch, Reattach) — genau dort entstehen subtile
Fehler. Workflow-Primitive schließen die auffälligste funktionale Lücke gegenüber
Celery/Dramatiq. Vollständiger Problemkontext, Aktoren und Flows: siehe
`docs/brainstorms/2026-05-30-workflow-primitives-requirements.md`.

---

## Requirements

- R1. `Signature` kapselt `(task_name, args, kwargs, options)` serialisierbar. *(see origin: R1)*
- R2. Zwei Erzeugungswege: `Task.s()` (type-safe) + `sig("name")` (string-basiert). *(R2)*
- R3. Immutable Signature `Task.si()` / `sig(..., immutable=True)` — kein Result-Inject. *(R3)*
- R4. `chain(*signatures)` — sequenziell, Ergebnis → erstes Arg des nächsten (mutable) Schritts. *(R4)*
- R5. `group(*signatures | iterable)` — parallel, geordnete Ergebnis-Liste. *(R5)*
- R6. `chord(header_group, body)` — Body genau einmal nach allen Headern; Member-Result-Lebensdauer ≥ Workflow, expired-vs-missing unterschieden. *(R6)*
- R7. Operator-Syntax: `A.s() | B.s()` = chain; `group(...) | body` = chord. *(R7)*
- R8. Flache Komponierbarkeit (DAG); keine beliebig tiefe Rekursion. *(R8)*
- R9. Safety-Limits `max_workflow_depth=10` / `max_group_size=1000`; Konstruktions-Validierung + Runtime-Tiefen-Guard für `sig()`-Expansion. *(R9)*
- R10. `.apply()` fire-and-forget: Topologie in Valkey persistiert, initial lauffähige Schritte enqueued, sofortiges Handle. *(R10)*
- R11. Worker treiben den Workflow voran; kein separater Orchestrator; Schritte laufen `AT_LEAST_ONCE` + idempotenter `next-enqueued`-Marker. *(R11)*
- R12. Chord-Completion atomar + idempotent gegen Redelivery (Completed-Set + One-Shot `body-enqueued`-Flag). *(R12)*
- R13. Handle: `workflow_id`, `await get(timeout=...)`, `status`, `cancel()`, `children`. *(R13)*
- R14. Reattach `client.workflow(workflow_id)`; `WorkflowNotFound` bei abgelaufenem/unbekanntem State. *(R14)*
- R15. Group-Member-Introspektion (`children`) mit Per-Member-Status/-Ergebnis. *(R15)*
- R16. Fail-fast inkl. Liveness: terminaler Fehler markiert Workflow synchron failed + weckt `get()`. *(R16)*
- R17. `handle.cancel()` setzt workflow-level Cancel-Flag, atomar beim Enqueue-next-Entscheid geprüft. *(R17)*

**Origin actors:** A1 (Producer/Client), A2 (Worker), A3 (Valkey-Backend)
**Origin flows:** F1 (Chain), F2 (Group), F3 (Chord), F4 (Fehler/Abbruch)
**Origin acceptance examples:** AE1–AE12 (siehe origin; AE-Links sind in den Unit-Test-Szenarien gesetzt)

---

## Scope Boundaries

- `on_error` / `link_error`-Callbacks: nicht in v1 (Fail-fast deckt v1 ab). *(origin)*
- Voll-rekursive Verschachtelung (chord-of-chord, group-in-group-in-group): bewusst ausgeschlossen. *(origin)*
- `starmap` / `chunks` / `map`-Primitive: nicht in v1. *(origin)*
- Bulk-Throughput-Optimierung (R4.2): eigenständiges Feature. *(origin)*

### Deferred to Follow-Up Work

- **Large-Result-Weitergabe via Valkey-Referenz:** v1 reicht Ergebnisse inline (Arg-Injektion am Advancement-Seam) weiter. Die Optimierung, sehr große Zwischenergebnisse als Valkey-Referenz statt inline durchzureichen (um Message-Size-Limits zu vermeiden), wird auf eine Folge-Iteration verschoben. *(Plan-Entscheidung, vom Nutzer bestätigt 2026-05-30.)*

---

## Context & Research

### Relevant Code and Patterns

- **Task/Signature-Surface:** `Task` ist class-based (`src/swarmq/task.py:267`); `.s()`/`.si()` werden Classmethods, die `cls.task_info.name` auflösen. Args/kwargs fließen verbatim aus dem JSON-Message-Body in `task_class().run(*args, **kwargs)` — **kein zentraler Arg-Binder**, daher Result-Injektion am Advancement-Seam durch Mutation der `args` des nächsten Schritts vor `broker.publish`.
- **Message/Publish:** `Client.schedule` (`src/swarmq/client.py:257`) + `schedule_many` (`:648`), `uuid.uuid7()`-IDs, `encode_json`-Message `{"task": {...}, "args": [...], "kwargs": {...}}`. Delayed/DLQ-Publish vorhanden (`src/swarmq/broker/rabbitmq.py:260,275`).
- **Worker-Completion-Seam:** `_handle_message` (`src/swarmq/worker.py:1352`); Advancement attacht **nach** `store_result`+`publish_notification` (`:1852-1883`) und **vor** dem AT_LEAST_ONCE-Ack (`:1883`). Terminal-Failure-Seam: `_send_to_dlq` (`:2114`). Collaborator-Wiring (RateLimiter/LockManager) in `start()` (`:727`) — ChordManager dort instanziieren.
- **Backend:** `ValkeyBackend` (`src/swarmq/backend/valkey.py`) — `_key`-Namespace-Konvention (`sq:<ns>:{id}`), `store_result`/`get_results_batch`/`publish_notification`/`subscribe_and_wait`, `set_cancel_flag`/`is_cancelled` (`SET NX PX`/`EXISTS`). TTL aus `SWARMQ_RESULT_TTL` (`config.py`).
- **Lua-Konvention:** `load_script(name)` + `client.register_script(...)` → `AsyncScript`; Header-Kommentar mit `KEYS`/`ARGV`/Atomicity-Note; `now_ms` server-seitig per `TIME`. Vorbilder: `sliding_window_counter.lua`, `sched_commit.lua`, `lock_acquire.lua`. Integration-Tests: `test_lock_lua_scripts.py` etc.
- **Serialisierung:** `encode_json`/`decode_json` (msgspec) für Topologie/State; nur JSON-native Primitive — keine Task-*Klassen* oder Callables im Payload. `src/swarmq/serialization.py`.
- **Exports:** PEP-562 lazy `__getattr__` in `src/swarmq/__init__.py`; leichtgewichtiges `swarmq/workflow.py` kann eager importiert + in `__all__` ergänzt werden.

### Institutional Learnings

- **Ack-Ordering / FAF-Trap** (`docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`): Jede schicksalsentscheidende Stage MUSS vor dem FAF-Ack laufen; publish-then-ack in `_evaluate_retry`-förmigem try/except, transient → `nack(requeue=True)`. **Workflow-Schritte daher AT_LEAST_ONCE, nie FIRE_AND_FORGET.**
- **R3.5 atomic-claim Patterns** (`docs/solutions/patterns/r3-5-unique-tasks-implementation-learnings-2026-04-17.md`): owner-checked Lua statt read-modify-write; shared-state-Mutation NACH Ack; Claim-Rollback bei nachgelagertem Fehler (Template für group-batch-publish); Ceiling-Division für Sub-Sekunden-TTL; Header als untrusted input vor Key-Nutzung validieren (workflow-id-Header → 64-hex-Check-Analog).
- **DLX-Routing** (`docs/solutions/logic-errors/nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md`): permanenter Fehler (Body-Fail) → `requeue=False` (→ `.XQ`), transient (Valkey-Blip) → `requeue=True`; Log-Wording „routed via DLX", nie „dropped".
- **BackendConnectionError-Boundary** (`docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md`): jede neue Backend-Methode wrappt `(ValkeyError, OSError)` → `BackendConnectionError`, sonst entkommt der Counter-Fehler dem Worker-Handler.
- **Pub/Sub-Reconnect** (`docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md`): Valkey-Pub/Sub reconnected NICHT automatisch — Result-Waiting braucht explizites Re-Subscribe + Result-Key-Fallback-Poll (vorhandenes `get_result` macht das bereits). AMQP-Redelivery hält unter Chaos (`msg.redelivered` als Test-Signal nutzbar).

### External References

- `docs/recherche-workflows-cancel-progress.md` — Celery/Dramatiq chain/group/chord-Mechanik + Chord-Completion-Detection (Redis-Counter-Muster). `docs/recherche-deepening-lua-scripts.md` Abschnitt 7 (Chord-Lua). `aufgabe.md` Abschnitt 6.6.

---

## Key Technical Decisions

- **Topologie als plain nested dict, `encode_json`-serialisiert:** `Signature.to_dict()`/`from_dict()`; keine Klassen/Callables im Payload (msgspec-/Wire-kompatibel, reattach-fähig).
- **Result-Injektion am Advancement-Seam:** da kein zentraler Arg-Binder existiert, mutiert der Worker die `args` des nächsten Schritts (Prepend des Vorgänger-Results) vor `broker.publish`; immutable `.si()` unterdrückt das Prepend. Inline-only für v1 (siehe Deferred to Follow-Up Work).
- **Chord-Completion via owner-checked Lua (`chord_complete.lua`):** `SADD` der Member-Task-ID in ein Completed-Set + Kardinalitäts-Check gegen N + One-Shot `body-enqueued`-Flag, alles in einem `EVAL`. Redelivery eines bereits gezählten Members ist No-op; genau ein Worker enqueued den Body. Manager wie RateLimiter (in `Worker.start()` registriert).
- **Advancement vor Ack, AT_LEAST_ONCE, publish-then-mark:** Advancement-Stage läuft nach `store_result` und vor dem Ack. **Reihenfolge: erst publishen, dann `next-enqueued`-Marker setzen** — nicht umgekehrt. Das `await broker.publish(next)` läuft über aio_pikas Default-Channel mit `publisher_confirms=True` (`Connection.channel(publisher_confirms=True)`), d. h. das awaited Publish kehrt erst nach Broker-Bestätigung zurück — ein zurückgekehrtes Publish == broker-bestätigtes Enqueue (kein eigener `confirm=`-Parameter nötig; `RabbitMQBroker.publish` hat keinen). Marker-vor-Publish würde at-least-once in at-most-once verkehren (Crash zwischen Marker und Publish ⇒ Redelivery sieht „schon enqueued" und überspringt den Schritt ⇒ Kette hängt dauerhaft). Publish-then-mark akzeptiert im seltenen Crash-Fenster (Publish bestätigt, Marker noch nicht gesetzt, Redelivery) ein **Doppel-Enqueue des Nachfolgers**, das durch die ohnehin geforderte Schritt-Idempotenz (AT_LEAST_ONCE) absorbiert wird — die sichere Richtung. Folgt Ack-Ordering-Learning. Der Marker ist ein eigener `SET NX`-Key (atomar, kein read-modify-write auf dem State-JSON).
- **Workflow-State + Cancel-Flag im `sq:wf:*`-Namespace:** `sq:wf:{id}` (JSON-State/Topologie/Status), `sq:wf:chord:{id}` (Completed-Set/Flag), `sq:wf:cancel:{id}` (`SET NX PX`/`EXISTS` wie bestehende Cancel-Flags). TTL = `config.result_ttl` (Ceiling-Division), Member-Results refreshed auf ≥ Workflow-Lebensdauer.
- **Fail-fast-Liveness am `_send_to_dlq`-Seam:** terminaler Fehler markiert `sq:wf:{id}` synchron failed + `publish_notification`, sodass wartende `get()` sofort die Exception bekommen statt in den Timeout zu laufen.
- **Neue Header in `src/swarmq/headers.py`:** `x-swarmq-workflow-id`, `x-swarmq-workflow-step` (Single-Source-of-Truth-Konvention); workflow-id wird am Worker-Ingress validiert, bevor er als Valkey-Key dient.

---

## Open Questions

### Resolved During Planning

- *Chord-Lua-Struktur (origin R12):* `SADD`-Completed-Set + Kardinalitäts-Check + One-Shot-Flag in einem EVAL (siehe Key Technical Decisions / U3).
- *Workflow-State-Schema/TTL (origin R10/R14):* `sq:wf:{id}` JSON, TTL = `config.result_ttl` mit Ceiling-Division; `WorkflowNotFound` bei Absenz (U2/U4).
- *Fail-fast-Rückmeldepfad (origin R16):* synchroner Hook in `_send_to_dlq`, nicht DLQ-Beobachtung (U6).
- *DAG-Validierung (origin R8/R9):* Konstruktionszeit in `workflow.py` + Runtime-Guard am Advancement-Seam für `sig()`-Expansion (U1/U7).
- *Delivery-Mode der Schritte (origin R10/R11):* AT_LEAST_ONCE + idempotenter `next-enqueued`-Marker (U5).

### Deferred to Implementation

- Return-Value-Edge-Cases von `chord_complete.lua` (KEYS/ARGV-Form steht in U3 fest: `KEYS=[completed_set, body_flag]`, `ARGV=[member_id, expected_N, ttl_ms]`) — Feinheiten nach erstem Integration-Test gegen echtes Valkey.
- Genaue Struktur des persistierten State-Dicts (Felder für Per-Step-Status, **geordnete** Member-Task-ID-Liste, terminale Schritt-IDs für `get()`). Minimal-Feld-Kontrakt vor U4-Start pinnen, damit U4 (children) und U6 (failed-Mark) nicht driften.
- Konkrete `WorkflowFailed`-Wrapper-Exception vs. Re-Raise des Original-Task-Exception-Typs an der öffentlichen `get()`-Oberfläche (R16 sagt „ursprüngliche Exception").
- Idempotenz-Kontrakt von `cancel()` auf bereits abgeschlossenem/failed Workflow (silent vs. `WorkflowNotFound` vs. dediziert).
- TTL-Verhältnis der Chord-Keys (`sq:wf:chord:{id}`) zum Workflow-State (`sq:wf:{id}`): Chord-Keys dürfen nicht vor dem State ablaufen, sonst sieht ein reattachtes Handle aktiven Workflow mit unauflösbarem Chord-State.

*(`next-enqueued`-Marker als eigener `SET NX`-Key entschieden — siehe Key Technical Decisions.)*

---

## Output Structure

    src/swarmq/
      workflow.py                       # Signature, chain, group, chord, sig, |-Operator, to_dict/from_dict, DAG-Validierung, WorkflowHandle
      workflow_orchestration.py         # ChordManager (Lua-Wrapper) + Advancement-Helfer
      lua/
        chord_complete.lua              # atomare+idempotente Chord-Completion
    tests/
      unit/test_workflow.py             # Topologie, Signatures, Operator, Validierung
      unit/test_workflow_handle.py      # Handle-Verhalten (gemockte Boundaries)
      property/test_workflow_roundtrip.py  # to_dict/from_dict Hypothesis-Roundtrip
      integration/test_workflow_backend.py # sq:wf:* Backend-Methoden
      integration/test_chord_lua_scripts.py # chord_complete.lua gegen echtes Valkey
      integration/test_workflow_orchestration.py # Advancement-Seam
      e2e/test_workflows.py             # chain/group/chord Roundtrips
      chaos/test_workflow_chord_idempotency.py # exactly-once unter Redelivery/Crash
      e2e/combinations/test_workflow_combinations.py # workflow × cancel/retry/DLQ/FAF

---

## High-Level Technical Design

> *Veranschaulicht die Richtung, ist Review-Guidance — keine Implementierungs-Spezifikation. Der implementierende Agent behandelt sie als Kontext, nicht als zu reproduzierenden Code.*

Worker-Advancement-Seam (vereinfacht, Happy-Path):

```
_handle_message (Erfolg):
  store_result(task_id, ...)        # bestehend
  publish_notification(task_id)     # bestehend
  ── Advancement-Stage (neu, nur wenn Message Workflow-Header trägt) ──
  if is_workflow_step and not is_error and not is_cancelled:
      if backend.is_workflow_cancelled(wf_id):  # R17: workflow-level Gate
          stop (kein Enqueue)
      match step.kind:
        chain:  next = topology.next_after(step)
                if next and not next_already_enqueued(wf_id, next):
                    inject_result_into_args(next, result)      # außer .si() (R3)
                    await broker.publish(next)                 # publisher_confirms=True-Channel:
                                                               #   kehrt erst nach Broker-Bestätigung zurück
                    mark_next_enqueued(wf_id, next)            # DANN markieren (R11)
                    # Crash zwischen publish und mark ⇒ Redelivery ⇒ seltenes
                    # Doppel-Enqueue, von Schritt-Idempotenz absorbiert (nie verloren)
        group-member / chord-header:
                last = chord_complete.lua(EVAL: SADD member; #set == N?; set body-flag once)  # R12
                if last:
                    broker.publish(chord_body, args=collect_member_results())  # R6
  ack(message)                      # AT_LEAST_ONCE, NACH Advancement
```

chord_complete.lua-Entscheidung (atomar):

```
KEYS = [completed_set, body_flag]   ARGV = [member_id, expected_N, ttl_ms]
SADD completed_set member_id
if SCARD completed_set == expected_N and SETNX body_flag 1:
    return 1   # dieser Aufruf feuert den Body, genau einmal
else:
    return 0   # No-op (Duplikat oder noch nicht vollständig)
```

---

## Implementation Units

### U1. Signature- und Workflow-Topologie (chain/group/chord/sig + Operator + Validierung)

**Goal:** Reine, infrastruktur-freie Topologie-Schicht: `Signature`, `chain`/`group`/`chord`, `sig`, `|`-Operator, `to_dict`/`from_dict`, Konstruktions-Validierung der Safety-Limits.

**Requirements:** R1, R2, R3, R4, R5, R6, R7, R8, R9

**Dependencies:** None

**Files:**
- Create: `src/swarmq/workflow.py`
- Modify: `src/swarmq/task.py` (Classmethods `s()` / `si()`), `src/swarmq/__init__.py` (Exports), `src/swarmq/exceptions.py` (`WorkflowValidationError`)
- Test: `tests/unit/test_workflow.py`, `tests/property/test_workflow_roundtrip.py`

**Approach:**
- `Signature` hält `task_name, args, kwargs, options, immutable: bool`. `Task.s(*a, **kw)` / `Task.si(...)` als Classmethods, lösen `cls.task_info.name`. `sig("name", ..., immutable=False)` als freie Funktion.
- `chain`/`group`/`chord` als leichte Container, die eine nested-dict-Topologie aufbauen. `__or__`/`__ror__` auf `Signature` und `group` für `|`-Zucker.
- `to_dict()`/`from_dict()` → JSON-native Struktur (nur Strings/Listen/Dicts; keine Klassen). Validierung von `max_workflow_depth`/`max_group_size` zur Konstruktionszeit (flache-DAG-Regel R8), wirft `WorkflowValidationError`.
- **FAF-Ablehnung:** Eine Signature, deren registrierter Task `DeliveryMode.FIRE_AND_FORGET` deklariert, wird zur Konstruktionszeit mit `WorkflowValidationError` abgelehnt (FAF acked vor Ausführung und erreicht den Advancement-Seam nie → würde den Workflow still hängen lassen). Bei `sig()`-by-name, das zur Konstruktionszeit nicht aufgelöst werden kann, greift der Runtime-Guard in U5.

**Patterns to follow:** Frozen-dataclass-Stil von `TaskInfo` (`task.py:55`); eager Export wie `ProgressInfo` (`__init__.py:33`); `canonical_encode`-JSON-Disziplin (`serialization.py`).

**Test scenarios:**
- Happy path: `Task.s(x)` und `sig("name", x)` erzeugen äquivalente Signatures (Covers AE9).
- Happy path: `A.s() | B.s() | C.s()` == `chain(...)`; `group(...) | M.s()` == `chord(...)` (Covers AE10).
- Happy path: `.si("done")` markiert immutable; `to_dict`/`from_dict` Roundtrip erhält alle Felder.
- Edge case: `chain(A, group(B, C), D)` valide; tiefe Rekursion (group-in-group-in-group) wird bei Konstruktion abgelehnt (Covers AE11).
- Edge case: `group` mit > `max_group_size` Membern → `WorkflowValidationError` bei Konstruktion (Covers AE7).
- Error path: Signature eines FAF-deklarierten Tasks in chain/group/chord → `WorkflowValidationError` bei Konstruktion.
- Property: beliebige valide Topologie überlebt `from_dict(to_dict(x))` identisch.

**Verification:** `tests/unit/test_workflow.py` + Property-Test grün; `from swarmq import chain, group, chord, sig, Signature` funktioniert; mypy-strict-clean (neues Modul in Strict-Liste).

---

### U2. Valkey-Backend: Workflow-State, Cancel-Flag, Namespaces

**Goal:** Persistenz-Schicht für Workflow-Topologie/Status, workflow-level Cancel-Flag und Chord-Counter-Keys, mit der bestehenden Error/Timeout-Disziplin.

**Requirements:** R10, R13, R14, R17

**Dependencies:** None (parallel zu U1)

**Files:**
- Modify: `src/swarmq/backend/valkey.py`
- Test: `tests/integration/test_workflow_backend.py`

**Approach:**
- Neue private Keys `_workflow_key → sq:wf:{id}`, `_workflow_cancel_key → sq:wf:cancel:{id}`, `_chord_set_key → sq:wf:chord:{id}`, `_chord_flag_key`, und Notification-Channel `_workflow_notify_channel → sq:wf:notify:{id}`.
- Methoden: `store_workflow_state(wf_id, bytes, ttl_ms)`, `get_workflow_state(wf_id)`, `set_workflow_cancel_flag`/`is_workflow_cancelled` (analog `set_cancel_flag`/`is_cancelled`), Helper für Per-Member-Result-Lese-Batch.
- **Workflow-gekeyte Notification:** `publish_workflow_notification(wf_id)` → `PUBLISH sq:wf:notify:{wf_id}` (analog dem bestehenden per-task `publish_notification`, aber workflow-id-gekeyt — die bestehende Primitive ist strikt `sq:notify:{task_id}` und passt NICHT für Workflow-Liveness). Subscribe erfolgt über denselben Channel-Namen. Dieser stabile, bei `apply()` bekannte Kanal ist die Grundlage für U4-`get()` und U6-Failed-Mark.
- TTL = `config.result_ttl` mit Ceiling-Division (Learning R3.5 Pattern 6). Jede Methode wrappt `(ValkeyError, OSError)` → `BackendConnectionError`, optionaler `op_timeout`.

**Patterns to follow:** `_key`/`store_result`/`set_cancel_flag` (`valkey.py:59,103,244`); `BackendConnectionError`-Wrapping in jeder Methode.

**Test scenarios:**
- Happy path: store/get Workflow-State Roundtrip; State respektiert TTL.
- Happy path: `set_workflow_cancel_flag` → `is_workflow_cancelled` True; doppeltes Set ist No-op.
- Edge case: `get_workflow_state` auf unbekannte ID → None (für `WorkflowNotFound`-Mapping in U4).
- Error path: Valkey down → `BackendConnectionError` (gemockter Client, der `ValkeyError` wirft).
- Integration: TTL-Ablauf entfernt State (kurze TTL + Warten via Event, kein `asyncio.sleep`).

**Verification:** `tests/integration/test_workflow_backend.py` grün gegen echtes Valkey (autouse `flush_valkey`).

---

### U3. Atomare+idempotente Chord-Completion (Lua + Manager)

**Goal:** `chord_complete.lua` + ein `ChordManager`-Wrapper, der die „genau ein Worker feuert den Body"-Entscheidung idempotent gegen Redelivery trifft.

**Requirements:** R12

**Dependencies:** U2

**Files:**
- Create: `src/swarmq/lua/chord_complete.lua`, `src/swarmq/workflow_orchestration.py` (ChordManager)
- Test: `tests/integration/test_chord_lua_scripts.py`

**Approach:**
- Lua: `KEYS=[completed_set, body_flag]`, `ARGV=[member_id, expected_N, ttl_ms]`. `SADD` Member, `SCARD`-Check gegen N, `SETNX` body_flag — Rückgabe `1` nur für den einzigen „letzten" Aufruf, der das Flag setzt. Header-Kommentar mit Atomicity-Note (Konvention).
- `ChordManager.__init__(self, client)` registriert `client.register_script(load_script("chord_complete"))`; Methode `record_and_check_complete(...) -> bool`.

**Patterns to follow:** `RateLimiter` (`limits.py:92`) Collaborator-owned-Script-Pattern; Lua-Header von `sched_commit.lua`/`lock_acquire.lua`; Integration-Test-Stil `test_lock_lua_scripts.py`.

**Test scenarios:**
- Happy path: N Member-Aufrufe → genau einer gibt `1`, der Rest `0`.
- Edge case: Redelivery (derselbe `member_id` zweimal) erhöht den Count nicht; `body_flag` bleibt einmalig.
- Edge case: konkurrierende „letzte" Aufrufe (zwei Member quasi gleichzeitig) → genau einer feuert (Covers AE4).
- Integration: TTL auf Set/Flag gesetzt; Ablauf räumt auf.

**Verification:** `tests/integration/test_chord_lua_scripts.py` grün; wiederholte Läufe mit dupliziertem `member_id` feuern den Body nie mehrfach.

---

### U4. Client `.apply()`, WorkflowHandle, Reattach

**Goal:** Fire-and-forget-Start eines Workflows mit `workflow_id`, Topologie-Persistenz, initialem Enqueue, und ein reattach-bares Handle (`get`/`status`/`cancel`/`children`).

**Requirements:** R10, R13, R14, R15

**Dependencies:** U1, U2

**Files:**
- Modify: `src/swarmq/client.py` (`.apply()`, `client.workflow(id)`), `src/swarmq/app.py` (Pass-Through), `src/swarmq/headers.py` (Workflow-Header), `src/swarmq/workflow.py` (`WorkflowHandle`), `src/swarmq/exceptions.py` (`WorkflowNotFound`)
- Test: `tests/unit/test_workflow_handle.py`, `tests/integration/test_workflow_orchestration.py` (apply-Teil)

**Approach:**
- `chain/group/chord(...).apply()` → `client.apply(topology)`: `workflow_id = uuid7()`, `store_workflow_state(...)`, initial lauffähige Schritte publishen, `WorkflowHandle` zurück.
- **Header-Passthrough:** `schedule`/`schedule_many` exponieren heute KEINEN `headers=`-Parameter (Header werden nur intern für dedup/enqueued-at gebaut). Daher: entweder ein internes `headers=`-Passthrough in `schedule`/`schedule_many` ergänzen (Dedup-/Rollback-Pfad muss mit Workflow-Headern weiter funktionieren) ODER initiale Workflow-Schritte direkt über `broker.publish` publishen. Die Workflow-Header (`x-swarmq-workflow-id`/`-step`) sind Pflicht, da der Advancement-Seam strikt darauf gated.
- **`get()`-Wartemechanismus:** `handle.get(timeout)` wartet auf den **stabilen, bei `apply()` bekannten** Workflow-Channel `sq:wf:notify:{wf_id}` (subscribe mit Re-Subscribe-Fallback im Client-Stil, nicht das einfache `backend.subscribe_and_wait`) **plus** Poll-Fallback auf den `sq:wf:{id}`-State-Key. `get()` pollt/wartet auf den **Workflow-Status** (`completed`/`failed`) im State — NICHT auf eine mid-flight generierte terminale `task_id` (die bei einer chain erst spät, beim group/chord erst beim Fan-in-Complete entsteht; daran zu binden hätte ein Race-Fenster, in dem die Notification feuert, bevor `get()` den Kanal kennt). Bei `status==completed` löst `get()` das Endergebnis über den im State hinterlegten `final_result`-Pointer auf. So ist der Wecker unabhängig vom Entstehungszeitpunkt der Schritt-IDs; U6's Failed-Mark + `publish_workflow_notification(wf_id)` weckt denselben Kanal (Liveness load-bearing).
- `WorkflowHandle`: `workflow_id`, `await get(timeout)` (s. o.), `status`, `await cancel()`, `children`. **`children` (R15)** wird beim Lesen aus der Topologie-Member-Liste + Completed-Set rekonstruiert (Reconciliation at read time), damit completed-Set und Per-Member-Status nicht transaktional synchron gehalten werden müssen.
- `client.workflow(workflow_id)` lädt State → Handle; `WorkflowNotFound` wenn State absent (R14-Kontrakt). Blockierender `.get()` ist der Convenience-Wrapper über fire-and-forget.

**Patterns to follow:** `Client.schedule`/`schedule_many`/`get_results`/`cancel` (`client.py:257,648,915,960`); Header-Konstanten (`headers.py`); `SwarmQ.cancel` Pass-Through (`app.py:167`).

**Test scenarios:**
- Happy path: `apply()` gibt sofort Handle mit `workflow_id`, `status`, ohne auf Completion zu warten (gemockter Broker/Backend).
- Happy path: `client.workflow(id)` rekonstruiert Handle mit `get`/`cancel` (Covers AE6).
- Error path: `client.workflow(unbekannt)` → `WorkflowNotFound`.
- Integration: `apply(chain)` enqueued nur Schritt 1; State enthält Topologie.
- Integration: `handle.children` listet group-Member mit Status.

**Verification:** Handle-Unit-Tests grün; Integration zeigt initialen Enqueue + reattach.

---

### U5. Worker-Advancement-Seam (chain-Weitergabe, chord-Counter, cancel-Gate)

**Goal:** Den Workflow im Worker voranbringen — nächsten chain-Schritt mit Result-Injektion enqueuen, chord-Counter via Lua erhöhen, Body bei Vollständigkeit enqueuen — idempotent und workflow-cancel-aware.

**Requirements:** R4, R6, R11, R12, R17

**Dependencies:** U1, U2, U3, U4

**Files:**
- Modify: `src/swarmq/worker.py` (Advancement-Stage bei `:1852-1883`, ChordManager-Wiring bei `:727`), `src/swarmq/workflow_orchestration.py` (Advancement-Helfer)
- Test: `tests/integration/test_workflow_orchestration.py`, `tests/e2e/test_workflows.py`

**Approach:**
- Advancement-Stage nach `store_result`+`publish_notification`, **vor** dem AT_LEAST_ONCE-Ack, nur wenn Message Workflow-Header trägt. `_try_advance_workflow(...) -> bool` im Stage-Stil des Ack-Ordering-Learnings (try/except wie `_evaluate_retry`; transient → `nack(requeue=True)`).
- Vor jedem Enqueue: `is_workflow_cancelled(wf_id)`-Gate (R17). chain: wenn `next` noch nicht enqueued → Result in `args` des nächsten Schritts prependen (außer `.si()`), **erst publishen, dann `next-enqueued`-Marker setzen** (publish-then-mark, siehe Key Technical Decisions). chord-Header/group-Member: `ChordManager.record_and_check_complete(...)`; bei `True` Body enqueuen.
- **Publisher-Confirm-Nack ist kein Erfolg:** `_publish_with_retry` (`broker/rabbitmq.py:119`) verwirft heute den Publish-Return-Value; aio_pika liefert unter `publisher_confirms` aber `Basic.Ack | Basic.Nack | Basic.Reject`. Ein `Nack`/`Reject` (z. B. Quorum-Queue ohne Disk) ist ein NEGATIVER Confirm — der Workflow-Schritt-Publish-Pfad MUSS ihn als Fehl-Publish behandeln (`next-enqueued`-Marker NICHT setzen, inbound Workflow-Message `nack(requeue=True)` für Advancement-Retry), sonst wird der Marker nach abgelehntem Publish gesetzt = verlorener Schritt — genau das, was publish-then-mark verhindern soll. Den Return-Value prüfen oder `mandatory`/`on_return_raises` so setzen, dass Confirm-/Routing-Fehler werfen.
- **Member-Result-Reihenfolge (R5):** Der Body-feuernde Worker liest die **geordnete** Member-Task-ID-Liste aus der persistierten Topologie (`sq:wf:{id}`) — nicht aus dem unsortierten `SADD`-Completed-Set — und übergibt sie an `get_results_batch` (das die Listenreihenfolge erhält). So entsteht die Definitionsreihenfolge unabhängig von der Completion-Reihenfolge.
- **Queue/Delivery-Auflösung:** Der Worker löst Queue + Delivery-Mode des nächsten Schritts aus `registry.get(name).task_info` auf (nicht über `Client`). **FAF-Schritte werden bereits in U1 zur Konstruktionszeit abgelehnt** (siehe U1); zusätzlich Runtime-Guard hier, falls ein per `sig()` referenzierter Task FAF ist (der FAF-Pfad acked vor `store_result` und erreicht den Seam nie → Stall vermeiden).
- **Chord-Body-Größen-Guard:** Die Inline-Sammlung von bis zu `max_group_size` Member-Results ist der dominante Message-Size-Risikopfad (schlimmer als chain-Schritt-zu-Schritt). Überschreitet der Body-Publish das Broker-Limit, wird der Workflow per U6-Pfad als failed markiert (kein stiller terminaler Publish-Fehler nach getaner Header-Arbeit). Large-Result-via-Referenz bleibt Follow-Up.
- ChordManager in `Worker.start()` (`:727`) nach `backend.connect()` instanziieren. Workflow-Schritte werden als AT_LEAST_ONCE publiziert; workflow-id-Header am Ingress validiert.

**Execution note:** Advancement-Stage test-first über einen Integration-Test des chain→chain-Übergangs (inkl. Result-Injektion) implementieren.

**Patterns to follow:** `_evaluate_retry`/`_try_apply_*`-Stage-Stil + Ack-Ordering-Learning; RateLimiter-Wiring (`worker.py:727`); DLX-transient-vs-permanent-Klassifikation.

**Test scenarios:**
- Happy path: chain A→B→C reicht A-Result als erstes Arg an B, B an C (Covers AE1-Happy-Teil).
- Happy path: `.si()`-Schritt erhält das Vorergebnis NICHT (Covers AE2).
- Happy path: chord-Header vollständig → Body genau einmal mit Member-Liste in Definitionsreihenfolge (Covers AE3, AE4).
- Edge case: Redelivery eines chain-Schritts enqueued den Nachfolger nicht doppelt (`next-enqueued`-Marker).
- Integration: `worker`-Crash-Simulation zwischen Advance und Ack → Redelivery, idempotent (Covers AE4-Redelivery).
- Integration: workflow-cancel-Flag gesetzt während laufendem A → B wird nicht enqueued (Covers AE8).
- Error path: Valkey-Blip im Advancement → `nack(requeue=True)`, keine verlorene/doppelte Weitergabe.

**Verification:** chain/group/chord-Roundtrips in `tests/e2e/test_workflows.py` grün; Advancement-Integration zeigt Idempotenz + cancel-Gate.

---

### U6. Terminal-Failure-Seam: Fail-fast mit Liveness

**Goal:** Ein endgültig gescheiterter Workflow-Schritt markiert den Workflow synchron als failed und weckt wartende `get()` — kein stiller Timeout; chord-Body läuft nicht.

**Requirements:** R16

**Dependencies:** U1, U2, U4, U5

**Files:**
- Modify: `src/swarmq/worker.py` (`_send_to_dlq`-Seam, `:2114`), `src/swarmq/workflow_orchestration.py`
- Test: `tests/integration/test_workflow_orchestration.py` (failure-Teil)

**Approach:**
- Im terminalen Failure-Pfad (Retry erschöpft → DLQ): wenn die Message Workflow-Header trägt, `sq:wf:{id}`-State synchron auf `failed` + Fehler-Payload setzen und `publish_workflow_notification(wf_id)` auf `sq:wf:notify:{wf_id}` feuern (derselbe Kanal, auf den U4-`get()` subscribed — kein per-task `sq:notify:{task_id}`), bevor der DLQ-Publish-then-ack abschließt. group-Member-Fail markiert die Group failed → chord-Body läuft nicht.
- `handle.get()` erkennt `failed`-State und propagiert die ursprüngliche Exception (kein Warten bis Timeout). Klassifikation transient vs permanent beibehalten (DLX-Learning).

**Execution note:** Test-first: ein Integration-Test, der `get()` innerhalb gebundener Zeit eine Exception werfen lässt, statt in den Timeout zu laufen.

**Patterns to follow:** `_send_to_dlq` (`worker.py:2114`), zwei-Level-DLQ + DLX-Wording-Learning; `publish_notification` (`valkey.py:137`).

**Test scenarios:**
- Happy path (Fehlerfall): chain-Schritt B scheitert endgültig → C nie enqueued, Workflow-State `failed`, `handle.get()` wirft B's Exception innerhalb der Timeout-Grenze (Covers AE1).
- Edge case: chord-Header-Member scheitert → Body nicht ausgeführt, chord failed (Covers AE5).
- Integration: `get(timeout=kurz)` kehrt mit Exception zurück, NICHT mit Timeout, sobald der State failed ist (Liveness).
- Error path: transienter Valkey-Fehler beim Failed-Markieren → `nack(requeue=True)`, kein Verlust.

**Verification:** Fehler-Integration zeigt gebundene `get()`-Propagation; kein stilles Hängen.

---

### U7. Härtung: Result-TTL-Kontrakt + Runtime-Tiefen-Guard

**Goal:** Member-Result-Lebensdauer ≥ Workflow absichern, expired-vs-missing unterscheiden, und dynamisch zusammengesetzte Tiefe (`sig()`-Expansion) gegen `max_workflow_depth` schützen.

**Requirements:** R6, R9, R14

**Dependencies:** U1, U2, U5

**Files:**
- Modify: `src/swarmq/backend/valkey.py` (TTL-Refresh / expired-Erkennung), `src/swarmq/workflow_orchestration.py` (Runtime-Guard), `src/swarmq/exceptions.py` (`WorkflowResultExpired`)
- Test: `tests/integration/test_workflow_backend.py` (TTL-Teil), `tests/integration/test_workflow_orchestration.py` (Guard-Teil)

**Approach:**
- Member-Results bekommen eine TTL ≥ Workflow-Lebensdauer (Refresh beim Fan-in-Sammeln statt kurzer Per-Task-TTL). Beim Einsammeln: fehlendes Ergebnis bei noch-aktivem Workflow → `WorkflowResultExpired` statt stilles `None` (Unterscheidung expired vs nie-geschrieben über State-Wissen, welche Member fertig gemeldet wurden).
- Runtime-Guard im Advancement-Seam: vor Enqueue eines aus `sig()` expandierten Sub-Schritts effektive Tiefe gegen `max_workflow_depth` prüfen; Überschreitung → Workflow failed mit klarer Fehlermeldung.

**Patterns to follow:** Ceiling-Division-TTL + Runtime-vs-Construction-Guard-Learning (R3.5); `get_results_batch` (`valkey.py:143`).

**Test scenarios:**
- Edge case: Member-Result abgelaufen, Workflow noch aktiv → `WorkflowResultExpired`, keine `None`-Löcher.
- Edge case: langer Fan-in, Frühergebnisse leben bis Body läuft (TTL-Refresh greift).
- Edge case: `sig()`-Kette, die zur Laufzeit `max_workflow_depth` überschreitet → Workflow failed mit klarer Meldung (nicht stilles Tieferlaufen).
- Happy path: normale Tiefe ≤ Limit läuft unverändert durch.

**Verification:** TTL- und Guard-Integration grün; kein stilles `None` bei abgelaufenen Ergebnissen.

---

### U8. E2E-, Chaos- und Kombinations-Abdeckung

**Goal:** End-to-End-Roundtrips, exactly-once-Chord unter Redelivery/Worker-Crash, und Interaktion mit bestehenden Features (cancel/retry/DLQ/FAF).

**Requirements:** R11, R12, R16, R17 (Verifikation der Korrektheits-Garantien)

**Dependencies:** U5, U6, U7

**Files:**
- Create: `tests/e2e/test_workflows.py`, `tests/chaos/test_workflow_chord_idempotency.py`, `tests/e2e/combinations/test_workflow_combinations.py`

**Approach:**
- E2E: vollständige `SwarmQ(...)`-Roundtrips für chain/group/chord inkl. reattach. Chaos (`-m chaos`, Toxiproxy/Container-Restart): Chord-Body exakt-einmal unter Redelivery und Worker-Crash; nutzt `msg.redelivered` als Signal. Kombinationen: Workflow × Cancellation, × Retry/DLQ, × FIRE_AND_FORGET-Tasks (Verhalten/Ablehnung), analog zu den 31 bestehenden Feature-Kombinationen.

**Execution note:** Chaos-/Kombinations-Tests sind opt-in markiert (`chaos`), laufen in den Nightly-/Kombinations-CI-Stufen.

**Patterns to follow:** `tests/e2e/conftest.py` (`check_docker`, urls); `tests/chaos/test_worker_crash.py`, `test_backend_restart.py`; `tests/e2e/combinations/`-Stil.

**Test scenarios:**
- Integration/E2E: chain/group/chord-Roundtrip liefert korrekte Endergebnisse.
- Chaos: 100+ Wiederholungen mit Redelivery → Chord-Body-Ausführungen == 1, nie 0/≥2 (Covers AE4; erfüllt Success-Criterion „race-frei").
- Chaos: Worker-Crash mitten in der Orchestrierung → Workflow läuft korrekt weiter (kein Verlust, keine Dopplung).
- Chaos: Crash zwischen Publish und Marker-Set → Nachfolger wird (per Redelivery) doch enqueued, Kette stallt nicht; höchstens seltenes Doppel-Enqueue, von Idempotenz absorbiert.
- Integration: group/chord liefert Member-Results in Definitionsreihenfolge trotz out-of-order-Completion (runtime-Pendant zu AE3).
- Integration: Workflow × `cancel()` (Covers AE8); Workflow × Retry-dann-DLQ (Covers AE1, AE5); FIRE_AND_FORGET-Task als Workflow-Schritt → klar definiertes Verhalten/Ablehnung.

**Verification:** E2E grün im regulären Lauf (Tier-1); Chaos/Kombination grün in der opt-in-Stufe; Success-Criteria der origin (race-frei, reattach, kein Glue) nachgewiesen.

---

## System-Wide Impact

- **Interaction graph:** Neue Advancement-Stage im Worker-Completion-Pfad (`_handle_message`) und im Terminal-Failure-Pfad (`_send_to_dlq`); ChordManager als neuer Worker-Collaborator. Client bekommt `.apply()`/`workflow()`. Keine Änderung an bestehenden Task-/Signal-/Retry-Semantiken außerhalb des Workflow-Headers-Gates.
- **Error propagation:** Advancement-Fehler folgen der Ack-Ordering-Regel — transient → `nack(requeue=True)`, permanent → DLX (`requeue=False`); neue Backend-Methoden wrappen in `BackendConnectionError`.
- **State lifecycle risks:** Doppel-Enqueue bei Redelivery (durch `next-enqueued`-Marker + idempotentes Chord-Lua abgesichert); Workflow-State-/Member-Result-TTL (Ceiling-Division, Refresh ≥ Workflow); Cancel-Race (workflow-level Flag am Enqueue-Gate).
- **API surface parity:** Neue öffentliche Symbole `chain/group/chord/sig/Signature` + `WorkflowHandle`; `Task.s()/.si()`; `SwarmQ`/`Client.apply()`/`workflow()`. Lazy-/eager-Export-Konvention beachten.
- **Integration coverage:** Chord-Exactly-once und Mid-Orchestrierungs-Crash sind nur in Integration/Chaos beweisbar (U3, U5, U8), nicht durch Unit-Mocks.
- **Unchanged invariants:** Bestehende einzelne-Task-Zustellung, Retry/DLQ, Rate-Limiting, Locking, Unique, Scheduler, Cancellation bleiben unverändert; Workflow-Logik ist strikt durch Workflow-Header gegated.

---

## Risk Analysis & Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Chord-Body feuert 0- oder mehrfach unter Redelivery | Med | High | Idempotentes `SADD`+One-Shot-Flag-Lua (U3); Chaos-Test 100+ Läufe (U8) |
| Verlorener chain-Schritt (Crash zwischen Marker und Publish) | Med | High | **publish-then-mark**-Reihenfolge (U5/Key Decisions): erst confirmed publishen, dann markieren — seltenes Doppel-Enqueue statt verlorenem Schritt; Chaos-Test „crash between publish and mark" (U8) |
| Doppel-Enqueue des nächsten chain-Schritts bei Crash | Med | Low | Von Schritt-Idempotenz (AT_LEAST_ONCE) absorbiert — bewusst die sichere Richtung |
| FAF-Task als Workflow-Schritt → stiller Stall | Med | High | Konstruktionszeit-Ablehnung (U1) + Runtime-Guard für `sig()`-by-name (U5) |
| Chord-Body-Message überschreitet Broker-Size-Limit (großes Fan-in) | Med | Med | Größen-Guard → Workflow-failed-Mark statt stillem Publish-Fehler (U5); Large-Result-Referenz als Follow-Up |
| `get()` hängt bis Timeout statt Fehler zu propagieren | Med | High | Synchroner Failed-Mark + Notification am `_send_to_dlq`-Seam (U6); Liveness-Test |
| Cancel-Race: N+1 trotz cancel enqueued | Med | Med | Workflow-level Flag am Enqueue-Gate atomar geprüft (U5, U7) |
| Member-Result-TTL läuft vor Fan-in ab → stille Löcher | Med | Med | TTL ≥ Workflow + expired-vs-missing-Fehler (U7) |
| `sig()`-Expansion umgeht Tiefen-Limit | Low | Med | Runtime-Tiefen-Guard zusätzlich zur Konstruktions-Validierung (U7) |
| Pub/Sub verliert Notification bei Reconnect | Low | Med | Bestehender `get_result`-Re-Subscribe + Result-Key-Poll-Fallback nutzen (U4) |

---

## Phased Delivery

### Phase 1 — Topologie (U1)
Infrastruktur-freie Signature-/Primitiv-/Validierungs-Schicht; sofort unit-testbar, keine Abhängigkeit zu Broker/Backend.

### Phase 2 — Persistenz & Lua (U2, U3)
Backend-Namespaces + atomares Chord-Lua; integration-getestet gegen echtes Valkey.

### Phase 3 — Start & Handle (U4)
`.apply()`, WorkflowHandle, Reattach — Workflows lassen sich starten und beobachten.

### Phase 4 — Orchestrierung (U5, U6)
Worker treibt Workflows voran (Erfolg) und behandelt terminale Fehler (Liveness) — das Herzstück.

### Phase 5 — Härtung & Nachweis (U7, U8)
TTL-/Tiefen-Kontrakte + E2E/Chaos/Kombinations-Tests, die die Korrektheits-Garantien beweisen.

---

## Documentation / Operational Notes

- README „What's in the box" + `What's planned next` aktualisieren (Workflow-Primitive von deferred → implemented).
- User-Guide-Seite unter `website/guides/` für chain/group/chord (analog hot-reload-Guide).
- Nach dem Landen: `/ce-compound` für die fehlende „distributed coordination / chord idempotency"-Lernlücke (vom Learnings-Researcher angemerkt).
- AT_LEAST_ONCE-Idempotenz-Anforderung für Workflow-Schritt-Tasks in der Doku hervorheben (Anwender-Verantwortung bei Seiteneffekten).

---

## Sources & References

- **Origin document:** `docs/brainstorms/2026-05-30-workflow-primitives-requirements.md`
- Related research: `docs/recherche-workflows-cancel-progress.md`, `docs/recherche-deepening-lua-scripts.md` (Abschnitt 7), `aufgabe.md` Abschnitt 6.6
- Learnings: `docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`, `docs/solutions/patterns/r3-5-unique-tasks-implementation-learnings-2026-04-17.md`, `docs/solutions/logic-errors/nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md`, `docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md`, `docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md`
- Key code seams: `src/swarmq/worker.py:1352,1852,2114,727`, `src/swarmq/client.py:257,648,915,960`, `src/swarmq/backend/valkey.py:59,103,137,244`, `src/swarmq/lua/__init__.py`, `src/swarmq/task.py:267`, `src/swarmq/__init__.py`
