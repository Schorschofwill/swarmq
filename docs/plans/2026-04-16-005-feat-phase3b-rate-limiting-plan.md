---
title: "feat: Phase 3b.1 — Rate Limiting via Sliding Window Counter"
type: feat
status: completed
date: 2026-04-16
origin: docs/brainstorms/implementation-order-requirements.md
---

# feat: Phase 3b.1 — Rate Limiting via Sliding Window Counter

## Overview

Phase 3b.1 implementiert R3.3 aus dem Brainstorm: globales Rate Limiting pro Task-Name, deklarativ an `TaskInfo`, atomar in Valkey via Lua-Script. String-Syntax `"100/m"` (100 pro Minute) wie Celery. Rate-limitierte Tasks werden **nicht** in die DLQ geroutet — sie werden mit dem Retry-After-Delay des Scripts requeued und laufen beim nächsten freien Worker.

Das Feature ist technisch geradeaus (ein Lua-Script, ein Python-Wrapper, ein TaskInfo-Feld, ein Worker-Integration-Point). Die wichtigste Entscheidung in diesem Plan ist **wo die Lua-Primitives leben**: sie werden von 3b.2 (Lock + Dedup) und 3b.3 (Cancel) wiederverwendet — Paper-Prototype-Evidence siehe unten. Der Plan etabliert die Shared-Helper-Konventionen so klein wie möglich, nutzt Valkey-Bibliotheks-Primitives statt Re-Implementierung.

## Problem Frame

Tasks mit externen Ratebegrenzungen (Payment-Gateways, SMS-APIs, LLM-Quotas) dürfen Worker nicht ungebremst abfeuern. Celery's `rate_limit='10/s'` ist **per-Worker** — 4 Worker × 10/s = effektiv 40/s — was gegen externe Limits oft nicht reicht. Dramatiq hat globales Rate-Limiting über Redis. SwarmQ will dieselbe Semantik liefern: **global** über alle Worker, atomar, deklarativ.

Gewählter Algorithmus: Sliding Window Counter (Cloudflare-Variante, Dramatiq-kompatibel). Speicher O(1) pro Task-Typ, Approximationsfehler ~0,003 %. Referenz: `docs/recherche-deepening-lua-scripts.md` §3.1.

## Paper-Prototype Evidence (verifiziert 2026-04-16)

Vor Implementation wurde inventarisiert, welche Valkey-Ops der drei geplanten Phase-3b-Unterfeatures Lua brauchen:

| Feature | Operations | Lua nötig? |
|---|---|:---:|
| **3b.1 Rate Limit** | Sliding Window Counter | ✅ 1 Script |
| **3b.2 Lock / Semaphore / Unique** | Acquire + Release mit Ownership-Check | ✅ 6 Scripts |
| **3b.3 Task Cancellation** | Set-Flag (mit Pub/Sub atomar) | ✅ 1 Script |
| 3b.3 Cancel Check / Clear | EXISTS / DEL | ❌ Native Valkey-Ops |

**8 von 10 Operations brauchen Lua.** Shared-Helper für Script-Loading + Registrierung ist echt, nicht spekulativ. Die Form des Helpers ist aber deutlich kleiner als ursprünglich geplant (siehe Key Decisions: kein eigener NOSCRIPT-Wrapper — `valkey.asyncio` Client macht das schon via `register_script()`).

## Requirements Trace

- **R1** (aus R3.3): TaskInfo-Feld `rate_limit: str | None = None`, Celery-kompatible Syntax `"N/s|m|h"`, optional
- **R2** (aus R3.3): **Global** über alle Worker via Valkey. Nicht per-Worker
- **R3** (aus R3.3): Rate-limitiert → Requeue mit Delay aus Lua (`retry_after_ms`), **nicht** DLQ
- **R4** (aus R3.3): Signal `TASK_RATE_LIMITED` (existiert bereits) wird bei Treffer dispatched mit `task_id`, `task_name`, `retry_after_ms`, `remaining`. Structured-Log-Zeile ist Teil dieses Signals — keine separate Observability-Requirement
- **R5** (aus R3.3): Sliding Window Counter. Kein Token Bucket (Burst-Toleranz) in dieser Phase — siehe Scope
- **R6** (Shared-Infra): Lua-Scripts leben als eigene `.lua`-Dateien unter `src/swarmq/lua/`, werden via `importlib.resources` geladen, via `client.register_script(source)` registriert. Folgepläne 3b.2/3b.3 konsumieren denselben Loader

## Scope Boundaries

- **Nur R3.3** (Rate Limit). R3.4 (Lock), R3.5 (Dedup), R3.6 (Cancel) sind eigenständige Folgepläne
- **Sliding Window only** — Token Bucket (Burst) als bewusstes Non-Goal für 3b.1. Falls später benötigt: eigener Plan, gleicher `register_script`-Pfad
- **Keine dynamische Rate-Limit-Änderung zur Laufzeit** — deklarativ in TaskInfo. Dynamik via Management-API in Phase 5
- **Keine Semantic-Validation** — `rate_limit="1/h"` für 100-req/min-Task ist technisch gültig, pragmatisch selbstgewählt. Kein Frequency-vs-Limit-Check
- **Kein unbounded Requeue-Loop-Schutz** — wenn Rate-Limit permanent zu eng gesetzt, rotieren Tasks zwischen Delay-Queue und Worker ohne Ende. Operational concern, nicht Plan-Scope. Max-Requeue-Count wäre eigener Plan (Phase 5, Management-APIs)

### Deferred to Separate Tasks

- **3b.2** — Lock + Semaphore + Unique Tasks (nutzt denselben Lua-Loader + `register_script`-Pattern)
- **3b.3** — Task Cancellation (1 Lua-Script + 2 native Ops)
- **3c** — Progress, Scheduler, CLI
- **Phase 5** — Prometheus `/metrics`, `rate_limit`-Hot-Reload, Permanent-Rate-Limit-Alerts

## Context & Research

### Relevant Code and Patterns

- `src/swarmq/task.py:17-78` — `TaskInfo` frozen dataclass mit custom `__init__` + `__post_init__`. Neues Feld analog zu bestehenden ergänzen (explicit `object.__setattr__` im `__init__`, Validierung in `__post_init__`)
- `src/swarmq/backend/valkey.py` — Valkey-Client (`valkey.asyncio.Valkey`). `client.register_script(source)` liefert ein `AsyncScript`-Object mit eingebautem NOSCRIPT-Recovery
- `src/swarmq/worker.py` — `_handle_message` ist die zentrale Pipeline. Rate-Limit-Check sitzt zwischen Registry-Lookup und `run_pre_execute`
- `src/swarmq/worker.py:461-540` (`_evaluate_retry`) — Referenzmuster für "requeue mit Delay, kein DLQ"
- `src/swarmq/broker/rabbitmq.py:publish_to_delay_queue(queue, body, delay_ms, headers)` — existiert seit Phase 2, wird wiederverwendet
- `src/swarmq/signals.py:38` — `TASK_RATE_LIMITED` ist bereits definiert (Phase 3a). Dispatch fehlt
- `src/swarmq/exceptions.py` — neue Exceptions (`RateLimitExceeded`) analog zu `Retry`, `NoRetry`
- `src/swarmq/app.py:23-52` — `SwarmQ.__init__` ist **sync**. Lifecycle-Punkte für async-Setup: `__aenter__` (Connection) und `async start_worker()` (Worker-Build). RateLimiter-Instantiierung gehört an einen dieser Async-Punkte, nicht in `__init__`

### Institutional Learnings

- `docs/solutions/logic-errors/aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md` — Valkey-Lifecycle-Patterns: kein try/except um close(), keine Exception-Swallows bei EVALSHA-Fehlern
- `docs/solutions/patterns/measure-before-planning-variance-claims-2026-04-16.md` — kein Varianz-Claim in diesem Plan. Sliding Window ist deterministisch bei atomarem Lua

### External References

- `docs/recherche-rate-limiting-locking-dedup.md` §1 — Vergleich Celery vs Dramatiq vs Custom
- `docs/recherche-deepening-lua-scripts.md` §3.1 — vollständiges Lua-Script + Python-Caller (wortgetreue Vorlage)
- Cloudflare Blog 2017 — Sliding-Window-Approximation, ~0,003 % Fehler

### Document-Review Findings (resolved)

Pre-Implementation-Review mit 5 Reviewern (coherence / scope / feasibility / adversarial / product) ergab:
- **Feasibility F3:** `valkey.asyncio.AsyncScript.register_script()` macht NOSCRIPT-Recovery bereits eingebaut → Unit 1 schrumpft auf File-Loader
- **Feasibility F2:** RateLimiter-Instantiierung muss an async-Lifecycle-Punkt, nicht `__init__` → siehe Unit 5 Approach
- **Coherence F001:** Clock-Source-Widerspruch zwischen Key Decision und Unit 2 → Entscheidung: **client-seitig `TIME` lesen**, als ARGV[3] übergeben (siehe Key Decisions)
- **Adversarial F3:** Premise "3 Features teilen Lua" war unbewiesen → Paper-Prototype oben verifiziert
- **Product P01:** "Dramatiq-Niveau"-Claim war im Plan erfunden → gestrichen
- **Scope F2:** Unit 6 duplizierte Unit 5 Integration-Test → gemerged

Alle Artifacts unter `.context/compound-engineering/document-review/plan005-*.json`.

## Key Technical Decisions

- **Sliding Window Counter, nicht Token Bucket:** Dramatiq-kompatibel, O(1) Speicher, keine impliziten Burst-Zusagen. Token Bucket als Non-Goal für 3b.1
- **Global über Valkey, nicht per-Worker:** zentrale Positionierung gegenüber Celery
- **Lua-Scripts leben als `.lua`-Dateien unter `src/swarmq/lua/`:** bessere IDE-Unterstützung, separat linterbar, versionierbar. Geladen via `importlib.resources.files("swarmq.lua")`
- **Script-Registrierung via `client.register_script(source)`, kein eigener NOSCRIPT-Wrapper:** `valkey.asyncio.AsyncScript` fängt `NoScriptError`, macht `SCRIPT LOAD`, retryt. Selbst-implementierter Wrapper wäre redundant (siehe Feasibility F3 aus Review)
- **Clock-Source: Client-seitig `TIME` lesen, als ARGV[3] übergeben:** Konsistent mit Recherche-Vorlage (`docs/recherche-deepening-lua-scripts.md:799-800`). Vorteil: ein einziger Zeitpunkt pro Request (kein Drift zwischen `current_key`-Berechnung im Client und Script-TIME). Nachteil: zwei Roundtrips (TIME + EVALSHA). Bei Replikation (später) weiterhin korrekt, weil jeder Request einen eigenen Zeitstempel hat
- **Key-Namespacing: `sq:rl:{task_name}:{window_ts}`** — etabliert Prefix-Konvention für 3b.2 (`sq:lock:*`, `sq:sem:*`, `sq:unique:*`) und 3b.3 (`sq:cancel:*`)
- **RateLimiter-Lifecycle: Lazy-Instantiierung beim ersten `check()` oder via `start_worker()`:** `SwarmQ.__init__` ist sync, `backend._client` erst nach `__aenter__`/`connect()` verfügbar. Entscheidung im Plan: Worker hält `_rate_limiter: RateLimiter | None`, initialisiert in `Worker.start()` nach `backend.connect()` — saubere Lifecycle-Semantik, einheitlich mit `_consume_channel`-Pattern
- **Requeue via `publish_to_delay_queue()`, NICHT ack+republish:** bestehender DLX-Delay-Mechanismus aus Phase 2, keine Duplicate-Delivery-Risiken
- **Rate-Limit-Check **vor** `run_pre_execute`-Hook:** Middleware sieht rate-limitierte Tasks nicht. `TASK_RATE_LIMITED` Signal ist der dokumentierte Observability-Hook
- **Kein Middleware-Hook `on_rate_limited`:** Signal reicht. Middleware-Pipeline bleibt fokussiert auf Success/Error-Pfade. 6 bestehende Hooks aus Phase 3a unverändert
- **String-Parser tolerant gegenüber Whitespace und Einheits-Aliasen:** `"100/m"`, `"100 / min"`, `"100/hour"` → OK. Unbekannte Einheit → `ValueError`
- **Packaging: `.lua`-Files werden ins Wheel gepackt:** `[tool.hatch.build.targets.wheel] force-include` (falls noch nicht) oder equivalent — verifiziert bei Implementation

## Open Questions

### Resolved During Planning

- **Clock-Source:** Client-seitig `TIME` lesen, als ARGV[3] übergeben (konsistent zur Recherche-Vorlage)
- **Lua-Infrastruktur:** `register_script()` aus `valkey.asyncio` nutzen, keinen eigenen Wrapper bauen
- **RateLimiter-Lifecycle:** Init in `Worker.start()` nach `backend.connect()`
- **Signal vs Middleware-Hook:** Signal only — kein 7ter Middleware-Hook
- **Permanent-Rate-Limit-Schutz:** Non-Goal für 3b.1 (siehe Scope)
- **Token Bucket:** Non-Goal für 3b.1 (siehe Scope)

### Deferred to Implementation

- Exakte Namen der privaten Helper (`_parse_rate_limit` vs `_rate_limit_from_string`)
- Ob `RateLimiter` eine eigene Klasse wird (`src/swarmq/limits.py`) oder Methoden am `ValkeyBackend` — Erwartung: eigene Klasse, weil 3b.2 `Locker` bekommt und 3b.3 `Canceller`. Konsistente Struktur
- Ob die `.lua`-Files per `hatchling`-`force-include` oder über die Standard-Package-Data-Regel ins Wheel kommen — wird bei Unit 1 verifiziert

## Implementation Units

- [x] **Unit 1: Lua-Script-Loader (`src/swarmq/lua/__init__.py`)**

**Goal:** Minimaler File-Loader der Lua-Dateien aus `src/swarmq/lua/*.lua` lädt. Konsumenten nutzen dann `client.register_script(source)` direkt (NOSCRIPT-Recovery ist dort eingebaut).

**Requirements:** R6

**Dependencies:** Keine

**Files:**
- Create: `src/swarmq/lua/__init__.py` (mit `load_script(name)`)
- Test: `tests/unit/test_lua_loader.py`

**Approach:**
- `load_script(name: str) -> str` — nutzt `importlib.resources.files("swarmq.lua").joinpath(f"{name}.lua").read_text(encoding="utf-8")`
- Fail-fast: `FileNotFoundError` wenn Script-File nicht existiert (klarer Fehlertext `"unknown lua script: {name}"`)
- Cacht **nicht** explizit — Konsumenten rufen `register_script(load_script(...))` im eigenen `__init__` auf, das ist eine einmalige Operation pro Instance
- Keine Valkey-Abhängigkeit in diesem Modul — pure String-Operation. Ermöglicht Tests ohne Docker

**Execution note:** test-first. Der Loader ist trivial, aber das `importlib.resources`-Verhalten muss stimmen (Editable-Install vs Wheel-Install).

**Patterns to follow:**
- `importlib.resources.files` Pattern (Python 3.9+). `.joinpath().read_text()` liefert String

**Test scenarios:**
- Happy path: `load_script("sliding_window_counter")` → String mit `redis.call` drin
- Happy path: Loader funktioniert bei editable-install (dev) und wheel-install (verifizieren via `uv pip install -e .` vs gebaute wheel)
- Error: `load_script("nonexistent")` → `FileNotFoundError`, message enthält `"nonexistent"` und `"swarmq.lua"`
- Edge case: Script-File leer → returns `""` (kein Special-Case)

**Verification:**
- `uv run pytest tests/unit/test_lua_loader.py` grün
- `uv run python -c "from swarmq.lua import load_script; print(load_script('sliding_window_counter')[:80])"` gibt Script-Head aus

---

- [x] **Unit 2: Sliding-Window-Counter Lua-Script + Integration-Tests**

**Goal:** Das Lua-Script selbst (wortgetreue Übernahme aus der Recherche) + Integration-Tests gegen eine echte Valkey-Instanz.

**Requirements:** R2, R5

**Dependencies:** Unit 1

**Files:**
- Create: `src/swarmq/lua/sliding_window_counter.lua`
- Test: `tests/integration/test_rate_limit_lua.py`

**Approach:**
- Lua-Script aus `docs/recherche-deepening-lua-scripts.md` §3.1 wortgetreu übernehmen
- **Signatur verifizieren (Clock-Source-Konsistenz):**
  - `KEYS[1]` = current window key (`sq:rl:{task_name}:{window_ts}`)
  - `KEYS[2]` = previous window key
  - `ARGV[1]` = max_requests
  - `ARGV[2]` = window_size_ms
  - `ARGV[3]` = **now_ms (vom Client, via `TIME`-Command)**
  - Returns: `[allowed: 0|1, remaining: int, retry_after_ms: int]`
- Integration-Test (marked `integration`, Docker-required): direkt via `client.register_script(source)` + Call — verifiziert Lua isoliert

**Execution note:** test-first. Kritisches atomares Verhalten, Fehler hier korrumpieren alle Rate-Limits.

**Test scenarios (Integration, Docker):**
- Happy path: `max=100, window=60000ms`, 10 Requests hintereinander → alle allowed, remaining 99..90
- Boundary: Request 100 bei `max=100` → letzter allowed, 101ster rate-limited mit `retry_after > 0`
- Rate-Limited: `retry_after_ms > 0`, `remaining = 0`
- Zeitfortschritt: Keys zwischen 2 Windows rotieren → Counter sliden, volle Kapazität nach vollem Window
- Edge: `max_requests = 0` → jeder Request rate-limited
- Edge: `window_size_ms = 0` → Lua returned `{0, 0, 0}` (Division-by-Zero-Schutz im Script)
- Atomarität: 150 parallele EVALSHA via `asyncio.gather`, `max=100` → exakt 100 allowed, 50 rate-limited

**Verification:**
- `uv run pytest tests/integration/test_rate_limit_lua.py` grün mit aktivem Docker
- Keine Race-Condition bei parallelen Aufrufen

---

- [x] **Unit 3: RateLimiter Python-Wrapper (`src/swarmq/limits.py`)**

**Goal:** `parse_rate_limit("100/m")` + `RateLimiter(client).check(task_name, config)` mit SCRIPT-Registration im Konstruktor.

**Requirements:** R1, R2, R5

**Dependencies:** Unit 1, Unit 2

**Files:**
- Create: `src/swarmq/limits.py`
- Test: `tests/unit/test_rate_limiter.py` (Parser)
- Test: `tests/integration/test_rate_limiter_integration.py` (end-to-end mit Valkey)

**Approach:**
- `RateLimitConfig(max_requests: int, window_size_ms: int)` NamedTuple
- `def parse_rate_limit(s: str) -> RateLimitConfig`:
  - Akzeptiert `"100/m"`, `"100 / m"`, `"10/s"`, `"1000/h"`, `"100/min"`, `"100/hour"`
  - Einheiten: `s`→1000, `m`/`min`→60_000, `h`/`hour`→3_600_000
  - `count < 1` → `ValueError("count must be >= 1")`
  - Unbekannte Einheit → `ValueError("unsupported unit: ...")`
  - Format ohne `/` → `ValueError("missing unit separator")`
- `RateLimiter`:
  - `__init__(client: valkey.asyncio.Valkey)`: `self._script = client.register_script(load_script("sliding_window_counter"))`
  - `async def check(self, task_name: str, config: RateLimitConfig) -> tuple[bool, int, int]`:
    - `now_ms` via `await client.time()` → `(seconds, microseconds)` → `seconds * 1000 + microseconds // 1000`
    - `window_ts = now_ms - (now_ms % config.window_size_ms)`, `prev_ts = window_ts - config.window_size_ms`
    - Keys: `[f"sq:rl:{task_name}:{window_ts}", f"sq:rl:{task_name}:{prev_ts}"]`
    - Args: `[config.max_requests, config.window_size_ms, now_ms]`
    - `result = await self._script(keys=keys, args=args)`
    - Return: `(bool(result[0]), int(result[1]), int(result[2]))`

**Execution note:** test-first für Parser (pure, einfach). Integration-first für `check` (braucht Valkey).

**Test scenarios (Unit, Parser):**
- Happy: `parse("100/m")` → `RateLimitConfig(100, 60_000)`
- Happy: `parse("10/s")` → `(10, 1_000)`
- Happy: `parse("1000/h")` → `(1000, 3_600_000)`
- Tolerant: `parse(" 100 / m ")` → `(100, 60_000)` (Whitespace)
- Tolerant: `parse("100/min")` → `(100, 60_000)` (Alias)
- Tolerant: `parse("100/hour")` → `(100, 3_600_000)` (Alias)
- Error: `parse("100/day")` → `ValueError("unsupported unit")`
- Error: `parse("abc/m")` → `ValueError`
- Error: `parse("0/m")` → `ValueError("count must be >= 1")`
- Error: `parse("-5/m")` → `ValueError`
- Error: `parse("100")` → `ValueError("missing unit separator")`

**Test scenarios (Integration):**
- Happy: `check("task_a", parse("5/s"))` 5× → alle allowed; 6ter → rate-limited, `retry_after_ms > 0`
- Isolation: `check("task_a")` rate-limited, `check("task_b")` mit gleicher Config OK (separate Keys)
- Client-TIME-Konsistenz: Check 10× hintereinander in einer Sekunde, alle bei selbem `window_ts` landen

**Verification:**
- Parser 100 % Branch-Coverage
- Integration-Test grün

---

- [x] **Unit 4: `TaskInfo.rate_limit` Feld + Validierung**

**Goal:** TaskInfo erweitern um `rate_limit: str | None = None`, Validierung beim Erstellen.

**Requirements:** R1

**Dependencies:** Unit 3 (für `parse_rate_limit`)

**Files:**
- Modify: `src/swarmq/task.py` (frozen dataclass, custom `__init__`, `__post_init__`)
- Test: `tests/unit/test_task_info.py` (erweitern)

**Approach:**
- Feld hinzufügen: `rate_limit: str | None = None` in Dataclass-Body + `__init__`-Parameter
- `object.__setattr__(self, "rate_limit", rate_limit)` in `__init__` (frozen-dataclass-Pattern)
- `__post_init__` ruft `parse_rate_limit(self.rate_limit)` wenn nicht None — wirft `ValueError` wenn ungültig
- **Nicht** cachen des parsed Configs in TaskInfo — TaskInfo bleibt deklarativ, Parser läuft im Worker zur Task-Execution-Zeit

**Execution note:** test-first. API-surface.

**Test scenarios:**
- Happy: `TaskInfo(name="t", queues=["q"], rate_limit="100/m")` → OK, `.rate_limit == "100/m"`
- Default: `TaskInfo(name="t", queues=["q"])` → `.rate_limit is None` (kein Breaking Change)
- Error: `TaskInfo(..., rate_limit="100/day")` → `ValueError` propagiert
- Immutability: frozen — kann nicht nachträglich gesetzt werden

**Verification:**
- Bestehende 405 TaskInfo-Tests unverändert grün
- Neue Tests grün

---

- [x] **Unit 5: Worker-Integration + Requeue-Logic + E2E-Test**

**Goal:** Worker prüft Rate-Limit **vor** `run_pre_execute`. Bei Rate-Limited: Signal dispatchen, `publish_to_delay_queue` mit `retry_after_ms`, ack — **kein** DLQ, kein pre_execute. E2E-Test in `tests/e2e/`.

**Requirements:** R1, R2, R3, R4

**Dependencies:** Unit 3, Unit 4

**Files:**
- Modify: `src/swarmq/worker.py` (Integration-Point, Lifecycle für RateLimiter)
- Modify: `src/swarmq/app.py` (leichte Anpassung — siehe Lifecycle)
- Test: `tests/unit/test_worker.py` (erweitern, Mock-Limiter)
- Test: `tests/e2e/test_rate_limiting.py` (neu, end-to-end)

**Approach (Lifecycle — löst Blocker B1 aus Review):**
- `Worker.__init__` bleibt sync, speichert `self._rate_limiter: RateLimiter | None = None`
- `Worker.start()` (async) instanziiert nach `await backend.connect()`: `self._rate_limiter = RateLimiter(backend._client)` — konsistent mit bestehendem `_consume_channel`-Muster (alles das Async-Setup braucht, läuft in `start()`)
- SwarmQ bleibt unverändert (kein `rate_limiter` im Constructor-Flow)

**Approach (Integration-Point in `_handle_message`):**
- Nach `task_info = task_class.task_info`, **vor** `message_dict` Build:
  - wenn `task_info.rate_limit` und `self._rate_limiter`:
    - `config = parse_rate_limit(task_info.rate_limit)` (Parser ist billig, keine Memoization)
    - `allowed, remaining, retry_after = await self._rate_limiter.check(task_name, config)`
    - wenn nicht allowed:
      - Signal `TASK_RATE_LIMITED` mit `(task_id, task_name, retry_after_ms=retry_after, remaining=remaining)`
      - Structured-Log-Zeile mit denselben Feldern
      - `await self._broker.publish_to_delay_queue(queue, message.body, delay_ms=retry_after, headers=message.headers)`
      - `await self._broker.ack(message)` — ursprüngliche Delivery schließen
      - `return` — kein pre_execute, kein run, kein DLQ
- Wenn allowed: normal weiter

**Approach (E2E-Test):**
- `RateLimitedTask` mit `TaskInfo(rate_limit="3/s")`
- Worker starten via `SwarmQ`
- 10 Messages in Reihe publishen
- Warten bis alle 10 in `post_execute` landen (Timeout 10s)
- Assertions: 10 successful, 0 DLQ, ≥7 `TASK_RATE_LIMITED` Signals

**Patterns to follow:**
- `_evaluate_retry` (worker.py:461-540) — Muster "requeue mit Delay statt DLQ"
- `tests/e2e/test_middleware_signals.py` — E2E-Test-Struktur mit SwarmQ-App-Boot

**Test scenarios (Unit, Mock-Limiter):**
- Happy path: Task ohne `rate_limit` → Limiter nicht gerufen, normaler Pfad
- Happy path: Task mit `rate_limit`, Limiter returned `(True, _, _)` → pre_execute wird gerufen
- Rate-limited: Limiter returned `(False, 0, 2500)` → `publish_to_delay_queue(delay_ms=2500)` + `ack`, kein pre_execute, kein DLQ, Signal feuerte
- Isolation: `rate_limit is None` + `_rate_limiter is None` → kein Attributfehler
- Error: Limiter wirft `ConnectionError` → Exception propagiert (Message bleibt für Redelivery)

**Test scenarios (E2E, Docker):**
- Happy: `rate_limit="3/s"`, 10 Messages → alle 10 success, 0 DLQ, ≥7 rate-limited-Events
- Signal-Observability: Handler zählt `TASK_RATE_LIMITED` → matched Rate-Limit-Hits

**Verification:**
- Alle bestehenden 405 Tests grün
- Neue Unit-Tests grün
- E2E grün mit Docker

## System-Wide Impact

- **Interaction graph:** Neue Komponente `RateLimiter` lebt in `Worker`. `TaskInfo.rate_limit` ist neues Public-API-Field (additiv, Default None → kein Breaking Change). Signal-Dispatcher bekommt neuen Call-Site
- **Error propagation:** Valkey-Fehler im RateLimiter propagieren → Message bleibt unacked → Redelivery. Gewünscht
- **State lifecycle risks:** Counter-Keys haben TTL = 2× window_size (selbstreinigend). Keine persistenten Leaks
- **API surface parity:** `TaskInfo.rate_limit` optional. Alle bestehenden Tasks funktionieren unverändert
- **Integration coverage:** Middleware-Pipeline wird bei Rate-Limit bewusst **nicht** durchlaufen. E2E-Test dokumentiert das
- **Unchanged invariants:** DLQ-Routing, `_evaluate_retry`-Logik, Middleware-Pipeline (Hooks unverändert), Signal-Dispatcher-Semantik

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Sliding-Window-Approximation nicht exakt genug für spezielle Use-Cases | Dokumentierter Fehler ~0,003 %; Token Bucket als mögliches Folgefeature |
| Rate-Limit-Check fügt einen Valkey-Roundtrip pro Message hinzu | Nur wenn `rate_limit` gesetzt (opt-in). Kein Overhead für Tasks ohne Limit |
| `client.time()` + `EVALSHA` = zwei Roundtrips | Akzeptabel; Alternative (Lua liest TIME) bricht Konsistenz zur Recherche-Vorlage und hat Replikations-Implikationen |
| Permanent-falsch-konfiguriertes Rate-Limit rotiert Tasks endlos | Explizit als Non-Goal für 3b.1 dokumentiert. Operational concern, Phase 5 |
| `.lua`-File kommt nicht ins Wheel | In Unit 1 verifiziert: Dev-Install (`uv pip install -e .`) + Wheel-Install beide testen |
| NOSCRIPT nach Valkey-Restart | `valkey.asyncio.AsyncScript` macht automatisches `SCRIPT LOAD` + retry — keine eigene Logic nötig |

## Documentation / Operational Notes

- README-Snippet (bei Implementation entscheiden): `TaskInfo(rate_limit="100/m")` als Beispiel
- Solution-Doc bei fertiger Implementation: falls während Implementation Lua/EVALSHA-Lessons entstehen, in `docs/solutions/` dokumentieren
- Operational: Permanent-Rate-Limit-Loops sind operator-sichtbar via `TASK_RATE_LIMITED`-Signal-Rate + `bench_queue.DQ`-Backlog — Alerts in Phase 5

## Sources & References

- **Origin:** `docs/brainstorms/implementation-order-requirements.md` R3.3
- **Lua-Vorlage:** `docs/recherche-deepening-lua-scripts.md` §3.1
- **Algorithmus-Vergleich:** `docs/recherche-rate-limiting-locking-dedup.md` §1
- **Pre-Implementation-Review:** 5 Reviewer, Artifacts unter `.context/compound-engineering/document-review/plan005-*.json`
  - Verifizierte Blocker adressiert: Lifecycle (B1), NOSCRIPT-Redundanz (B2/F3), Clock-Source (B3/F1)
  - Strukturelle Änderungen: Unit 6 → Unit 5, Unit 1 shrinkt auf 10 Zeilen, R7 in R4 gemergt, "Dramatiq-Niveau"-Claim gestrichen
- **Institutional learnings:**
  - `docs/solutions/logic-errors/aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md`
  - `docs/solutions/patterns/measure-before-planning-variance-claims-2026-04-16.md`
- **Nachfolgepläne (nutzen denselben `load_script` + `register_script` Pfad):**
  - 3b.2 — Lock + Semaphore + Unique Tasks (R3.4 + R3.5) — 6 Lua-Scripts
  - 3b.3 — Task Cancellation (R3.6) — 1 Lua-Script + 2 native Ops
