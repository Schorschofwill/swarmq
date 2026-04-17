---
title: "feat: E2E-Hardening — Full-Spectrum Test-Suite"
type: feat
status: planned
date: 2026-04-17
origin: docs/brainstorms/2026-04-17-e2e-hardening-full-spectrum-requirements.md
---

# feat: E2E-Hardening — Full-Spectrum Test-Suite

## Overview

Implementiert die im Brainstorm spezifizierte Hardening-Initiative:
Feature-Parity in E2E, Chaos-Test-Layer mit toxiproxy, Property-Based +
Fuzz + Mutation + Concurrency-Stress + Long-Soak Suiten, Drei-Stufen-
CI-Pipeline. Ziel: so viele Bugs wie möglich vor Production finden.

Gegliedert in **vier Phasen**, jede als eigenständig shipbarer Block
mit Merge-fähigem Branch. Reihenfolge orientiert sich am Hebel: erst
Foundation + Quick-Wins (maximaler Wert pro LoC), dann Chaos, dann
Bug-Hunting-Arsenal, dann CI-Integration.

## Problem Frame

Die bestehende Test-Suite (886 Unit + ~150 Integration + 39 E2E) deckt
Happy-Paths gut ab, verfehlt aber:

- **Feature-Parity-Lücken.** `tests/e2e/test_scheduler.py` ist leer.
  DLQ-CLI (R3.10) hat 0 E2E-Tests. Bulk-Mixed-Outcome,
  grosse Payloads, Unicode-Edge-Cases, Signal-Katalog, Worker-
  Shutdown unter Last sind dünn oder fehlen.
- **Operative Ausfälle.** Kein Test für Worker-SIGKILL mid-task,
  Broker-Restart, Netzwerk-Partition, Clock-Skew. Das
  Toxiproxy-Spike (2026-04-17) fand zwei reale Bugs, die in der
  bestehenden Suite nicht auffielen.
- **Logik-Bugs in Edge-Cases.** Kein Property-Based-Testing auf
  Serialization/Dedup/Backoff, kein Fuzz auf Message-Parser, kein
  Mutation-Testing das beweist dass die Tests scharf sind.

Das Spike-Ergebnis (`docs/solutions/spikes/2026-04-17-toxiproxy-
reconnect-spike.md`) zeigt zusätzlich einen **dritten Bug**, der
noch nicht gefixt ist: `RabbitMQBroker` behält langlebige Channels
(`_publish_channel`, `_consume_channel`) die nach Connection-Drop
im `ChannelInvalidStateError` sterben. `connect_robust` reconnected
die Connection, aber die Channel-Referenzen bleiben tot. Dieser Fix
kommt in Phase 1.

## Requirements Trace

Direkte 1:1-Übernahme aus dem Brainstorm (§2 Goals, §4 Success,
§5-7 Scope). Tabelle mappt Goal → Phase → Implementation Units.

| Goal | Bedeutung | Phase | Units |
|---|---|---|---|
| G1 | Feature-Parity-E2E | 1 | U1.3–U1.9 |
| G2 | 8 Failure-Kategorien abgedeckt | 2 | U2.1–U2.8 |
| G3 | Property-Based | 3 | U3.1–U3.5 |
| G4 | Fuzz | 3 | U3.6–U3.7 |
| G5 | Mutation ≥ 85% | 3 | U3.10 |
| G6 | Concurrency-Stress | 3 | U3.8 |
| G7 | Long-Soak | 3 | U3.9 |
| G8 | CI-Pipeline 3-stufig | 4 | U4.1–U4.4 |
| S1 | Feature-Parity-Matrix vollständig | 4 | U4.5 (Matrix-Artefakt) |
| S3 | Mutation ≥ 85%, Coverage ≥ 90%, Static grün | 3+4 | U3.10 + U4.2 |
| S4 | Chaos-Invariants alle grün | 2 | U2.1–U2.8 |

## Scope Boundaries

### In-Scope

- Alle §5 Feature-Parity-Lücken des Brainstorms.
- Alle §6 Failure-Kategorien F1–F8.
- Techniken §7.1–§7.6 des Brainstorms.
- GitHub Actions Workflows für die 3 CI-Stufen.
- **Zusätzlich:** `RabbitMQBroker`-Channel-Recreate-Fix
  (Spike-Finding, Phase 1 U1.1).

### Non-Goals

Gemäss Brainstorm §3: kein Multi-Node-Cluster, kein K8s, kein
Differential-Testing, kein Replacement für bestehende Suiten,
kein Production-Runbook.

### Deferred to Implementation

Gemäss Brainstorm §10: toxiproxy-Topologie (ein Proxy pro
Service), mutmut vs. cosmic-ray Final-Entscheidung, libfaketime
vs. freezegun Mix, GitHub-Actions-Concurrency-Policy, Coverage-
Reporting-Integration, Flaky-Quarantäne-Automatisierung.

**Ruff-Ignore-Liste** muss vor U4.2 final festgelegt werden
(S3-Voraussetzung aus Brainstorm).

## Context & Research

### Relevant Code and Patterns

- **Bestehende E2E-Suite:** `tests/e2e/` mit 9 Dateien, nutzt
  `SWARMQ_TEST_RABBITMQ_PORT`/`SWARMQ_TEST_VALKEY_PORT` Env-Vars
  (Dev-Stack `:5673`/`:6380`). Root-Fixtures in
  `tests/e2e/conftest.py`.
- **Broker-Channel-Lifecycle:** `src/swarmq/broker/rabbitmq.py:15-30` —
  zwei langlebige Channels über die Connection-Lebensdauer. Nach
  Toxi-Spike: müssen per Callback auf `connection.reconnect_callbacks`
  lazy neu erstellt werden.
- **Valkey-Retry:** bereits in Phase 0 (Merge `d2d23f2`) konfiguriert.
- **Pub/Sub-Resubscribe:** bereits in `watch_progress` gepatcht.
- **Pytest-Config:** `pyproject.toml` `[tool.pytest.ini_options]` mit
  `asyncio_mode=auto`, `timeout=30`. Markers werden hier ergänzt.

### Key Technical Decisions

- **Chaos-Harness:** toxiproxy (Shopify-Image) als Layer-7-Proxy für
  RabbitMQ + Valkey. Kein `toxiproxy-python`-Package — direktes
  `httpx.AsyncClient` gegen REST-API (Spike-validiert, eine
  Abhängigkeit gespart).
- **Isolation:** Hybrid. E2E bleibt auf Dev-Stack, Chaos startet
  eigenen Compose-Stack via `docker compose -f
  tests/chaos/compose.yml up` auf randomisierten Ports.
- **Property-Based:** `hypothesis` (Industrie-Standard, mature async-
  Support). Profile: `quick` (100 examples, pre-merge) + `thorough`
  (10k examples, nightly).
- **Mutation-Tool:** `mutmut` (gegenüber cosmic-ray: aktiver gepflegt,
  integriert besser mit pytest). Bestätigt im ersten Implementation-
  Spike in U3.10.
- **Clock-Mocking:** Dual. `freezegun` für In-Process-Patches
  (Scheduler-Unit-Tests), `libfaketime` via `LD_PRELOAD` für
  Subprocess-Worker (Worker-Clock-Skew E2E).
- **CI:** GitHub Actions mit `workflow_dispatch` + `schedule`-Cron.
  Jobs matrixisiert für Parallelität. Docker-Layer-Cache via
  `actions/cache`.

### External References

- Spike-Findings: `docs/solutions/spikes/2026-04-17-toxiproxy-
  reconnect-spike.md`
- Brainstorm: `docs/brainstorms/2026-04-17-e2e-hardening-full-
  spectrum-requirements.md`
- toxiproxy REST-API: https://github.com/Shopify/toxiproxy#http-api
- hypothesis async: https://hypothesis.readthedocs.io/en/latest/
  details.html#hypothesis.HealthCheck.function_scoped_fixture
- mutmut: https://mutmut.readthedocs.io/

## Open Questions

### Resolved During Planning

- **Plan-Granularität:** Monolithischer Plan oder 4 Sub-Pläne?
  → Monolithisch mit Phasen. Jede Phase ein Merge-fähiger Branch,
  aber ein Tracking-Dokument.
- **Reihenfolge Chaos vs Feature-Parity:** Feature-Parity zuerst,
  weil Chaos-Tests auf neuen E2E-Tests aufbauen (z.B. Scheduler-
  E2E → Scheduler-Leader-Chaos-Test).
- **Property-Based-Framework:** hypothesis. schema-thesis verworfen
  (Overkill für nicht-HTTP).

### Deferred to Implementation

- Konkrete Toxic-Parameter pro F3-Test (Latency-Range, Packet-Loss-%).
  Erste Werte empirisch im U2.3-Implementation-Loop.
- Mutation-Score-Ziel pro Modul falls 85% nicht erreichbar (z.B.
  Lua-heavy `locking.py` könnte niedriger sein — entscheiden bei
  ersten Results).
- GitHub-Actions-Runner-Wahl: `ubuntu-latest` vs. self-hosted.
  Default `ubuntu-latest` bis CI-Minuten zum Problem werden.
- Long-Soak-Frequenz: weekly vs. monthly. Start mit weekly,
  adjustieren nach Stabilität.

---

## Implementation Units

### Phase 1 — Foundation + Feature-Parity

**Branch:** `feat/e2e-hardening-phase1-foundation`
**Ziel:** Quick-Wins + Infrastruktur. Broker-Fix, Pytest-Marker,
Feature-Parity-Lücken der bestehenden APIs.

#### U1.1 — RabbitMQBroker Channel-Recreate

**Goal:** Langlebige Channels überleben Connection-Reset.

**Files:**
- `src/swarmq/broker/rabbitmq.py` (~+40 LoC)
- `tests/integration/broker/test_rabbitmq_reconnect.py` (neu, ~120 LoC)

**Approach:** Registriere `reconnect_callbacks` auf der
`connect_robust`-Connection. Im Callback: `self._publish_channel =
await self._connection.channel()` + gleiches für consume. Setze Flag
`_channels_ready` das initial False ist und vom Callback True gesetzt
wird. Jede publish/consume-Methode wartet mit Event auf
`_channels_ready` falls False.

**Patterns to follow:** `connect_robust` Callback-Pattern wie in
aio_pika-Dokumentation. Ähnliche Struktur wie Worker-Signal-Dispatcher
(Event-basiertes Warten).

**Test scenarios:**
- Happy path: publish nach Reconnect succeeds.
- Consume: in-flight Message wird nach Reconnect korrekt ack'd.
- Race: publish während Reconnect blockiert bis Channel ready (kein
  Error).
- QoS-Einstellung (prefetch) bleibt nach Reconnect bestehen.

**Verification:** Integration-Test mit toxiproxy (trotz Phase-1-
Status — wir brauchen ihn hier bereits; Compose-Stack wird in U2.0
formalisiert, aber schon in U1.1 genutzt). Nach `toxi.down()/up()`
kann der Broker weiter publizieren UND konsumieren. Alle 886
bestehenden Unit + existing Integration Tests bleiben grün.

**Execution note:** Test-first. Reproduziere das Spike-Verhalten
(`ChannelInvalidStateError` nach Reconnect) als roter Test, dann
fix, dann grün.

---

#### U1.2 — Test-Infrastruktur: Pytest-Marker + Verzeichnis-Layout

**Goal:** Marker-Registrierung und Layout für die neuen Suiten.

**Files:**
- `pyproject.toml` (Marker in `[tool.pytest.ini_options]`)
- `tests/chaos/__init__.py`, `tests/chaos/conftest.py` (neu)
- `tests/stress/__init__.py`, `tests/stress/conftest.py` (neu)
- `tests/soak/__init__.py`, `tests/soak/conftest.py` (neu)
- `tests/property/__init__.py` (neu)
- `tests/fuzz/__init__.py` (neu)

**Approach:** Marker `chaos`, `stress`, `soak`, `property`, `fuzz`,
`mutation` registrieren. Default-Ausschluss via
`addopts = "-m 'not chaos and not stress and not soak'"` in
pyproject.toml (nur Unit+Integration+E2E laufen by default). Opt-in
via `pytest -m chaos`.

**Patterns to follow:** bestehende `e2e`-Marker-Registrierung.

**Test scenarios:** `pytest --collect-only -m chaos` findet 0 Tests
(noch keine geschrieben), aber failed nicht. `pytest` (default)
sammelt nur bestehende Suiten.

**Verification:** `uv run --extra test pytest --collect-only 2>&1 |
grep -c "collected"` zeigt unverändert 886 Tests. `pytest -m chaos`
exitet mit Code 5 (no tests collected) ohne Marker-Warning.

---

#### U1.3 — E2E: Scheduler (bisher leer)

**Goal:** Cron, delayed, recurring Tasks + Leader-Failover E2E.

**Files:**
- `tests/e2e/test_scheduler.py` (füllen, ~250 LoC, 6 Tests)

**Approach:** Zeitgesteuerte Tests mit `freezegun` für Scheduler-
interne Clock. Broker-Events werden über Signal-Dispatcher
mitgeschnitten. Für Leader-Failover: zwei Scheduler-Instanzen,
einer wird via `app.__aexit__` terminiert mid-election.

**Patterns to follow:** `tests/integration/test_scheduler_leader_
election.py` nutzt bereits zwei Scheduler-Instanzen — Pattern
übernehmen, aber mit echtem Stack statt Mocks.

**Test scenarios:**
- Cron fires zu erwarteten Zeitpunkten (Clock-controlled).
- Delayed eta=+3s landet nicht vor Timestamp (+/- 200ms tolerance).
- Recurring überspringt keinen Fire bei Leader-Wechsel.
- Leader crashed, neuer Leader übernimmt ohne doppelte/verpasste Fires.
- Cron + Rate-Limit: Limit greift auch für Scheduler-Fires.
- Cron + Unique: zwei konkurrierende Scheduler dedupen korrekt.

**Verification:** Alle 6 Tests grün gegen Dev-Stack.

**Execution note:** Test-first erschwert wegen Zeitsteuerung —
pragmatisch: erst Test-Harness schreiben, dann happy-path grün
machen, dann Edge-Cases.

---

#### U1.4 — E2E: DLQ-CLI (R3.10)

**Goal:** `swarmq dlq list/inspect/retry/purge` gegen echten Stack.

**Files:**
- `tests/e2e/test_dlq_cli.py` (neu, ~300 LoC, 7 Tests)

**Approach:** Task der garantiert in DLQ landet (`NoRetry` wirft),
schedule + consume bis DLQ-Eintrag. Dann CLI-Subprocess via
`subprocess.Popen` gegen `SWARMQ_BROKER_URL`/`SWARMQ_BACKEND_URL`.
stdout/stderr-Parsing für list/inspect, exit-code-Check für retry/purge.

**Patterns to follow:** `tests/integration/test_cli_worker.py`
nutzt bereits das Subprocess-CLI-Pattern.

**Test scenarios:**
- `dlq list` findet N vergiftete Messages (Happy).
- `dlq inspect <id>` zeigt Payload + retry-count-Header.
- `dlq retry <id>` republisht genau diese.
- `dlq retry --all --filter=task_name=X` republisht alle matching.
- `dlq purge` leert deterministisch (list danach → 0).
- Drain-and-republish: 50 DLQ-Messages, `list --limit 100` scannt alle.
- Exit-Codes: 0 success, 2 config, 3 not-found.

**Verification:** 7/7 grün.

**Execution note:** Pragmatisch — Subprocess-Tests sind langsam,
parallelisierbar via pytest-xdist erst in U4.4 interessant.

---

#### U1.5 — E2E: Bulk + Mixed Outcomes

**Goal:** `schedule_many` mit Mischung aus success/error/cancel/dup.

**Files:**
- `tests/e2e/test_bulk_mixed.py` (neu, ~180 LoC, 3 Tests)

**Approach:** Single Queue, multi-Task-Class Registry. Tasks:
SuccessTask, FailingTask, SlowTask (für cancel), StableTask (für
unique).

**Test scenarios:**
- 100 Tasks (30 raise, 30 cancelled mid-flight, 40 succeed) →
  `get_results` sammelt alle korrekt (errors als Exceptions).
- 50 Originale + 50 unique-Duplikate → 50 unique Results.
- `schedule_many` mit partiellem Broker-Outage: Messages, die
  während des Outages enqueued werden, landen nach Recovery
  (benötigt U1.1 Broker-Fix).

**Verification:** 3/3 grün.

---

#### U1.6 — E2E: Grosse Payloads + Unicode + Edge-Cases

**Goal:** Ungewöhnliche Inputs überleben Roundtrip.

**Files:**
- `tests/e2e/test_edge_inputs.py` (neu, ~150 LoC, 8 Tests)

**Test scenarios:**
- 5 MB Task-Args überleben Publish + Consume + Result.
- 10 MB Result-Payload, `get_result` liefert korrekt, TTL räumt auf.
- 1 MB Progress-`message`-Field wird getruncatet oder durchgereicht
  (Contract dokumentieren, Test pinnt Verhalten).
- Unicode: Emoji, 4-byte UTF-8, BOM, Zero-Width-Joiner in Args+Result.
- Leere Strings, leere Listen, leere Dicts, None-Values.
- Deeply nested (8 Levels) dict als Arg.
- Float-Edge-Cases: `1e300`, `1e-300`, subnormal (NaN/Inf werden
  bereits rejected per Unique-Hash-Code — Regression-Test).

**Verification:** 8/8 grün.

---

#### U1.7 — E2E: Middleware-Ketten

**Goal:** Mehrere Middleware in Kette verhalten sich konsistent.

**Files:**
- `tests/e2e/test_middleware_chains.py` (neu, ~120 LoC, 4 Tests)

**Test scenarios:**
- 3 Middleware Kette: pre_execute in order, post_execute reversed.
- Middleware in der Mitte raised → nachfolgende pre skipped, on_error
  aller vorhergehenden fire.
- Middleware modifiziert Payload → in Task sichtbar.
- Middleware + Signal-Handler Reihenfolge.

**Verification:** 4/4 grün.

---

#### U1.8 — E2E: Signal-Katalog

**Goal:** Jeder Signal-Enum-Wert hat E2E-Coverage.

**Files:**
- `tests/e2e/test_signal_catalog.py` (neu, ~180 LoC)
- Ein Test pro Signal-Enum-Wert aus `src/swarmq/signals.py`
  (zur Planzeit enumerieren, ~10-12 Tests erwartet).

**Test scenarios:** für jedes Signal: Task-Setup, Handler registriert,
Scenario triggert das Signal, Handler-Assertion.

**Verification:** Alle Signale abgedeckt — Quelle: `grep -c "= " src/
swarmq/signals.py` (Enum-Members) muss ≤ Test-Count.

---

#### U1.9 — E2E: Worker-Shutdown unter Last

**Goal:** SIGTERM/SIGKILL-Verhalten unter in-flight Last.

**Files:**
- `tests/e2e/test_worker_shutdown_load.py` (neu, ~200 LoC, 3 Tests)

**Test scenarios:**
- 50 Tasks in-flight, SIGTERM → alle laufen zu Ende, keine
  Redelivery (graceful).
- 50 Tasks in-flight, SIGKILL → alle redelivered, keine verloren,
  keine Duplikat-Results.
- Worker-Shutdown während Task im Retry-Delay → Retry landet im
  Successor.

**Verification:** 3/3 grün. Cross-reference mit `tests/e2e/test_phase2.
py::test_graceful_shutdown` — nicht duplizieren.

**Execution note:** Echter Subprocess-Worker via `subprocess.Popen`,
`os.kill(pid, sig)`.

---

### Phase 2 — Chaos Layer (F1–F8)

**Branch:** `feat/e2e-hardening-phase2-chaos`
**Ziel:** Alle acht Failure-Kategorien automatisiert getestet.

#### U2.0 — Chaos-Compose-Stack + Toxiproxy-Fixture

**Goal:** Dediziertes Compose-Harness für alle Chaos-Tests.

**Files:**
- `tests/chaos/compose.yml` (neu, ~50 LoC)
- `tests/chaos/conftest.py` (erweitert, ~200 LoC)

**Approach:** Compose-Stack mit RabbitMQ + Valkey + Toxiproxy auf
randomisierten Ports (via `COMPOSE_PROJECT_NAME` mit UUID-Prefix
pro Test-Class). Fixture `chaos_stack` (scope=class) startet + stoppt,
gibt URLs + Toxiproxy-Admin-URL zurück.

Fixture `toxiproxy` (scope=function) wrappt `httpx.AsyncClient` mit
Helper-Methoden: `add_latency(ms)`, `add_bandwidth(kbps)`, `down()`,
`up()`, `reset()`. Auto-reset via finalizer.

**Patterns to follow:** Compose-Struktur aus Spike
(`/tmp/toxispike/docker-compose.yml`).

**Test scenarios:** Meta-Test — `test_chaos_stack_starts_cleanly`.
Stack kommt hoch, toxiproxy erreichbar, alle Health-Checks grün.

**Verification:** Meta-Test grün.

**Execution note:** Pragmatisch. Cache Compose-Bilder lokal so dass
Fixture-Startup < 30s.

---

#### U2.1 — F1: Worker-Prozess-Crash

**Goal:** SIGKILL mid-task verliert keine Tasks, dupliziert keine
Results.

**Files:**
- `tests/chaos/test_worker_crash.py` (neu, ~250 LoC, 4 Tests)

**Test scenarios:**
- 1 Task in-flight, `os.kill(worker_pid, SIGKILL)` → Task redelivered,
  erfolgreicher Abschluss auf anderem Worker, genau 1 Result.
- 50 Tasks in-flight, SIGKILL → alle redelivered (ggf. auf
  Successor), DLQ hat keine Einträge.
- OOM-Simulation: Task mit `ctypes`-Memory-Allocation sprengt
  Container-Limit → Docker-Kill, Redelivery greift.
- Task panic (SIGSEGV via `ctypes.string_at(0)`) → Redelivery, nach
  max_retries DLQ.

**Verification:** 4/4 grün, Invariante „kein Result-Duplikat" geprüft
via `get_results` nach N Redelivery-Runden.

---

#### U2.2 — F2: Broker/Backend-Restart

**Goal:** Worker überlebt Valkey + RabbitMQ Restart.

**Files:**
- `tests/chaos/test_broker_restart.py` (neu, ~200 LoC, 4 Tests)
- `tests/chaos/test_backend_restart.py` (neu, ~180 LoC, 4 Tests)

**Test scenarios (Broker):**
- 10 Tasks in-flight, `docker restart rabbitmq` → alle nach Recovery
  abgeschlossen.
- Quorum-Queue überlebt Restart (message-durable).
- Publisher kann nach Restart weiter publishen (Channel-Recreate U1.1).
- Worker-Reconnect ohne manuellen Reset.

**Test scenarios (Backend):**
- Result-Storage überlebt Valkey-Restart (bei AOF-on — als
  Dev-Stack-Voraussetzung dokumentieren).
- Progress-HASH: Verhalten-Check (verloren bei AOF-off, bleibt bei
  AOF-on — Test pinnt, docstring informiert).
- Pub/Sub-Subscriber reconnected via Resubscribe-Fix (Regression-Test
  für Phase-0-Fix).
- Cancel-Flag überlebt kurze Outage.

**Verification:** 8/8 grün.

---

#### U2.3 — F3: Netzwerk-Ausfälle (Toxiproxy Toxics)

**Goal:** Latenz, Bandbreite, Paketverlust, Full-Down.

**Files:**
- `tests/chaos/test_network_chaos.py` (neu, ~300 LoC, 6 Tests)

**Test scenarios:**
- 500ms Latency auf Valkey → Tasks laufen weiter (kein Spurious-Retry,
  op_timeout respektiert).
- Bandwidth-Limit 10 KB/s auf RabbitMQ → grosse Payloads langsamer,
  aber kein Loss.
- Packet-Loss 30% → Worker + Broker reconnecten, Throughput sinkt,
  aber Completeness intakt.
- Full-Down 10s → Reconnect-Backoff; nach Recovery kein Backlog-
  Verlust.
- Partial-Network-Partition: Worker sieht Broker, aber nicht Backend
  → Tasks hängen, kein Silent-Fail.
- Dauerhafte Disconnection von Pub/Sub → Resubscribe-Fix greift,
  kein Message-Loss post-recovery.

**Verification:** 6/6 grün.

---

#### U2.4 — F4: Disk-Druck

**Goal:** OOM-Responses korrekt surfaced als BackendConnectionError.

**Files:**
- `tests/chaos/test_disk_pressure.py` (neu, ~150 LoC, 3 Tests)
- `tests/chaos/compose.disk.yml` (separate Compose mit storage-opt)

**Test scenarios:**
- Valkey mit `maxmemory 20M` + `maxmemory-policy noeviction`.
  Füllung bis OOM, SET wirft OOM-Response, wird als
  `BackendConnectionError` propagiert (nicht stille Korruption).
- RabbitMQ Disk-Alarm: Producer kriegt Publisher-Confirm-Fehler,
  kein Data-Loss.
- Recovery: nach DEL von Füll-Keys läuft alles weiter.

**Verification:** 3/3 grün.

**Execution note:** `storage-opt` ist CI-hostil. Test als
`@pytest.mark.skipif(not is_local())` falls GitHub Actions den Modus
nicht unterstützt — entscheiden im Implementation-Loop.

---

#### U2.5 — F5: Resource-Exhaustion

**Goal:** Connection-Pool-Erschöpfung, Prefetch-Sättigung.

**Files:**
- `tests/chaos/test_resource_exhaustion.py` (neu, ~180 LoC, 3 Tests)

**Test scenarios:**
- prefetch=1, 20 Tasks, 1 Worker → strikt seriell, keine Verluste.
- 100 parallele Valkey-Clients (Pool-Limit 10) → Warten oder saubere
  `BackendConnectionError`, kein Hang.
- Channel-Limit erreicht → Graceful-Failure mit typed Exception.

**Verification:** 3/3 grün.

---

#### U2.6 — F6: Clock-Skew

**Goal:** Rate-Limit, TTL, Scheduler robust gegen Clock-Drift.

**Files:**
- `tests/chaos/test_clock_skew.py` (neu, ~200 LoC, 4 Tests)

**Approach:** Worker via Subprocess mit `libfaketime` LD_PRELOAD,
Clock auf T+60s vorgestellt. Broker/Backend bleiben auf realer Zeit.

**Test scenarios:**
- Rate-Limit-Fenster nutzt Monotonic, immun gegen Wall-Clock-Rücksprung.
- Cancel-Flag-TTL, Unique-TTL, Result-TTL orientieren an Valkey-Server-
  Zeit (Test prüft dass TTL nicht frühzeitig abläuft bei Worker-Drift).
- Cron-Fires bei NTP-Resync: kein doppelter/verpasster Fire.
- `update_progress` Throttle: Monotonic-basiert (Regression-Test).

**Verification:** 4/4 grün.

---

#### U2.7 — F7: Message-Korruption

**Goal:** Garbled Input crashed System nicht.

**Files:**
- `tests/chaos/test_message_corruption.py` (neu, ~220 LoC, 5 Tests)

**Test scenarios:**
- Direkter `aio_pika`-Client schreibt corrupt Headers → Worker
  rejected (DLQ), crashed nicht.
- Direkter `valkey`-Client schreibt corrupt Result-Blob → `get_result`
  wirft `ResultSerializationError`.
- Rogue Pub/Sub Publisher → `watch_progress` dropped garbled Payload
  (Regression für R3.7-Fix).
- Corrupt unique-hash-Value in Header → Worker rejected vs. akzeptiert?
  Contract pinnen.
- Manuelles HSET gebrochener Progress-HASH → `get_progress` liefert
  None (malformed-Guard, Regression).

**Verification:** 5/5 grün.

---

#### U2.8 — F8: Byzantine Worker

**Goal:** Schema-Evolution hält, Hash-Divergenz dokumentiert.

**Files:**
- `tests/chaos/test_byzantine_worker.py` (neu, ~200 LoC, 3 Tests)

**Test scenarios:**
- Worker A schreibt altes Result-Schema, Worker B liest mit neuem
  Decoder → Erfolg (msgspec append-only hält).
- Worker A schreibt neues Schema, Worker B liest mit altem Decoder →
  Erfolg (unbekannte Felder ignoriert).
- Worker A berechnet unique_hash mit alter Funktion, Worker B mit
  neuer → dokumentierter Bruch, Test nachweist dass Dedup-ID
  abweicht.

**Verification:** 3/3 grün. Letzter Test dokumentiert Failure-Mode,
nicht Fix.

---

### Phase 3 — Bug-Hunting Arsenal

**Branch:** `feat/e2e-hardening-phase3-arsenal`

#### U3.1 — hypothesis-Setup + Profile

**Goal:** Framework einrichten mit Quick + Thorough Profilen.

**Files:**
- `tests/property/conftest.py` (Profile-Registrierung)
- `pyproject.toml` (hypothesis in `[test]` optional-deps)

**Approach:** `hypothesis.settings.register_profile("quick",
max_examples=100)` + `"thorough", max_examples=10_000`. Pre-merge
nutzt quick, nightly thorough via Env-Var.

**Verification:** Meta-Test läuft in Quick-Profile < 1s, in Thorough
< 30s.

---

#### U3.2 — Property: canonical_encode + compute_dedup_hash

**Goal:** Deterministik + Permutation + NaN/Inf-Rejection.

**Files:**
- `tests/property/test_dedup_hash_properties.py` (neu, ~200 LoC)

**Test scenarios:**
- Determinismus: gleiche Args → gleicher Hash (10k Iterationen).
- Permutation: `{a:1,b:2}` == `{b:2,a:1}` Hash.
- Kollisionsrate < 0.01% auf 10k zufälligen Inputs (mit seed für
  Reproduzierbarkeit).
- NaN/Inf in nested Positions raised ValueError.
- Unicode-Equivalence (NFC vs NFD): dokumentierter Contract —
  derselbe String in verschiedener Normalisierung = verschiedener Hash.

**Verification:** 5/5 property tests grün in thorough-Profile.

---

#### U3.3 — Property: ProgressInfo + Serialization

**Goal:** Roundtrip-Invarianten.

**Files:**
- `tests/property/test_progress_properties.py` (neu, ~120 LoC)
- `tests/property/test_serialization_properties.py` (neu, ~180 LoC)

**Test scenarios:**
- ProgressInfo.from_hash(to_fields(info)) == info für jedes
  valid info (roundtrip).
- `encode_result(x); decode_json(...) == x` für alle
  hypothesis-generierten Types (int, float, str, list, dict,
  nested, Optional, Union).
- Nicht-encodierbare Types wirken typed Exception (NaN, Inf,
  datetime).

**Verification:** grün im thorough-Profile.

---

#### U3.4 — Property: Retry-Backoff

**Goal:** Monotonie + Jitter-Grenzen.

**Files:** `tests/property/test_backoff_properties.py` (~100 LoC)

**Test scenarios:**
- Monotonie: `delay(attempt=n+1) >= delay(attempt=n)`
  (bei exp-backoff ohne jitter).
- Jitter-Range: `delay(n)` ∈ `[base*factor^n * (1-jitter), base*factor^n *
  (1+jitter)]`.
- Upper-Bound: `delay(n) <= max_delay` für alle n.

**Verification:** grün.

---

#### U3.5 — Property: Rate-Limiter (Lua)

**Goal:** Window-Invariante.

**Files:** `tests/property/test_rate_limiter_properties.py` (~150 LoC)

**Test scenarios:**
- Über beliebige Sequenz von `allow?`-Calls in einem gleitenden
  Fenster werden ≤ limit akzeptiert.
- Denial ist persistent bis Fenster verschoben ist.

**Verification:** grün. Benötigt Integration mit echtem Valkey (daher
property-Tests mit `@pytest.mark.property` + `chaos_stack`-ähnlicher
Fixture für einen schnellen Valkey-Container).

**Execution note:** Hypothesis + Integration ist langsam. Quick-
Profil = 30 Examples, Thorough = 300. Bewusst reduziert vs. reine
In-Process Properties.

---

#### U3.6 — Fuzz: Byte-Parser

**Goal:** `_unpack`, `decode_json` crashed nie.

**Files:** `tests/fuzz/test_parser_fuzz.py` (~150 LoC)

**Test scenarios:**
- `st.binary()` gegen `_unpack` → immer typed Exception, nie
  unhandled.
- Zufällige JSON-ähnliche Strings gegen `decode_json` → ValueError
  oder ResultSerializationError.

**Verification:** grün.

---

#### U3.7 — Fuzz: AMQP-Header + Pub/Sub-Payload

**Goal:** Externe Producer können Worker nicht crashen.

**Files:** `tests/fuzz/test_external_publish_fuzz.py` (~180 LoC)

**Test scenarios:**
- Direkter aio_pika-Publisher mit `st.fixed_dictionaries()` für
  Headers (zufällige Keys/Values). Worker rejected graceful.
- Direkter Valkey-Publisher mit `st.binary()` in Pub/Sub-Channel.
  `watch_progress` dropped, bleibt lebendig.

**Verification:** grün. Benötigt chaos_stack.

---

#### U3.8 — Concurrency-Stress

**Goal:** Worker-Races, Scheduler-Races, Lua-Atomizität.

**Files:** `tests/stress/test_concurrency.py` (~300 LoC)

**Test scenarios:**
- 10 Worker × 1000 Tasks → jeder Task exakt einmal ausgeführt (kein
  Verlust, kein Duplikat).
- 100 parallele `schedule`-Calls → 100 unique Task-IDs.
- 50 parallele `cancel`-Calls auf denselben Task → genau 1 True.
- Scheduler: 3 Leader-Kandidaten × 1000 Cron-Fires → kein verpasster
  Fire, kein Duplikat.
- 200 parallele `update_progress` → Throttle + Final-Flush hält.
- 50 parallele Rate-Limit-Checks auf demselben Key → ≤ limit akzeptiert.

**Verification:** 6/6 grün.

---

#### U3.9 — Long-Soak

**Goal:** Memory-/FD-/Connection-Leak-Detection über 1h Last.

**Files:** `tests/soak/test_soak_1h.py` (~200 LoC)

**Approach:** 100 Tasks/s konstant, 1h Laufzeit. Messungen via
`psutil` auf RSS, FD-Count, Valkey-Connections (via `CLIENT LIST`),
RabbitMQ-Channels (via Management-API).

**Test scenarios:**
- RSS-Drift < 10 MB / Stunde.
- FD-Count stabil ±5 über die Stunde.
- Valkey-Connections ≤ pool-max konstant.
- RabbitMQ-Channel-Count konstant.

**Verification:** Alle Messgrössen < 20% Drift-Threshold.

**Execution note:** Nur Weekly. Test als `@pytest.mark.soak`, default
Skip.

---

#### U3.10 — Mutation-Testing (mutmut)

**Goal:** Score ≥ 85% auf Kernmodulen.

**Files:**
- `pyproject.toml` (`[tool.mutmut]` config)
- `scripts/run_mutation.sh` (wrapper)
- `.github/workflows/weekly.yml` (wird in U4.4 finalisiert, hier nur
  Mutation-Job-Draft)

**Approach:** `mutmut run --paths-to-mutate src/swarmq/client.py,src/
swarmq/worker.py,...`. Default runner ist der bestehende Unit-Test-
Command. Survivors werden geprüft: Test ergänzen oder `# pragma: no
mutate` mit Begründung.

**Verification:** Initial-Run auf bestehender Test-Basis zeigt Score
X%. Ziel: alle Units von Phase 1-3 gemerged, dann mutation → ≥ 85%.

**Execution note:** Spike-first. Erst 1-Modul-Pilot (client.py) um
mutmut-Konfig zu validieren, dann auf alle.

---

### Phase 4 — CI-Integration + Success-Criteria

**Branch:** `feat/e2e-hardening-phase4-ci`

#### U4.1 — GitHub Actions: Pre-merge Workflow

**Goal:** `.github/workflows/ci.yml` mit Unit + E2E + Property-Quick +
Static-Analysis, ≤ 5 min.

**Files:** `.github/workflows/ci.yml` (neu, ~80 LoC)

**Approach:** Job-Matrix für Python 3.12/3.14. Docker-Services
RabbitMQ + Valkey als GitHub-Actions-`services`. Compose-Cache via
`actions/cache`.

**Verification:** Workflow grün auf einem PR, Laufzeit < 5 min.

---

#### U4.2 — Static-Analysis-Gates

**Goal:** mypy --strict + ruff + bandit als Gates.

**Files:**
- `pyproject.toml` (mypy strict-Sektion, ruff-Ignore-Liste final)
- `.github/workflows/ci.yml` (Static-Job ergänzen)

**Approach:** mypy-Strict schrittweise einführen — erst pro Modul via
`[[tool.mypy.overrides]]`, dann alle. Ruff-Ignore-Liste aus bereits
vorhandenen Rules ableiten, Bandit-High-Severity-Only-Failure-Modus.

**Verification:** Alle Gates grün oder mit dokumentierten Ignores.

**Execution note:** Ruff-Ignore-Liste ist S3-Voraussetzung (Brainstorm
§4) — vor Merge final.

---

#### U4.3 — Nightly + Weekly Workflows

**Goal:** Chaos + Stress + Fuzz + Mutation + Soak scheduled.

**Files:**
- `.github/workflows/nightly.yml` (neu, cron `0 2 * * *`)
- `.github/workflows/weekly.yml` (neu, cron `0 2 * * 0`)

**Approach:** Nightly ≤ 30 min: Chaos + Stress + Fuzz + Property-
Thorough. Weekly ≤ 4h: + Mutation + Soak. Benachrichtigung bei
Failure via GitHub Issue (bot).

**Verification:** Beide Workflows laufen manuell via
`workflow_dispatch` erfolgreich.

---

#### U4.4 — Flaky-Quarantäne-Mechanismus

**Goal:** 3× fail-in-a-row → Auto-Skip mit Issue.

**Files:**
- `tests/chaos/conftest.py` (Retry-Wrapper via pytest-rerunfailures)
- `scripts/flaky_quarantine.py` (neu, Failure-Log-Parser)

**Approach:** `@pytest.mark.flaky(reruns=2, reruns_delay=5)` auf
allen chaos-Tests. Nightly-Log wird geparst; >3 konsekutive Failures
desselben Tests → script erstellt GitHub-Issue + skipt Test via
Marker-Override in `pyproject.toml` (Commit via bot).

**Verification:** Künstlicher flaky-Test (sleep-random) wird nach 3
Läufen quarantäniert.

**Execution note:** Pragmatisch. Auto-Commit-Bot ist Scope-Expansion
— Phase-1-Version ist manuelle Issue-Erstellung, Quarantäne manuell.

---

#### U4.5 — Feature-Parity-Matrix + Dokumentation

**Goal:** S1 als Artefakt — Matrix macht Parity sichtbar.

**Files:**
- `docs/testing/feature-parity-matrix.md` (neu, ~200 LoC)
- `docs/testing/test-strategy.md` (neu, ~150 LoC)

**Approach:** Matrix-Tabelle: Zeilen = Public-API-Methoden + Feature-
Kombinationen, Spalten = {happy-E2E, sad-E2E, chaos, property, fuzz,
stress}. Jede Zelle verweist auf Testfile + Testname oder "n/a".

**Verification:** Matrix vollständig, kein leeres happy-oder-sad-Feld
für eine Public-API.

---

## System-Wide Impact

- **Dev-Loop:** `uv run --extra test pytest` bleibt ≤ 15s (default-
  Marker-Ausschluss). Chaos/Stress/Soak sind opt-in.
- **CI-Minuten:** Pre-merge + Nightly + Weekly summieren sich —
  ~500 CI-Minuten / Monat geschätzt. Falls Engpass, Selfhosted-
  Runner als Fallback.
- **Repository-Grösse:** +~6000 LoC Tests, +3 neue Top-Level-
  Verzeichnisse. Docs + Workflows +~500 LoC.
- **Dependencies:** `hypothesis`, `mutmut`, `pytest-rerunfailures`,
  `httpx` (falls nicht bereits), `freezegun`, optional `libfaketime`
  (apt-paket). Alle in `[test]` optional-deps, nicht im Default-
  Install.
- **Developer-Doku:** `docs/testing/` mit Strategie + Matrix + Chaos-
  Runbook (wie lokal ausführen).

## Risks & Dependencies

| Risiko | Mitigation |
|---|---|
| Chaos-Flakes blockieren Dev-Loop | Default-Ausschluss via Marker; nur Nightly. |
| toxiproxy funktioniert anders als erwartet | Spike (2026-04-17) validiert Grundfunktion; Edge-Cases im Implementation-Loop. |
| mutmut zu langsam für 4h-Budget | Incremental via `--paths-to-mutate` auf geänderte Files. |
| Disk-Pressure-Tests CI-hostil | Als local-only via `is_local()`-Helper. |
| Auto-Quarantäne falsch-positiv | Phase-1-Version manuell (nur Issue, kein Auto-Skip). |
| Channel-Recreate-Fix (U1.1) bricht bestehenden Code | Test-first + gesamte Suite grün-Gate. |

**Dependencies:**
- Phase 2 abhängig von U1.1 (Channel-Recreate) und U2.0 (Compose-
  Stack).
- Phase 3 U3.10 abhängig von abgeschlossener Phase 1+2 (damit
  Mutation gegen finalen Testkörper läuft).
- Phase 4 abhängig von allen vorherigen Phasen.

## Documentation / Operational Notes

- Neue `docs/testing/`-Sektion: Strategie-Overview, Matrix, Chaos-
  Runbook (wie lokal ausführen, wie debuggen, wie neue Szenarien
  hinzufügen).
- `AGENTS.md` ergänzen: wie neue Tests korrekt markieren (welcher
  Marker, welches Verzeichnis).
- Bei jedem Phase-Merge: Phase-Completion-Note in `CHANGELOG.md`.

## Sources & References

- Brainstorm: `docs/brainstorms/2026-04-17-e2e-hardening-full-
  spectrum-requirements.md`
- Spike: `docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-
  spike.md`
- Spike-Commit (Bugs gefixt): `d2d23f2`
- Phase-3-Completion (R3.5 + R3.10 + R3.7): main branch
- Existing E2E-Suite: `tests/e2e/`
- Public-API-Surface: `src/swarmq/__init__.py`, `app.py`, `client.py`,
  `task.py`, `cli.py`
- Signal-Enum: `src/swarmq/signals.py`
