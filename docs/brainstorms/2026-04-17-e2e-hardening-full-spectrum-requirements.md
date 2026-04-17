# E2E-Hardening — Full-Spectrum Test-Suite

**Status:** Requirements — bereit für Planung
**Datum:** 2026-04-17
**Owner:** Georg Stricker
**Scope:** swarmq_v2

---

## 1. Motivation

Die bestehende E2E-Suite (39 Tests über 9 Dateien) deckt die Happy-Paths und
einige Sad-Paths ab, lässt aber drei Klassen von Bugs ungetestet:

1. **Feature-Parity-Lücken** — nicht jede Public-API hat E2E-Abdeckung.
   `tests/e2e/test_scheduler.py` ist leer. DLQ-CLI (R3.10) hat keine E2E-Tests
   gegen echten Stack. Bulk-Flows mit gemischten Outcomes sind nicht abgedeckt.

2. **Operative Ausfälle** — keine Tests für Worker-SIGKILL mid-task,
   Broker/Backend-Restart, Netzwerk-Partition, Clock-Skew. Die Realität eines
   Task-Queue-Systems *ist* diese Ausfälle — sie zu umgehen testet das System
   nur im Schönwetter-Fall.

3. **Logik-Bugs in Edge-Cases** — kein Property-Based-Testing auf
   Serialization/Dedup/Backoff, kein Fuzz auf Message-Parser, kein
   Mutation-Testing das beweist, dass die Tests scharf sind.

Ziel ist ein System, das **so viele Bugs wie möglich vor Production findet**,
ohne die Dev-Loop zu töten.

---

## 2. Goals

- **G1** — Jede Public-API (Client, SwarmQ, Task, CLI) hat mindestens einen
  Happy-Path- und einen Sad-Path-E2E-Test.
- **G2** — Alle acht definierten Failure-Kategorien (siehe §6) sind durch
  automatisierte Chaos-Tests abgedeckt.
- **G3** — Logik-kritische Pfade (Serialization, Dedup-Hash, Backoff,
  Rate-Limit-Fenster, Progress-Encode/Decode) sind durch Property-Based-Tests
  abgesichert.
- **G4** — Message-Parser (AMQP-Headers, JSON-Payloads, Valkey-HASH-Decode)
  sind durch Byte-/String-Fuzz gegen garbled Input gehärtet.
- **G5** — Mutation-Test-Score ≥ 85% auf den Kernmodulen; beweist, dass die
  Tests scharf genug sind, um kleine Code-Änderungen zu erkennen.
- **G6** — Concurrency-Stress-Suite deckt Worker-Races, Scheduler-Failover
  und Lua-Skript-Atomizität unter Last ab.
- **G7** — Long-Soak-Suite (≥ 1h Last) findet Memory-/FD-/Connection-Leaks.
- **G8** — Drei-Stufen-CI-Pipeline: Pre-merge ≤ 5 min, Nightly ≤ 30 min,
  Weekly ≤ 4h. Jede Stufe in GitHub Actions automatisiert.

---

## 3. Non-Goals / Scope-Boundaries

- **Kein** Multi-Node-Cluster-Testing (RabbitMQ Cluster mit 3 Nodes, Valkey
  Cluster/Sentinel). Single-Broker / Single-Backend reicht für Phase 1.
- **Kein** Kubernetes-/Multi-AZ-Scenario. Docker Compose ist das Ziel-Harness.
- **Kein** Performance-Benchmark-Ersatz. Stress-Tests finden Bugs, nicht
  Throughput-Regressionen — letztere bleiben in den bestehenden Benchmarks.
- **Kein** Differential-Testing gegen alte Versionen (explizit deprioritisiert).
- **Kein** Ersatz für die bestehende Integration- oder Unit-Suite. Die neue
  Arbeit ergänzt, ersetzt nicht. Für Features in §5, die bereits Integration-
  Tests haben (z.B. Scheduler-Leader-Election in
  `tests/integration/test_scheduler_leader_election.py`), prüft der
  Implementierer vor dem Hinzufügen neuer E2E-Tests, ob der bestehende Test
  das Szenario abdeckt; Duplikate werden vermieden.
- **Kein** manuelles Chaos-Engineering-Runbook für Produktion — die Tests sind
  automatisiert und reproduzierbar, keine Production-Game-Days.

---

## 4. Success Criteria

Das Projekt ist abgeschlossen, wenn **alle drei** unten definierten Kriterien
(S1, S3, S4) erfüllt sind. Die Lücke bei S2 ist Absicht: im Brainstorm wurde
"Bug-Finding-Rate" als Kandidat explizit verworfen (organisch, kein klares
Ende). Die Nummerierung ist zur Referenz im Projekt-Tracking beibehalten.

### S1 — Feature-Parity-Matrix vollständig

Eine Tabelle im finalen Plan-Dokument listet jede Public-API-Methode und jede
Feature-Kombination mit zugeordneten E2E-Tests (Happy + Sad). Done = alle
Zellen grün.

### S3 — Coverage-Floor erreicht

- Mutation-Test-Score (via `mutmut`) ≥ 85% auf Kernmodulen:
  `client.py`, `worker.py`, `task.py`, `serialization.py`, `backend/valkey.py`,
  `broker/rabbitmq.py`, `scheduler.py`, `progress.py`, `cancellation.py`,
  `locking.py`.
- Code-Coverage (via `pytest-cov`) ≥ 90% Line + ≥ 85% Branch über `src/swarmq/`.
- Static-Analysis-Gate: `mypy --strict` grün, `ruff check --select ALL` mit
  Ignore-Liste (muss im Plan vor Implementierungsbeginn final festgelegt und
  in `pyproject.toml` gepflegt sein) grün, `bandit -r src/` ohne
  High-Severity.

### S4 — Chaos-Invariants alle grün

Jede Chaos-Szenario-Invariante (definierte Liste in §6) hat einen grünen Test.
Beispiel: "Worker-SIGKILL mid-task mit 50 in-flight Tasks → kein Task ist nach
Redelivery verloren, kein Duplikat-Result wird geschrieben."

---

## 5. Feature-Parity-Scope (G1)

Tests die nachweislich fehlen oder dünn sind:

### 5.1 Scheduler (R3.3)

`tests/e2e/test_scheduler.py` ist leer. Abzudecken:

- Cron-Task fires zu erwarteten Zeitpunkten (Clock-controlled).
- Delayed-Task (`eta=...`) landet nicht vor dem Timestamp.
- Recurring-Task überspringt keine Execution bei Leader-Wechsel.
- Scheduler-Leader-Failover: alter Leader crashed, neuer übernimmt ohne
  verpasste oder doppelte Fires.
- Cron + Rate-Limit: Limit greift auch für Scheduler-getriggerte Fires.
- Cron + Unique: zwei gleichzeitige Scheduler-Fires dedupen korrekt.

### 5.2 DLQ-CLI (R3.10)

Keine E2E-Tests gegen echten Stack.

- `swarmq dlq list` findet vergiftete Messages nach Retry-Exhaustion.
- `swarmq dlq inspect <id>` zeigt Payload + Retry-Count-Header.
- `swarmq dlq retry --filter=...` republisht genau die gematchten.
- `swarmq dlq purge` leert deterministisch.
- Drain-and-republish: DLQ mit N Messages, `list` scannt alle N ohne
  Infinite-Loop.
- Exit-Codes: 0 success, 2 configuration-error, 3 not-found.

### 5.3 Bulk + Mixed Outcomes

- `schedule_many` mit 100 Tasks, von denen 30 raise, 30 cancelled werden, 40
  erfolgen. `get_results` sammelt alle korrekt.
- `schedule_many` mit Unique-Dedup: 50 Originale + 50 Duplikate → 50 Results.

### 5.4 Grosse Payloads

- Task mit 5 MB Argumenten (innerhalb AMQP-Grenze von 128 MB frame).
- Result mit 10 MB payload: `get_result` liefert korrekt, Valkey-Speicher wird
  freigegeben nach TTL.
- Progress-Message mit 1 MB `message`-Field.

### 5.5 Unicode + Edge-Cases

- Task-Args mit Emoji, 4-byte UTF-8, BOM, Zero-Width-Joiners.
- Leere Strings, leere Listen, leere Dicts als Argumente.
- `None`-Werte in nested Structures.

### 5.6 Middleware-Ketten

- Drei Middleware in Kette: alle pre_execute fire in Reihenfolge, alle
  post_execute in umgekehrter Reihenfolge.
- Middleware in der Mitte raised → nachfolgende pre_execute skipped, on_error
  aller vorhergehenden fire.
- Middleware modifiziert Payload → Modifikation ist im Task sichtbar.

### 5.7 Signal-Katalog

Jeder `Signal`-Enum-Wert hat mindestens einen E2E-Test, der den Handler fires:
`WORKER_ONLINE`, `WORKER_SHUTDOWN`, `TASK_ENQUEUED`, `TASK_STARTED`,
`TASK_COMPLETED`, `TASK_FAILED`, `TASK_CANCELLED`, `TASK_RETRY`, ...
(Enum aus `src/swarmq/signals.py` beim Implementieren enumerieren.)

### 5.8 Worker-Shutdown unter Last

- 50 Tasks in-flight, SIGTERM: Tasks laufen zu Ende, keine Re-Delivery.
- Hart-Shutdown (SIGKILL): alle in-flight Tasks redelivered, keine verloren.
- Worker-Shutdown während Task im Retry-Delay: Retry landet im Successor-Worker.

---

## 6. Failure-Taxonomie — Chaos-Szenarien (G2, S4)

Alle acht Kategorien sind in-scope. Im Dokument und Code konsistent als
"Chaos-Szenarien" / "Chaos-Tests" bezeichnet; die hier aufgelisteten F1-F8
sind die Failure-**Kategorien** innerhalb des Chaos-Konzepts. Für jede ist
die Kern-Invariante notiert.

### F1 — Prozess-Crash (Worker-SIGKILL/OOM)

- **Tooling:** `docker kill` oder subprocess + `os.kill(SIGKILL)`.
- **Invariante:** Jeder in-flight Task wird nach Redelivery-Delay entweder
  abgeschlossen oder in DLQ nach max_retries. Keine Results gehen verloren,
  keine Duplikate werden geschrieben (ack-ordering hält).

### F2 — Broker/Backend-Restart

- **Tooling:** `docker restart rabbitmq` / `docker restart valkey` +
  health-check wait.
- **Invariante-Broker:** Worker reconnected automatisch. Queued Messages
  überleben Quorum-Queue-Restart. In-flight Tasks werden redelivered.
- **Invariante-Backend:** Worker überlebt kurze Valkey-Outage. Result-Storage
  wird nach Restart wieder erreichbar. Pub/Sub-Subscriber reconnected.
  Progress-HASH überlebt (Valkey-AOF-Persistenz, falls aktiviert; sonst
  dokumentieren dass Progress verloren geht).

### F3 — Netzwerk-Ausfälle

- **Tooling:** `toxiproxy` als Layer-7-Proxy vor Broker und Backend.
  Szenarios: `timeout`, `latency` (100ms-5s), `bandwidth_limit`, `down`,
  `slicer` (Paketverlust).
- **Invariante:** Unter Latenz-Spike bleiben Tasks in-flight (kein Spurious
  Retry). Unter Full-Down läuft Reconnect-Backoff. Nach Netz-Recovery läuft
  alles weiter ohne Manual-Intervention.

### F4 — Disk-Druck

- **Tooling:** `docker compose` mit `storage-opt size=100M` für
  Valkey/RabbitMQ. Test füllt bis Limit.
- **Invariante-Rabbit:** Producer kriegt Publisher-Confirms-Fehler, kein Data-
  Loss. Worker-Side: Consumer pausiert korrekt.
- **Invariante-Valkey:** OOM-Response `OOM command not allowed when used
  memory > 'maxmemory'` wird gefangen und als BackendConnectionError
  propagiert, nicht als stille Korruption.

### F5 — Resource-Exhaustion (Software)

- **Tooling:** Test-interne Connection-Pool-Limits, Prefetch-Wert auf 1
  setzen und N × 2 Tasks enqueuen.
- **Invariante-Prefetch:** Bei prefetch=1 werden Tasks streng seriell
  verarbeitet, nichts verloren.
- **Invariante-Connection:** Pool-Erschöpfung führt zu sauberem
  BackendConnectionError, nicht zu hängender Coroutine.

### F6 — Clock-Skew

- **Tooling:** `libfaketime` via `LD_PRELOAD` für Worker-Subprocess, oder
  `freezegun` für In-Process-Patches.
- **Invariante-Rate-Limit:** Rate-Limit-Fenster respektiert Monotonic-Clock
  und ist immun gegen Wall-Clock-Rücksprünge.
- **Invariante-TTL:** Cancel-Flag-TTL, Unique-TTL, Result-TTL respektieren
  Valkey-Server-Zeit. Wenn Worker-Clock davon abdriftet, wird nicht frühzeitig
  freigegeben.
- **Invariante-Scheduler:** Cron-Fires orientieren sich an Wall-Clock. Bei
  NTP-Resync ohne doppelten oder verpassten Fire.

### F7 — Message-Korruption

- **Tooling:** Direkter `aio_pika` / `valkey-py` Client im Test, schreibt
  garbled Input.
- **Invariante-AMQP:** Worker mit corrupt Headers rejected die Message
  (→ DLQ), crashed nicht.
- **Invariante-Pub/Sub:** `watch_progress` dropped garbled Payload, Iterator
  bleibt aktiv. (Bereits durch R3.7-Fix gehärtet — Regression-Test.)
- **Invariante-Result:** Corrupt Result-Blob in Valkey wird als
  `ResultSerializationError` surfaced.

### F8 — Byzantine Worker

- **Tooling:** Zwei Worker-Subprocesses, einer mit modifiziertem Task-Code
  (z.B. schreibt altes Result-Schema).
- **Invariante-Schema-Evolution:** Alter Consumer überlebt neue Felder
  (msgspec append-only), neuer Consumer überlebt alte Payloads.
- **Invariante-Dedup:** Zwei Worker-Versionen mit unterschiedlicher
  Hash-Berechnung → wenn Dedup aktiv, dokumentierter Bruch und Test, der
  nachweist *dass* der Bruch passiert (damit bei zukünftigen Refactors
  bewusste Entscheidung möglich ist).

---

## 7. Bug-Hunting-Arsenal (G3–G7)

Sechs Techniken (§7.1–§7.6), mapped auf die fünf Bug-Hunting-Goals G3–G7:
Property-Based (G3), Fuzz (G4), Mutation (G5), Concurrency-Stress (G6),
Long-Soak (G7), Static Analysis (cross-cutting, unterstützt alle Goals).
G8 (CI-Pipeline) ist Infrastruktur und wird in §8 behandelt, nicht hier.

Priorisierung: **1, 4, 6 primär**, **2, 3, 5 sekundär**. Differential-Testing
deprioritisiert.

### 7.1 Property-Based Testing (hypothesis) — primär

Ziel-Module und Invarianten:

- **`canonical_encode` / `compute_dedup_hash`:**
  - Determinismus: gleiche Args → gleicher Hash.
  - Permutation: `{"a": 1, "b": 2}` und `{"b": 2, "a": 1}` → gleicher Hash.
  - Kollisionsfreiheit (probabilistisch): 10k zufällige Inputs, < 0.01%
    Kollisionen.
  - NaN/Inf-Rejection hält für alle Float-Positions (nested).
- **`ProgressInfo.from_hash`:** Roundtrip jedes Dict mit valid Fields.
  Garbled Input raised ValueError (dokumentierte Contract).
- **Serialization (msgspec):** Roundtrip jedes supported Type (int, float,
  str, list, dict, nested, Optional, Union).
- **Retry-Backoff:** Monotonie (`attempt` steigt → `delay` steigt), obere
  Schranke (`max_delay`), Jitter-Range.
- **Rate-Limiter (Lua):** Property: "in jedem gleitenden Fenster der Länge
  `period_ms` werden maximal `limit` Executions zugelassen".

### 7.2 Fuzz-Testing — sekundär

- **Byte-Fuzz auf `_unpack`/`decode_json`:** `st.binary()` gegen die Parser.
  Contract: wirft typed Exception, crashed nie.
- **AMQP-Header-Fuzz:** Publisher schreibt zufällige Header-Werte, Worker
  rejected sauber.
- **JSON-Pub/Sub-Fuzz:** `watch_progress` muss jeden random bytes-Payload
  droppen ohne zu crashen.

### 7.3 Mutation-Testing — sekundär

- Tool: `mutmut` (ausgereifter als cosmic-ray für modernes Python).
- Ziel-Module: siehe S3 oben.
- Laufzeit-Budget: ≤ 3h weekly.
- Policy: mutants die überleben → entweder Test ergänzen oder
  dokumentiertes `# pragma: no mutate` mit Begründung.

### 7.4 Concurrency-Stress — primär

Pattern: ein Test, mehrere Worker/Clients parallel via
`asyncio.gather` + Subprocesses:

- 10 Worker × 1000 Tasks → alle Tasks exakt einmal ausgeführt.
- 100 parallele `schedule`-Calls → 100 Task-IDs, keine Dubletten.
- 50 parallele `cancel`-Calls auf denselben Task → 1 wird True, 49 False.
- Scheduler mit 3 Leader-Kandidaten × 1000 Cron-Fires → kein verpasster Fire.
- 200 parallele `update_progress`-Calls (throttle + final-flush stress).

### 7.5 Long-Soak — sekundär

- 1h Dauer, 100 Tasks/s, konstante Last.
- Messgrössen:
  - Memory-RSS des Workers (kein unbounded Wachstum, ±10 MB Drift).
  - File-Descriptor-Count.
  - Valkey-Connection-Pool-Grösse.
  - RabbitMQ-Channel-Count.
- Fail-Kriterium: ≥ 20% Anstieg einer Messgrösse über die Stunde.

### 7.6 Static Analysis — primär

- `mypy --strict` als Pre-merge-Gate.
- `ruff check --select ALL` mit explicit Ignore-Liste in `pyproject.toml`.
- `bandit -r src/` als Nightly-Job; High-Severity bricht den Build.
- `vulture src/ --min-confidence 80` als Informational-Job (kein Hard-Fail).

---

## 8. Test-Architektur (G8)

### 8.1 Verzeichnis-Layout

```
tests/
├── unit/              (existierend, ~900 Tests)
├── integration/       (existierend, Broker/Backend-spezifisch)
├── e2e/               (existierend + ergänzt — Feature-Parity)
├── chaos/             (NEU — F1–F8, Hybrid-Stack)
├── stress/            (NEU — §7.4 Concurrency-Stress)
├── soak/              (NEU — §7.5 Long-Soak)
├── property/          (NEU — §7.1 Property-Based)
└── fuzz/              (NEU — §7.2 Byte/JSON-Fuzz)
```

### 8.2 Isolation — Hybrid

- **E2E-Suite** nutzt das bestehende Dev-Stack (`:5673`/`:6380`). Schnell,
  parallelisierbar, kein Overhead.
- **Chaos-/Stress-/Soak-Suiten** starten via `docker compose -f
  tests/chaos/compose.yml up` einen dedizierten Stack auf randomisierten
  Ports. Toxiproxy sitzt als Layer-7-Proxy davor. Jede Test-Klasse startet/
  stoppt den Stack (Fixture `scope="class"`), maximal 5 Klassen parallel.
- **Property-/Fuzz-Suiten** brauchen keinen Stack (reine In-Process-Tests).

### 8.3 CI-Staffelung

| Stufe | Trigger | Dauer-Budget | Inhalt |
|---|---|---|---|
| **Pre-merge** | Jeder Push / PR | ≤ 5 min | Unit + E2E + Property-based (quick preset) + Static Analysis |
| **Nightly** | Cron `0 2 * * *` | ≤ 30 min | + Chaos (F1–F8) + Stress + Fuzz |
| **Weekly** | Cron `0 2 * * 0` | ≤ 4h | + Mutation-Testing + Long-Soak |

Alle drei Stufen laufen in GitHub Actions mit scheduled workflows. Flaky-
Chaos-Tests im Nightly werden mit `@pytest.mark.flaky(reruns=2)` markiert
und im Report getrackt — drei konsekutive Failures = Quarantäne (Skip mit
Issue-Referenz).

### 8.4 Pytest-Marker

```python
@pytest.mark.e2e          # bestehend
@pytest.mark.chaos        # neu — Docker-Chaos, serialisiert
@pytest.mark.stress       # neu — hohe Parallelität
@pytest.mark.soak         # neu — long-running
@pytest.mark.property     # neu — hypothesis
@pytest.mark.fuzz         # neu — byte/json fuzz
@pytest.mark.mutation     # neu — nur für mutmut-Config-Zwecke
```

### 8.5 Fixture-Strategie

- **`chaos_stack`** (scope=class): startet dedizierte Compose-Stack, gibt
  URLs + Toxiproxy-Client zurück. Cleanup in Finalizer.
- **`toxiproxy`**: Fixture wrappt `toxiproxy-python`, bietet helper wie
  `proxy.add_toxic(latency_ms=500)` und auto-reset nach Test.
- **`frozen_clock`**: `freezegun` + `libfaketime` Bridge.
- **`worker_subprocess`**: Startet Worker als echter Subprocess (für SIGKILL-
  Tests), gibt Handle zurück.

---

## 9. Risiken & Mitigationen

| Risiko | Mitigation |
|---|---|
| **Chaos-Flakes in CI** blockieren Merges | Chaos nur im Nightly, nicht pre-merge. Quarantäne-Mechanismus (3× fail → skip + Issue). |
| **Mutation-Runtime > 4h** — sprengt Weekly-Budget | Incremental Mutation (nur geänderte Files), Modul-Parallelisierung. |
| **Docker-Overhead in GitHub Actions** — slow startup | Docker-Layer-Caching, Compose-Image-Pinning, parallele Stack-Starts pro Test-Class. |
| **Toxiproxy-Kompatibilität** mit unseren Libs | Vor Implementierung: 30-min Spike mit `toxiproxy + aio_pika + valkey-py`, verifizieren dass unterbrochene Connections sauber reconnecten. |
| **`libfaketime` Linux-only** | Clock-Tests als Linux-only markieren (ist bereits Standard für SwarmQ). |
| **Test-Suite-Wuchs macht Refactoring teurer** | Test-Helpers/Fixtures aggressiv factor-out. Property-Tests > Example-Tests wo möglich. |

---

## 10. Deferred to Implementation

- Konkrete toxiproxy-Topologie (ein Proxy pro Service vs. geteilter Proxy).
- Mutation-Testing-Tool-Wahl final: `mutmut` vs. `cosmic-ray` — entscheiden
  im Plan nach Spike.
- Clock-Tooling final: `libfaketime` (Subprocess) vs. `freezegun` (In-
  Process) — wahrscheinlich beide, je nach Test-Shape.
- GitHub-Actions-Concurrency-Policy: wie viele parallele Chaos-Jobs.
- Coverage-Reporting-Integration (codecov / Coveralls / GitHub-Actions-
  Summary).
- Flaky-Quarantäne-Mechanismus: manuell vs. automatisiert (`pytest-rerunfailures`).

---

## 11. Out-of-Scope — bewusst verschoben

- **Production-Game-Days / Chaos-Engineering-Runbook.** Separate Initiative.
- **Benchmark-Ersatz.** Performance bleibt in `benchmarks/`.
- **Multi-Cluster / Multi-AZ-Szenarien.** Phase 2, falls SwarmQ dorthin wächst.
- **Differential-Testing gegen alte Version.** User-priorisiert nach unten.
- **Linear / Grafana / Datadog-Integration für Test-Metriken.** Nice-to-have,
  nicht für Phase 1.

---

## 12. Referenzen

- Bestehende E2E-Suite: `tests/e2e/`
- Bestehende Integration-Suite: `tests/integration/`
- Dev-Stack: `docker-compose.dev.yml` (`:5673`/`:6380`)
- Pytest-Config: `pyproject.toml` `[tool.pytest.ini_options]`
- Public-API-Quelle: `src/swarmq/__init__.py`, `src/swarmq/app.py`,
  `src/swarmq/client.py`, `src/swarmq/task.py`, `src/swarmq/cli.py`
- Signal-Enum: `src/swarmq/signals.py`
