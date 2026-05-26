# E2E-Tests für Feature-Kombinationen auf einer Task

**Status:** Implemented via plan 2026-05-26-002 — committed to branch `feat/e2e-feature-combinations` (133 tests across 4 risk-category files + helpers + infrastructure)
**Datum:** 2026-05-26
**Owner:** Georg Stricker
**Scope:** swarmq_v2 — Erweiterung der `tests/e2e/`-Suite

---

## 1. Motivation

Die bestehende E2E-Suite (`tests/e2e/`, ~6075 LoC über 16 Files) ist
**Feature-First** organisiert: eine Datei pro Einzelfeature
(`test_locking.py`, `test_unique_tasks.py`, `test_scheduler.py`, …).
Was fehlt ist systematische Coverage der **Kombinationen** zwischen
Features auf einer einzelnen Task — z.B. *retry + unique*, *lock +
cancel*, *cron + catchup + rate_limit*.

**Anlass:** Vorsorge, kein konkreter Bug. SwarmQ wird produktiv genutzt
oder bald genutzt; eine fragile Feature-Combo würde heute erst in
Production auffallen, weil kein systematischer Test sie deckt.

**Was NICHT der Anlass ist:** Task-zu-Task Chains, Workflows oder Sagas.
SwarmQ hat dieses Feature derzeit nicht out-of-box; das wäre ein
separater Neubau, kein Test-Vorhaben.

## 2. Goal & Non-Goals

### Goal
Eine deterministische E2E-Test-Suite, die für 31 handgepickte
Feature-Kombinationen jeweils (a) den Happy-Path und (b) ein bis
mehrere Failure-Pfade verifiziert. Bei jeder Combo soll **jede
beteiligte "Station" einmal failen** können — von Task-Exception bis
Worker-SIGKILL — und das erwartete Recovery-Verhalten geprüft werden.

### Non-Goals
- **Exhaustive Combinatorial Coverage.** 14 Feature-Achsen ergeben
  2^14 ≈ 16k Kombinationen — exhaustiv unrealistisch. Wir testen die
  31 nach Risk-Heuristik identifizierten Combos, nicht jede mögliche.
- **Task-zu-Task Chains / Workflows.** Out of scope.
- **Pipeline-Stationen einer Single-Feature-Task** (schedule → publish
  → consume → run → ack). Wird durch existing Single-Feature-Tests
  bereits abgedeckt.
- **Property-Based / Hypothesis-Coverage.** Möglicher Followup-Plan,
  nicht in diesem Scope.
- **Performance-Regression-Tests.** Andere Suite (`test_performance.py`).

## 3. Feature-Achsen (14)

Aus `TaskInfo` (`src/swarmq/task.py:64-82`) plus cross-cutting Features:

| Kategorie | Features |
|---|---|
| **Timing** | `soft_timeout`, `hard_timeout`, `max_age` |
| **Retry** | `max_retries`, `retry_backoff`, `retry_base_delay`, `retry_exceptions` |
| **Concurrency** | `lock`, `max_concurrency`, `rate_limit` |
| **Routing** | `priority`, `queues`, `delivery` |
| **Scheduling** | `cron`, `catchup` |
| **Dedup** | `unique_for`, `unique_until` |
| **Persistence** | `result_ttl` |
| **Cross-cutting** | Cancellation API, Progress API, Middleware-Chain, Signal-Handler |

## 4. Risk-Combinations (31, kategorisiert)

Identifiziert durch 6 Risk-Heuristiken: State-Sharing, Timing/Order,
Failure-Cascades, Concurrency, Worker-Lifecycle, Cross-Process.

### 4.1 State-Sharing (6 Combos)
Zwei Features schreiben in denselben Backend-Eintrag.

1. **`retry + unique_for`** — Wird unique-Hash bei retry erneut acquired oder bleibt er? Bug = identische Task fälschlich geblockt/zugelassen.
2. **`lock + cancel`** — Cancel mitten in gehaltenem Lock: garantiert Lock-Release? Bug = Deadlock für nächste Tasks.
3. **`lock + retry`** — Retry hält Lock weiter, gibt frei, oder acquires neu? Edge: Lock-TTL läuft zwischen Retries ab → zwei parallele Runs.
4. **`lock + hard_timeout`** — Hard-Kill-Worker → stale Lock im Backend. Recovery-Pfad?
5. **`progress + cancel`** — Progress-Update nach `set_cancel_flag` → Race mit Cancel-Detection?
6. **`result_ttl + retry`** — TTL-Start bei Enqueue oder Completion? TTL läuft während Retries ab?

### 4.2 Timing/Order (8 Combos)
Sequenzielle Wechselwirkung; Race-Conditions wahrscheinlich.

7. **`cron + catchup + unique_for`** — Catchup queued N fires, unique blockt N-1. Korrekt verifiziert?
8. **`cron + catchup + rate_limit`** — Catchup-Burst hitting rate-limit → Backlog-Verhalten.
9. **`soft_timeout + cancel`** — Race: welches Signal gewinnt? `TASK_CANCELLED` vs `TASK_FAILED`?
10. **`hard_timeout + retry`** — Hard-Kill → wird retry trotzdem ausgeführt? Lock/unique-State konsistent?
11. **`max_age + scheduler`** — Cron feuert Task die schon älter als `max_age` ist → `TASK_EXPIRED` ohne run?
12. **`rate_limit + retry + retry_backoff`** — Retry hit rate-limit → exponential backoff doubled → endlose Verzögerung?
13. **`priority + lock`** — High-priority Task wartet auf Lock → Reihenfolge in Lock-Queue priorisiert oder strikt FIFO?
14. **`unique_until=start + retry`** — unique released bei `start`, nicht completion → kann zweite Task während retry-Fenster slippen?

### 4.3 Failure-Cascades (6 Combos)
Eine Feature-Aktivierung verändert Failure-Verhalten eines anderen.

15. **`DLQ + unique_for`** — Task in DLQ → unique-Hash noch gehalten → identische Task neuer enqueue geblockt?
16. **`DLQ + retry erschöpft + result_ttl`** — Wird ein result gespeichert oder fehlt es? `get_result()`-Verhalten.
17. **`middleware raise (pre_execute) + signal-handler`** — pre-execute throws → wird `TASK_STARTED` gefeuert? `post_execute`? retry-Pfad?
18. **`middleware raise (post_execute) + retry`** — post-execute throws nach erfolgreichem run → retry getriggert? Bug-Potential: zweimal ausgeführt.
19. **`broker-reconnect mid-publish + at-most-once delivery`** — Connection bricht zwischen publisher_confirm und ack — Message verloren? at-least-once: dup?
20. **`backend-down + result-store`** — Task fertig, will result speichern, backend down → stille Datenverlust?

### 4.4 Concurrency + Lifecycle + Cross-Process (11 Combos)

**Concurrency:**

21. **`max_concurrency=1 + retry`** — Retry vom selben Worker oder anderem? Wenn anderem: gleichzeitiger Run möglich?
22. **`lock + max_concurrency`** — Zwei Concurrency-Layer. Welcher dominiert?
23. **`rate_limit + max_concurrency`** — `rate_limit=10/s, max_concurrency=20` — wer gewinnt?

**Worker-Lifecycle:**

24. **`graceful shutdown + in-flight + retry`** — SIGTERM mit laufender Task → Zuende geführt oder enqueued mit retry?
25. **`hot-reload + in-flight + lock`** — Reload während Lock gehalten → respektiert neuer Worker den Lock?
26. **`burst-mode exit + retry pending`** — `--burst` exit on idle aber Retry-Backoff läuft → exit oder warten?
27. **`worker SIGKILL + lock + at-least-once`** — Gekillter Worker mit Lock + ungeacked Message → re-delivery zu anderem Worker → wartet auf Lock-TTL → Deadlock-Window?

**Cross-Process:**

28. **`scheduler-leader-failover + mid-fire`** — Leader fällt aus mid-fire → übernimmt neuer Leader → Doppelausführung?
29. **`process_manager + worker crash + lock acquired`** — ProcessManager startet Worker neu → re-claimed messages mit Lock-Held-State?
30. **`autoscale shrink + currently-running`** — ProcessManager skaliert runter → wartet auf in-flight oder Kill?
31. **`hot-reload + scheduler-leader`** — Reload während Worker scheduler-leader ist → handover korrekt?

## 5. Failure-Injection-Tiers

Tier 3 ist Scope (alle Tiers + Worker-SIGKILL + Cross-Process).

| Tier | Mechanismus | Beispiel |
|---|---|---|
| **1: Task-Level Exception** | Task-Body throws | `raise RuntimeError("fail")` in run() |
| **2: Connection-Drop mid-op** | Broker/Backend Verbindung manipuliert | Toxiproxy oder Connection.close() zwischen `publish` und `ack` |
| **3: Worker SIGKILL** | Subprocess hart gekillt | `os.kill(pid, SIGKILL)` während Lock gehalten |
| **3: Cross-Process** | Scheduler-Failover, ProcessManager-Shrink | Leader-Worker gekillt, neuer übernimmt mid-fire |

Pro Combo nicht zwingend alle 4 Failure-Modes — nur die, die für die
Combo Sinn machen. Z.B. *`lock + cancel`* wird mit Task-Exception
und SIGKILL getestet, aber Broker-Drop ist weniger relevant.

## 6. Test-Organisation (Approach A — Risk-Kategorie-Files)

```
tests/e2e/combinations/
  __init__.py
  conftest.py                       # shared fixtures: subprocess workers,
                                    # failure-injection helpers, broker/backend
                                    # proxy harness
  test_state_sharing.py             # Combos 1-6
  test_timing_order.py              # Combos 7-14
  test_failure_cascades.py          # Combos 15-20
  test_concurrency_lifecycle.py     # Combos 21-31
```

Jede Combo ist eine `TestClass`, Methoden pro Failure-Mode:

```python
class TestLockCancelCombo:
    """Combo #2: lock + cancel — does cancel guarantee lock release?"""

    async def test_happy_path(self, ...): ...
    async def test_cancel_during_run_releases_lock(self, ...): ...
    async def test_sigkill_during_lock_held_recovery(self, ...): ...
    async def test_concurrent_task_blocked_until_lock_release(self, ...): ...
```

Wiederverwendete existing Patterns:
- `tests/e2e/conftest.py` — `rabbitmq_url`, `valkey_url`, `check_docker`
- `tests/e2e/test_worker_shutdown_load.py` — Subprocess-Worker + SIGKILL-Harness
- `_unique_queue()`, `_task_name()`, `_cleanup_tasks` als Building Blocks

## 7. Open Questions / Deferred Decisions

Diese werden in der Planungs-Phase entschieden, nicht hier:

### 7.1 Pass/Fail-Invarianten pro Combo

Brauchen wir eine **universelle Invariants-Liste** (z.B. always-check
`no_data_loss`, `no_deadlock`, `correct_signal_sequence`, `at-least-
once_guarantee_held`) oder definieren wir pro Combo eigene
Assertions?

**Empfehlung:** Hybrid — eine Basis-Invarianten-Liste (4-5 universelle
Assertions als Helper-Funktion) + Combo-spezifische zusätzliche
Assertions. Aber Detail-Design ist Plan-Scope.

### 7.2 Failure-Injection-Mechanismus für Tier 2

Für **Broker/Backend Mid-Op-Drops** drei Optionen:

- **(a) Toxiproxy** — externes Service-Proxy, präzise Connection-
  Manipulation. Braucht Docker-Compose-Erweiterung.
- **(b) Connection-Mock auf SwarmQ-Level** — `monkeypatch` der
  internal Connection-Objekte. Schneller, aber weniger realistisch.
- **(c) Eigene Fault-Injection-Klasse** — `FaultyConnection`-Wrapper,
  die nach N Operationen disconnect. Mittelweg.

**Empfehlung:** (c) für die meisten Combos, (a) als Fallback für
realistische Network-Failure-Scenarios. Aber Plan-Scope.

### 7.3 CI vs. Nightly

Bei geschätzten 10-15 Min Suite-Laufzeit:

- **CI für PR-Gate:** zu langsam für jede PR, aber kritisch genug für
  Pre-Merge-Validation. Tier 1+2 (~5 min) als PR-Gate, Tier 3 als
  Post-Merge-Nightly?
- **Komplette Suite nightly + Tier 1+2 in PR-CI:** pragmatisch, hält
  PR-Loop schnell, erfasst regressions vor next-day-merge.

**Empfehlung:** Geteilter Lauf — PR-CI nur Tier 1+2, Nightly Tier 3.
Aber Plan-Scope-Entscheidung mit Owner.

## 8. Dependencies / Assumptions

### Dependencies
- Existing `tests/e2e/conftest.py` (docker fixtures) — bleibt nutzbar.
- Subprocess-Worker-Pattern aus `test_worker_shutdown_load.py` — Vorlage.
- Docker-Compose mit RabbitMQ + Valkey (vermutlich existing).
- Optional: Toxiproxy für Tier 2 (Plan-Entscheidung).

### Assumptions
- Alle 14 Features sind ausreichend stabil — wir testen Combos,
  keine neuen Features.
- Existing Single-Feature-E2E-Tests bleiben grün und sind Vorlage.
- 10-15 min Suite-Lauf akzeptabel für Nightly. Falls nicht: weitere
  Tier-3-Reduktion oder Parallel-Pytest.

## 9. Success Criteria

Plan ist erfolgreich, wenn:

1. **Alle 31 Combos haben mind. Happy-Path-Test** in der entsprechenden
   `test_*_*.py`-Datei.
2. **Pro Combo mind. 1 Failure-Mode getestet** — passende Tier-Auswahl
   (mind. eines aus Tier 1, 2, oder 3 je Combo).
3. **Test-Lauf grün auf clean main + Docker.**
4. **Signal-Sequenz-Assertions vorhanden** wo emit-Order kritisch
   (siehe Combos 9, 17, 18).
5. **Suite-Lauf-Zeit unter 15 Min** auf Standard-Dev-Maschine.
6. **Dokumentierter Skip-Pfad** wenn Docker-Services nicht verfügbar
   (analog zu existing `check_docker`-Fixture).

## 10. Next Steps

→ `ce-plan` aufrufen mit diesem Brainstorm als Origin. Plan-Scope:
   - Unit 0: Audit Infrastructure-Setup (Toxiproxy ja/nein, Subprocess-Helpers)
   - Unit 1: `tests/e2e/combinations/conftest.py` — Fault-Injection-Helpers + Universal-Invariants
   - Unit 2-5: Je ein Test-File pro Risk-Kategorie (Combos 1-6, 7-14, 15-20, 21-31)
   - Unit 6: CI-Config-Anpassung (Split PR-Gate vs Nightly)
   - Definition of Done: alle 6 Success Criteria erfüllt
