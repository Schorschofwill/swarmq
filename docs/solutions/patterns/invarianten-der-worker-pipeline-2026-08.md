---
title: "Pattern: Die vier tragenden Invarianten der Worker-Pipeline und ihre strukturelle Erzwingung"
date: 2026-08-27
category: patterns
module: src/swarmq/worker/, src/swarmq/contracts/, src/swarmq/workflow_orchestration.py
problem_type: invariant_enforcement
component: worker
symptoms:
  - "Invarianten lebten vor 0.2.0 verstreut in Kommentaren der Monolith-Module (worker.py 2808 Zeilen)"
  - "Review-Runden (Audit 2026-06-02, Härtung R2) fanden dieselben Invarianten-Verletzungen mehrfach"
root_cause: invariants_in_comments_not_structure
---

# Die vier tragenden Invarianten der Worker-Pipeline (Stand 0.2.0)

Sammelstelle nach Plan U13 (R6, R10): Für jede Invariante steht hier der
Mechanismus, der sie seit der 0.2.0-Restrukturierung **strukturell** erzwingt,
und wo die Provenance-Historie liegt. Die Code-Kommentare erklären nur noch
das Warum vor Ort; die Review-Zitate (P0-/P1-/P2-IDs, ce-review, Härtung R2,
Audit 2026-06-02) sind aus `src/` entfernt — die Historie liegt in der
Git-Historie der jeweiligen MRs (!8, !10, !12) und in
`docs/solutions/logic-errors/` bzw. den Härtungs-/Audit-Notizen.

## 1. Ack-Ordering — „kein Broker-Verb vor dem letzten Gate, genau ein Verb pro Zustellung"

**Mechanismus (U7/U19):** Die Gates (`worker/gates/`) geben ein
`DeliveryOutcome` zurück statt selbst zu bestätigen; nur der Runner
(`pipeline._handle_message`) und der Ergebnis-Tail (`worker/results.py`)
sprechen die vier Vertragsverben `ack`/`redeliver`/`retry_later`/`dead_letter`
(KTD2). `retry_later`/`dead_letter` bestätigen das Original selbst — ein
zweites Verb wäre ein Doppel-Ack und fällt im Contract-Test bzw. am
InMemory-Stub (`tests/support/inmemory/broker.py`) auf.
**Test-Anker:** `tests/unit/worker/test_pipeline.py` (Ordnungstest: kein Verb
vor dem letzten Gate; FAF-Modus `acked_before_execute`).

## 2. Terminal-Flags — früher `_unique_release_pending`/`_schedfire_terminal`

**Mechanismus (U19, KTD5):** Ersetzt durch den Summentyp
`worker/outcome.py` — `Continue`/`Redelivered`-Singletons und
`Terminal(release_unique, finalize_schedfire)` mit genau drei benannten
Konstruktoren (`release_in_finally()`, `already_released()`,
`never_release()`). Der `finally`-Block des Runners liest ausschließlich das
Outcome; einen „vergessenen Flag-Reset" gibt es nicht mehr, weil kein
mutierbarer Flag-Zustand existiert.
**Test-Anker:** `tests/unit/worker/test_pipeline.py` (drei
Terminal-Varianten, unbehandelte Exception → `Redelivered`).

## 3. Owner-Checks — Locks und Dedup-Flags gibt nur der Besitzer frei

**Mechanismus (U18, KTD4):** Die Verträge erzwingen Ownership in der
Signatur: `LockStore.acquire` liefert einen `LockToken` (Name+Owner+
max_concurrency), `release`/`renew` nehmen nur den Token;
`ResultBackend.claim_unique`/`release_unique` verlangen den Owner-String.
Die Lua-Skripte (`lock_release.lua`, Owner-Vergleich beim Unique-Release)
bleiben die Laufzeit-Wahrheit; die Contract-Suite
(`tests/contracts/coordination.py`) prüft Fremd-Owner-Ablehnung gegen Stub
und Valkey identisch.
**Test-Anker:** `tests/integration/test_contracts_valkey.py`
(Koordinationsverträge), `tests/unit/backend/test_valkey_locks.py`.

## 4. publish-then-mark — Workflow-Schritt erst als enqueued markieren, wenn der Broker bestätigt hat

**Mechanismus (U20, AE5):** `WorkflowAdvancer.mark_enqueued(state, step_path,
task_id, receipt)` verlangt einen `PublishReceipt` als Pflichtargument —
statisch (mypy strict) und zur Laufzeit (`TypeError` bei anderem Typ). Den
Receipt gibt es nur als Rückgabe von `Publisher.publish` (Publisher-Confirms),
also kann der Mark-Schritt dem Publish strukturell nicht vorausgehen. Der
Publish-Callable-Typ des Advancers ist `… -> Awaitable[PublishReceipt]`.
**Test-Anker:** `tests/unit/test_workflow_advancer.py` (Laufzeit-TypeError),
`tests/unit/typing/` (negative mypy-Fixture, AE5).

## Begründete Ausnahme

Die Dead-Letter-Fallbacks sind **je Aufrufstelle verschieden** (Retry/Expiry
→ `redeliver`; Lock-Pfade → `route_via_dlx`) und bleiben bewusst
uneinheitlich (KTD3) — gepinnt durch die Characterization
`tests/integration/broker/test_rabbitmq_fallbacks.py`, Vereinheitlichung
frühestens nach 0.2.0 (U14-Inventar).
