---
title: "fix: Produktionssicherheits-Härtung — 2 P0 + 6 P1 + P2"
type: fix
status: in-progress
date: 2026-06-02
origin: 4-Dimensionen-Produktionssicherheits-Audit 2026-06-02 (Reliability/Data-Integrity/Security/Operational), alle Findings am Code verifiziert
---

# fix: Produktionssicherheits-Härtung

## Summary

Audit per vier parallelen Subagenten (Reliability, Data-Integrity, Security,
Operational). Beide P0 wurden anschließend manuell am Kontrollfluss verifiziert.
User-Scope: **alles inkl. P2-Härtung**, TDD. Reihenfolge: P0 → P1 → P2, in
logischen Commit-Gruppen. Tests, die echte Infra brauchen, laufen im
integration/e2e-Tier; reine Logik wird unit-getestet.

## P0 — brechen Kerngarantien

### P0-A · Task-Lock-TTL bricht Mutex bei `hard_timeout > 30s`
`compute_lock_ttl_ms` (`locking.py:41-66`) = `max(30s, 0.25·hard_timeout)`.
`renew()` läuft nur für den Scheduler-Leader-Lock, nie für Task-Locks. Für
jedes `hard_timeout > 30s` ist die TTL kürzer als die erlaubte Laufzeit → Lock
läuft mitten in der Ausführung ab → zweiter Worker greift denselben Mutex →
Doppelausführung.
**Fix:** TTL so wählen, dass sie die maximale Laufzeit überdeckt — TTL =
`hard_timeout + Puffer` (Watchdog cancelt bei `hard_timeout`, danach cooperative
release; bei hartem Crash wartet der Nachfolger TTL). Docstring korrigieren.
**Tests:** `compute_lock_ttl_ms`-Tabelle (ht=10/30/60/600 → TTL ≥ ht); Property
„TTL ≥ hard_timeout für alle ht".

### P0-B · Scheduled-Task-Retries werden still gedroppt
Retry-Republish behält `x-swarmq-schedfire` (`worker.py:2410`); `finally`
schreibt unbedingt den Long-TTL-completed-Marker (`worker.py:2179`); die
Retry-Kopie wird beim Dedup-Check ge-ackt+geskippt (`worker.py:1189/1202`).
**Fix:** completed-Marker nur bei **terminalem** Outcome. Flag analog
`_unique_release_pending` (z. B. `_schedfire_terminal`), das im Retry-/Requeue-/
shutdown-nack-Pfad auf „nicht terminal" gesetzt wird; `_mark_schedfire_completed`
poppt dann nur den inflight-Claim (läuft per kurz-TTL aus → Retry-Kopie
re-claimt), ohne den completed-Marker zu schreiben.
**Tests:** e2e — scheduled Task mit `max_retries=1`, erster Lauf wirft, Retry
läuft und finished; Regression: completed-Marker erst nach terminalem Lauf.

## P1 — ernst

- **P1-1** `store_result` ohne `op_timeout` an 3 Stellen (`worker.py:254/2064/2450`) → `op_timeout=self._config.backend_op_timeout` ergänzen. Test: store_result-Call übergibt op_timeout.
- **P1-2** `cancel_consume` ohne Fehlerisolation (`rabbitmq.py:314-318`) → per-Consumer try/except, `_consumer_tags` immer leeren; Aufruf in `_execute_shutdown` gewrappt. Test: ein cancel wirft → restliche werden trotzdem gecancelt.
- **P1-3** Metric-Gauges nie gesetzt (`metrics.py`) → periodischer Sampler in `Worker.start()`: `queue_depth`/`dlq_messages` per `broker.get_queue_depth`, `worker_uptime_seconds`. Test: Sampler setzt Gauges.
- **P1-4** DLQ-Move ohne Signal/Metric (`worker.py:2433`) → neues `Signal.TASK_DLQ` + Handler in `install_signal_collectors` (`outcome="dlq"`). Test: _send_to_dlq dispatcht, Counter steigt.
- **P1-5** kein Scheduler-Leader-Gauge → `swarmq_scheduler_leader{worker_id}` aus `SCHEDULER_LEADER/FAILOVER`=1, `HANDOVER/SHUTDOWN`=0. Test: Signal → Gauge.
- **P1-6** Broker-URL-Passwort-Leak (`rabbitmq.py:193/200`) → `sanitize_url()`. Test: BrokerConnectionError-Text enthält kein Passwort.

## P2 — Härtung

- Semaphore-Renewal implementieren (`lock_renew.lua` für max_concurrency>1) ODER Semaphore-TTL ≥ hard_timeout koppeln (gleiche Wurzel wie P0-A) + dokumentieren.
- `max_retries=0`-Erschöpfung über `_send_to_dlq` routen (DLQ-Inspektion auch hier).
- handover-marker token-scoped statt „alle löschen" (`valkey.py:714-731`).
- `Signal.SCHEDULER_LEADER`-Missed-Cron: optional `cron_fire_missed`-Signal wenn `now-score > Intervall` (`scheduler.py:824`).
- `decode_result` unknown-ext-code → fail-loud statt stiller `ExtType` (`serialization.py:206`).
- args/kwargs-Typvalidierung (`isinstance(args,list)`/`(kwargs,dict)`) vor `run(*args,**kwargs)` (`worker.py:1509`) → sonst DLQ.
- Tote Enums beleben: `WORKER_HEARTBEAT` (periodischer Tick im Sampler aus P1-3), `SCHEDULER_STANDBY` (Emit im `_election_tick`-standby-Branch). Beide an Metrik koppeln.
- Log-Queue-Drop-Counter / Doku (`logging.py:68`).
- `/health` optionaler Consumer-Staleness-Check (`health.py:186`) — mind. dokumentieren.

## Verifiziert sauber (kein Handlungsbedarf)
Keine Deserialisierungs-RCE, keine Arbitrary-Dispatch, kein Lua-Injection,
Publisher-Confirms an, `x-delivery-limit=20` gesetzt, Split-Brain-State-Schutz,
Graceful-Shutdown/ProcessManager reif. (feature-parity-matrix + SIGSEGV-Skip-
Begründung bzgl. „kein x-delivery-limit" sind VERALTET → mit korrigieren.)
