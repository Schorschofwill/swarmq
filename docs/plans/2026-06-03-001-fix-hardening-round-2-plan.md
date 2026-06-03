---
title: "fix: Härtungs-Runde 2 — 1 P0 + 10 P1 + 13 P2 + 6 Test-/CI-Lücken"
type: fix
status: in-progress
date: 2026-06-03
origin: 6-Bereiche-Härtungs-Review 2026-06-03 (Worker-Lifecycle, Verbindungsschicht, Scheduler/Lua, Security, Operations, Client/Workflows/Tests), alle Findings am Kontrollfluss verifiziert
---

# fix: Härtungs-Runde 2

## Summary

Review per sechs parallelen Subagenten über alle Teilbereiche; jeder Befund am
tatsächlichen Kontrollfluss verifiziert, refutierte Hypothesen dokumentiert.
User-Scope: **alles** (P0 + P1 + P2 + Test-/CI-Lücken), TDD. Cluster-Befund
(P1-7): Hash-Tags einführen (User-Entscheidung; Breaking für bestehende
Schedule-Keys → Migrationshinweis in Doku).

Umsetzung in 6 Arbeitspaketen (U1–U6), je in logischen Commit-Gruppen.

## U1 — ProcessManager-Lifecycle

### P0-1 · Crash-Loop-Schutz im `--processes`-Modus umgangen (Hot-Loop)
Nach 5 Restarts greift `_maybe_restart`-Limit, aber die Backfill-Schleife in
`_monitor_loop` (`process_manager.py:519-524`) füllt auf `initial_workers` auf —
als neue `WorkerInfo` mit leerer `restart_timestamps`-Historie. Instant-crashende
Worker → endloser Spawn-Sturm ohne Backoff. Autoscale-Pfad ist nicht betroffen
(`_current_target` = `len(self.workers)`).
**Fix:** Limit-erschöpfte Slots zählen (`_slots_exhausted`), Backfill-Ziel um
erschöpfte Slots reduzieren; sind ALLE Slots erschöpft → sauberer Manager-Exit
mit Exit-Code ≠ 0 (systemd/k8s übernimmt mit eigenem Backoff). Restart-Historie
an logische `worker_id` binden, nicht ans transiente `WorkerInfo`-Objekt.
**Tests:** echte (instant-exit) Subprozesse statt gemocktem `_monitor_loop`:
Limit erreicht → kein Respawn-Sturm, Manager beendet sich mit Fehlercode.

### P1-1 · SIGCHLD `waitpid(-1)`-Race gegen `multiprocessing`
`_on_sigchld` reapt mit `waitpid(-1, WNOHANG)` auch Kinder, die
`multiprocessing.Process` besitzt → `Popen.poll()` bekommt `ECHILD` → `is_alive()`
dauerhaft `True` für tote Worker → kein Restart; Shutdown wartet volle Grace.
**Fix:** Reaping aus dem Handler entfernen — Handler nur noch Wake-Signal;
Reaping ausschließlich via `multiprocessing`-API (`_check_dead_workers`/`join`).
**Tests:** echter Kurzlebig-Subprozess: nach Tod wird er erkannt + restarted.

### P1-8 · Supervisor loggt nie JSON
Parent ruft nie `setup_logging()` → `worker_died`/`restart_limit_exceeded`/
Autoscale-Events im Console-Format trotz `--log-format json`.
**Fix:** `setup_logging(...)` in `run_multi_process_worker` vor `pm.run()`,
Listener-Stop im `finally`.
**Tests:** Unit — Parent-Logging konfiguriert JSON-Renderer gemäß Flag.

## U2 — Verbindungsschicht & App-Lifecycle

### P1-2 · Valkey-Lesepfade ohne Timeout (half-open hängt ewig)
`Valkey.from_url` ohne `socket_timeout`/`socket_connect_timeout`
(`valkey.py:108-113`); `get_result`/`get_results_batch`/Scheduler-Reads ohne
`op_timeout`; `Client.get_result`-Deadline prüft nur ZWISCHEN den Aufrufen.
**Fix:** `socket_timeout`+`socket_connect_timeout` aus Config an `from_url`;
`op_timeout` auf Lese-Ops durchreichen (analog `store_result`).
**Tests:** Unit — from_url erhält Timeouts; hängende Op → `BackendConnectionError`
innerhalb Timeout.

### P1-3 · `connect()` hängt bei erschöpftem injiziertem Pool
`await self._pool_ctx.__aenter__()` (`rabbitmq.py:162-164`) blockiert unbegrenzt.
**Fix:** `asyncio.wait_for(..., _CHANNEL_READY_TIMEOUT_S)` → bei Timeout
`BrokerConnectionError("pool acquire timed out — pool exhausted?")`,
Checkout-Release im bestehenden except-Block.
**Tests:** Unit — erschöpfter Pool → Fehler statt Hang.

### P1-4 · `SwarmQ.__aexit__` ohne try/finally
Wirft `broker.close()`, bleiben Backend-Client + Log-Listener offen
(`app.py:106-109`).
**Fix:** gestaffeltes try/finally; erste Exception re-raisen, Rest läuft.
**Tests:** Unit — broker.close wirft → backend.close + listener.stop laufen.

### P2 (U2) · plain-Connection-Warnung · `px=0`-Guard · Config-Parses
- Warnung beim `connect()` im Inject-Pfad, wenn Connection keine
  RobustConnection ist (10s-Stall-Wand dokumentieren) (`rabbitmq.py:298`).
- `store_result` auf dem Expired-Pfad mit `ttl_ms=0` → Skip statt `SET PX 0`
  (`worker.py:274`).
- `_parse_int`/`_parse_float`-Helfer analog `_parse_port` für alle 13 numerischen
  Env-Parses (`config.py`) → `ConfigurationError` statt rohem `ValueError`.

## U3 — Scheduler: Split-Brain & Cluster

### P1-5 · Renew-Exception stuft Leader nicht herab
`BackendConnectionError` im Renew → `return`, `_state` bleibt `"leader"`,
Poll-Loop feuert weiter (`scheduler.py:628-630`). Asymmetrisch zu
`renew==False`.
**Fix:** konsekutive Renew-Fehler zählen; nach N = max(1, lock_ttl//heartbeat - 1)
Fehlern proaktiv Step-down (`_state="standby"` + `SCHEDULER_STANDBY`), bevor
die Lock-TTL sicher abgelaufen ist.
**Tests:** Unit — N renew-Exceptions → standby; einzelner Fehler → leader bleibt.

### P1-6 · Schedfire-Dedup-TTL (300s fix) als einziger Split-Brain-Schutz
`scheduler_inflight_ttl` (300s) kann vor dem completed-Marker ablaufen, wenn
`hard_timeout > 300s` → Re-Publish + zweiter SETNX-Gewinner → Doppelausführung.
Doku verspricht zudem „Lua-atomares ZREM-Claim im Poll" — existiert nicht.
**Fix:** effektive inflight-TTL = `max(scheduler_inflight_ttl, hard_timeout + Puffer)`
analog `compute_lock_ttl_ms` (Worker kennt task-spezifisches `hard_timeout` beim
Claim). Doku korrigieren (Schutz = Publish-vor-Commit + Schedfire-Dedup).
**Tests:** Unit — Claim-TTL ≥ hard_timeout+Puffer; Doku-Diff.

### P1-7 · Multi-Key-Lua ohne Hash-Tags → CROSSSLOT im Cluster
`sched_commit`/`sched_poll` über `sq:sched:zset`+`sq:sched:defs`+Leader-Lock;
`sliding_window_counter` über current+previous — alles ohne `{...}`.
**Fix (User-Entscheidung):** Hash-Tags einführen: `sq:{sched}:zset`,
`sq:{sched}:defs`, `sq:lock:{sched}:leader`; Rate-Limiter-Fenster-Keys unter
gemeinsamen `{rl:<task>}`-Tag. **Breaking:** bestehende Schedule-Keys werden
nicht migriert → Migrationshinweis in Doku (Schedules nach Upgrade neu anlegen
oder manuell RENAME).
**Tests:** Unit-Key-Funktionen; Integration — Lua-Skripte laufen mit neuen Keys.

### P2 (U3) · verwaiste Schedules · Missed-Cron-Signal · UTC-Doku
- `_fire_one`: Task nicht in Registry → Skip + `scheduler_schedule_unregistered_task`-
  Warnung statt blindem Publish (`scheduler.py:771-811`).
- `SCHEDULER_FIRE_SKIPPED`-Signal mit `skipped_count` beim Catch-up.
- Doku: Crons sind UTC, keine DST-Semantik.

## U4 — Worker-Signale & Workflow-Cancel

### P1-9 · Blockierender Lifecycle-Signal-Handler hängt Shutdown
`dispatch(WORKER_SHUTDOWN)` als erste Shutdown-Aktion ohne Timeout
(`worker.py:1051`; symmetrisch ONLINE:930, Heartbeat:1038).
**Fix:** Lifecycle-Dispatches mit `asyncio.wait_for` (gekoppelt an
`shutdown_timeout`-Anteil bzw. festes Budget), Timeout loggen + fortfahren.
**Tests:** Unit — hängender Handler blockiert Shutdown nicht länger als Budget.

### P1-10 · Workflow-Cancel stoppt eingereihte Steps nicht
Worker-Pickup prüft nur `is_cancelled(task_id)` (`worker.py:1393`), nie
`is_workflow_cancelled(wf_id)` → group/chord-Member laufen nach `cancel()` voll
durch.
**Fix:** bei gesetztem `HEADER_WORKFLOW_ID` zusätzlich Workflow-Flag prüfen →
Step als cancelled abschließen. `WorkflowHandle.cancel`-Docstring präzisieren.
**Tests:** e2e — `test_workflow_cancel_stops_chain` asserted zusätzlich, dass
der Downstream-Step NICHT lief (B6); neuer Test cancel-während-group.

### P2 (U4) · Sampler-Resilienz · CancelledError-Doku · Decode-Limits
- `_sample_metrics_once`: breites `except Exception` pro Queue + um den
  Loop-Körper in `_metrics_sampler` (`worker.py:1004-1035`).
- `SignalDispatcher.dispatch`/`_swallow`: `CancelledError` explizit re-raisen,
  Docstring „swallows Exception, not BaseException".
- msgpack-Limits (`max_array_len` etc.) in `decode_result` + rekursivem
  ext-hook (`serialization.py:200-236`); Body-Size-Cap (16 MiB, konfigurierbar)
  vor `decode_json(message.body)` (`worker.py:1608`) → nack ohne requeue.

## U5 — P2-Restpaket

- Untere Dependency-Pins: `msgpack>=1.0`, `msgspec>=0.18`, `aio-pika>=9`,
  Floors für valkey/structlog/croniter/watchfiles (`pyproject.toml`).
- Log-Queue-Drop sichtbar machen: geratelimitete WARNING + Counter
  (`logging.py:68`).
- `/ready`-Doku: keine Auth, bei `0.0.0.0` hinter Proxy (analog metrics.py).
- `schedule add`: Hinweis, dass `--task` nicht gegen Registry validiert wird
  (Doku/Help); optional `--module`-Validierung.
- `setup_dishka`: Warn-Log bei Überschreiben eines gesetzten Containers.

## U6 — Test- & CI-Lücken

- **B1:** CI — `fuzz-tests`-Job (blockierend, billig); `stress-tests` als
  scheduled; chaos-Job auf ganzes `tests/chaos`-Verzeichnis.
- **B2:** Skip-Tests `WORKER_HEARTBEAT`/`SCHEDULER_STANDBY` reaktivieren
  (`test_signal_catalog.py:210/894`) — Begründungen sind faktisch falsch.
- **B3:** Integration-Test inject-pools: echte injizierte Connection/Client,
  Producer-Roundtrip, Caller-Reuse nach Client-close.
- **B4:** Integration-Test Metrics-Sampler: echter Worker, echte Gauge-Werte +
  `WORKER_HEARTBEAT` nach einem Sampler-Intervall.
- **B5:** Property-Test WorkflowAdvancer: zufällige Topologien ×
  Completion-Reihenfolgen × Redeliveries; Invarianten: Chord-Body genau einmal
  und erst nach allen Headern; Group-Result in Definitionsreihenfolge.
- **B6:** in U4/P1-10 enthalten.

## Verifiziert sauber (kein Handlungsbedarf)

Ack/Nack-Vollständigkeit aller Pfade, Semaphore-Slot-Handling (kein Leak),
Lock-Release-Ownership, `unique_release`-TTL-Race-Schutz, `chord_complete`-
Idempotenz (SADD statt INCR), `sliding_window_counter`-Atomizität,
`sched_disable`-TOCTOU, `_http.py` (Slowloris/Header-Caps), hot_reload
(Orphan-Guard, kein Code-Eval), DLQ-Replay (verbatim bytes, keine Deser.),
Health-Liveness/Readiness-Trennung (Deferral von 2026-06-02 ist gelöst),
graceful Scale-Down + Flapping-Schutz, dishka-Scoping, apply()-Partial-Failure-
Handling, schedule_many-Rollback. Semaphore-Fencing-Deferral neu bewertet:
durch P0-A-TTL-Fix entschärft, bleibt vertretbar (Doku-Hinweis genügt).
