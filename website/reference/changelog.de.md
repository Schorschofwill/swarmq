# Changelog

Alle nennenswerten Änderungen an SwarmQ. Dieses Projekt folgt
[Keep a Changelog](https://keepachangelog.com/) und
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Performance

- **`import swarmq.cli` lädt `valkey` und `aio_pika` nicht mehr eager.**
  Der Package-Init in `swarmq/__init__.py` und die CLI-Fassade
  auflösen Public-Surfaces (`SwarmQ`, `ValkeyBackend`,
  `RabbitMQBroker`, `Metrics`, `HealthChecker`, `Middleware`,
  `Signal`, `Task` u.a.) jetzt lazy via PEP-562 `__getattr__`. Die
  Identität bleibt beim ersten Zugriff erhalten — `from swarmq
  import ValkeyBackend` liefert weiterhin das kanonische Klassen-
  Objekt, keine API-Änderung für Konsumenten. Median-`importtime`-
  Cumulative-Latency für `import swarmq.cli` sinkt von ~188 ms auf
  ~100 ms (Best Case ~78 ms). Betrifft `swarmq --help`,
  `swarmq worker --check` und jede Agent-Introspektion, die den
  CLI-Namespace ohne die schweren Backends importiert.

### Verhaltensänderungen

- **`--format`-Default ist jetzt TTY-bewusst (CLI-04).** Für
  Subcommands mit `--format`-Flag (`dlq list`, `dlq inspect`,
  `dlq retry`, `dlq purge`, `schedule list`, `schedule add`,
  `schedule remove`, `schedule status`, `inspect`) ist der
  Standard-Output jetzt `table` bei einem TTY und `json` bei
  einer Pipe/Umleitung. Vorher war der Default stets `table`.

  **Skripte, die auf dem alten Piped-Table-Default aufbauten**
  können das Altverhalten ohne Code-Änderung mit
  `SWARMQ_DEFAULT_FORMAT=table` wiederherstellen. Explizite
  `--format=table`-Flags funktionieren unverändert. Ein
  explizites `--format=…` auf der Kommandozeile schlägt die
  Env-Var immer.

  Ungültige `SWARMQ_DEFAULT_FORMAT`-Werte (Tippfehler, unbekanntes
  Format, leerer String) werden stillschweigend ignoriert — der
  CLI fällt auf den TTY-basierten Default zurück statt zu
  fehlen. Siehe [Plan 2026-04-23-001 Unit 5](…).

### Hinzugefügt — Phase 5 Welle 2 (Dev Experience)

- **R5.3 Hot Reload** (`pip install swarmq[reload]`). `--reload`
  startet einen `watchfiles`-gestützten Daemon-Thread, der bei
  `.py`-Änderungen unter dem Task-Modul-Verzeichnis `SIGHUP` an
  den Supervisor feuert, was die bestehende R4.6-Reload-Pipeline
  (Drain → execvpe) triggert. Neue Flags: `--reload`, `--watch-dir
  PATH` (wiederholbar). Filter ignoriert `__pycache__`, `*.pyc`,
  Editor-Noise (`4913`, `*.swp`, `*.bak`, `*.tmp`). Loggt
  `hot_reload_enabled`-WARNING bei jedem Start — Developer-Only-
  Feature. Siehe [guides/hot-reload](../guides/hot-reload.md).

### Hinzugefügt — Phase 5 Welle 1 (Operations)

- **R5.1 Prometheus-Metriken** (`pip install swarmq[metrics]`).
  Sieben Metriken mit `task_name`/`queue`/`outcome`-Labels:
  `swarmq_tasks_total`, `swarmq_task_duration_seconds`,
  `swarmq_tasks_active`, `swarmq_queue_depth`,
  `swarmq_worker_uptime_seconds`, `swarmq_retries_total`,
  `swarmq_dlq_messages`. Stdlib-only HTTP-`/metrics`-Endpoint.
- **R5.2 Health- und Ready-Endpoints.** `GET /health` (Liveness,
  immer 200 solange Event-Loop lebt), `GET /ready` (200, wenn
  Broker + Backend erreichbar, 503 mit sanitisierter JSON-Body
  andernfalls). `HealthChecker`, `HealthServer`, `serve_health`
  als öffentliche API.
- **Auto-Wire.** `SwarmQConfig.metrics_port` / `health_port` (oder
  `--metrics-port` / `--health-port` auf `swarmq worker`) aktivieren
  beide Server + Signal-basierte Instrumentierung ohne Python-
  Entrypoint-Boilerplate.

### Hinzugefügt — Phase 4 (Operator-Features)

- **R4.3 Task-Priorities.** `TaskInfo(priority="high"|"normal"|"low")`
  routet zu `{queue}.{priority}`-Suffixen. Worker deklariert das
  Triplet automatisch, wenn `priorities=True` (oder `--priorities`).
  Warnt beim Start, wenn Tasks Nicht-Normal-Priority deklarieren,
  die Worker-Config aber Priorities deaktiviert hat.
- **R4.4 Task-Expiry.** `TaskInfo(max_age=seconds)` + der
  `x-swarmq-enqueued-at`-Header wird beim Publish gestempelt
  (Client + Scheduler). Worker routet abgelaufene Nachrichten
  vor Rate-Limit-/Lock-/pre_execute-Stages mit `TASK_EXPIRED`-
  Signal + `TaskError("task expired: …")`-Ergebnis in die DLQ.
- **R4.5 Burst-Mode.** `swarmq worker --burst` exited mit 0,
  sobald alle Queues `--burst-idle-timeout` Sekunden Idle waren.
  `SwarmQConfig.burst_mode` / `burst_idle_timeout` +
  `SWARMQ_BURST_MODE` / `SWARMQ_BURST_IDLE_TIMEOUT`-Env-Vars.
- **R4.6 ProcessManager.** `swarmq worker --processes N` (Alias
  `-p`) lässt N Worker-Subprozesse unter einem Supervisor laufen,
  der gecrashte Worker automatisch neu startet (Rolling-Window-
  Limit: 5 Restarts pro Stunde pro Worker). `spawn`-Context hält
  aio-pika- + Valkey-Verbindungen subprozess-scoped. SIGHUP
  triggert `os.execvp()`-basierten Reload nach graceful Drain;
  SIGTERM / SIGINT triggern denselben 3-Phasen-Shutdown
  (kooperatives Event → SIGTERM → SIGKILL) wie ein Single-
  Process-Worker.
- **R4.7 Autoscaling.** `swarmq worker --autoscale=MIN,MAX`
  skaliert die Worker-Anzahl auf Basis der Queue-Tiefe. Scale-Up,
  wenn Pending-Tasks `50 * active` übersteigen; Scale-Down nach
  60s sustained Idle; 30s Cooldown zwischen Entscheidungen.
  Queue-Tiefe per neuer `RabbitMQBroker.get_queue_depth()`
  (passives `declare(..., passive=True)` auf Main + `.DQ`).

### Hinzugefügt — Phase 3 (Komfort-Features)

- **R3.5 Unique Tasks.** `TaskInfo(unique_for=seconds,
  unique_until="start"|"completion")` dedupt auf kanonisch
  kodierten Args. Owner-Check via Lua für TTL-Expiry-Race-Schutz.
  Sowohl `Client.schedule`- als auch `Client.schedule_many`-Pfade.
- **R3.6 Task-Cancellation.** `Client.cancel(task_id)` setzt ein
  Valkey-Flag, das der Worker pre-pickup und während der
  Ausführung checkt. Dediziertes `TASK_CANCELLED`-Signal. Cancels
  propagieren als `TaskCancelledError` via `get_result`.
- **R3.7 Progress Tracking.** `Task.update_progress(current,
  total, message)` auf 500ms pro Instanz gedrosselt mit
  Force-Flush bei `current == total`. `Client.get_progress` +
  `Client.watch_progress` AsyncIterator streamt Updates via
  Pub/Sub mit HASH-Fallback bei Disconnect. ETA via linearer
  Extrapolation.
- **R3.10 DLQ-CLI.** `swarmq dlq list|inspect|retry|purge` für
  Operator-Recovery. Drain-and-Republish-Scan-Pattern, Exit-Codes
  0 (Success), 2 (Config), 3 (task_id nicht gefunden).
- **Embedded Scheduler.** Cron + Delayed (`delay=` / `eta=`) +
  Recurring mit Leader-Election-Failover. Graceful Handover ohne
  TTL-Wartezeit.
- **Full CLI.** `swarmq worker`, `swarmq schedule`, `swarmq inspect`
  — kein Python-Entrypoint für typische Deployments nötig.

### Hinzugefügt — E2E-Hardening (separate Initiative)

- **Chaos-Suite** (`tests/chaos/`): toxiproxy + Container-Restart-
  Abdeckung für Worker-SIGKILL, Broker/Backend-Restart, Netzwerk-
  Chaos (Latency/Bandwidth/Packet-Loss/Full-Down), Clock-Skew,
  Message Corruption, Byzantine Workers.
- **Property-based Tests** (`tests/property/`): Hypothesis gegen
  Dedup-Hash, Progress, Serialization, Backoff, Rate-Limiter.
- **Fuzz** (`tests/fuzz/`): Byte-Fuzz auf Message-Parser +
  AMQP-Header- / Pub/Sub-External-Publisher-Fuzz.
- **Stress** (`tests/stress/`): 10-Worker × 1000-Task exact-once,
  50-Parallel-Cancel-Winner, 200-Parallel-Progress-Throttle.
- **Long-Soak** (`tests/soak/`): 1h-Opt-in-Leak-Detection-Harness
  mit psutil-RSS- / FD- / Valkey-Conn- / Rabbit-Channel-Zählern.
- **Drei-Tier-CI** (GitHub Actions): pre-merge ≤ 5 min, nightly
  Chaos+Stress+Fuzz ≤ 30 min, weekly Mutation+Soak ≤ 4h.

### Reliability-Hardening (ce-review Follow-ups)

- **RabbitMQBroker** Channel-Recreate nach Connection-Reset;
  `_publish_with_retry`-Bounded-Retry-Wrapper erhält asyncio-
  Task-Level-Cancellation.
- **Quorum-Queue `x-delivery-limit=20`** auf Main-Queue — pathologische
  SIGSEGV-Loops terminieren jetzt in der DLQ statt in endloser
  Redelivery.
- **ValkeyBackend** Retry-Policy + `health_check_interval` auf dem
  darunterliegenden valkey-py-Client.
- **Client.watch_progress** re-subscribed bei Pub/Sub-Disconnect.
  valkey-py re-subscribed nicht automatisch.
- **Task.update_progress** asyncio.Lock um den 500ms-Throttle-
  Read-Compare-Write — raste zuvor unter `asyncio.gather`.
- **DLQ-Drain-Republish** routet durch `_publish_with_retry` —
  transiente Channel-Fehler lassen gedrainte Nachrichten nicht
  mehr stumm fallen.
- **HTTP-Oberflächen** (`/metrics`, `/health`, `/ready`) durch
  `asyncio.timeout(5.0)` begrenzt, abdeckend Request-Line +
  Header-Drain + 200-Line-Cap. Binäre Request-Lines geben 400
  zurück, keinen Log-Spam. Default-Bind `127.0.0.1`. `/ready`-
  Error-Body sanitisiert, um Host:Port-Leak zu verhindern.

## [0.x] — frühere Phasen

Phase 1 (minimaler Roundtrip) + Phase 2 (Retries, DLQ, Logging,
Graceful Shutdown) + Phase 3 Core-Komfort-Features (Middleware,
Signals, Rate-Limit, Locking) landeten im selben Branch wie die
Punkte oben. Einzelne Commit-History in git.
