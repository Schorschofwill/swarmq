# Changelog

All notable changes to SwarmQ. This project follows
[Keep a Changelog](https://keepachangelog.com/) and
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Performance

- **`import swarmq.cli` no longer eagerly loads `valkey` and
  `aio_pika`.** The package init in `swarmq/__init__.py` and the
  CLI facade now resolve public surfaces (`SwarmQ`, `ValkeyBackend`,
  `RabbitMQBroker`, `Metrics`, `HealthChecker`, `Middleware`,
  `Signal`, `Task`, etc.) lazily via PEP-562 `__getattr__`. Identity
  is preserved on first access — `from swarmq import ValkeyBackend`
  still returns the canonical class object, no API change for
  consumers. Median `importtime` cumulative latency for
  `import swarmq.cli` drops from ~188 ms to ~100 ms (best case
  ~78 ms). Affects `swarmq --help`, `swarmq worker --check`, and any
  agent-side introspection that imports the CLI namespace without
  needing the heavy backends.

### Behaviour Changes

- **`--format` default is now TTY-aware (CLI-04).** For
  subcommands that accept `--format` (`dlq list`, `dlq inspect`,
  `dlq retry`, `dlq purge`, `schedule list`, `schedule add`,
  `schedule remove`, `schedule status`, `inspect`), the output
  format now defaults to `table` when stdout is a TTY and `json`
  when stdout is piped, redirected, or otherwise non-interactive.
  Previously the default was always `table`.

  **Scripts relying on the old piped-table default** can restore
  the previous behaviour without code changes by exporting
  `SWARMQ_DEFAULT_FORMAT=table`. Explicit `--format=table` flags
  continue to work unchanged. An explicit `--format=…` on the
  command line always wins over the env var.

  Invalid `SWARMQ_DEFAULT_FORMAT` values (typo, unknown format,
  empty string) are silently ignored — the CLI falls back to the
  TTY-aware default rather than failing. See
  [plan 2026-04-23-001 Unit 5](https://github.com/…).

### Added — Phase 5 Wave 2 (Dev Experience)

- **R5.3 Hot reload** (`pip install swarmq[reload]`). `--reload` adds a
  `watchfiles`-backed daemon thread that fires `SIGHUP` at the
  supervisor on `.py` changes under the task module's directory,
  triggering the existing R4.6 reload pipeline (drain → execvpe).
  New flags: `--reload`, `--watch-dir PATH` (repeatable). Filter
  ignores `__pycache__`, `*.pyc`, editor noise (`4913`, `*.swp`,
  `*.bak`, `*.tmp`). Logs `hot_reload_enabled` WARNING on every
  start — developer-only feature. See
  [guides/hot-reload](../guides/hot-reload.md).
- **`swarmq worker --check`.** Dry-run validation: verifies
  `--module` is importable, and with `--reload` that `watchfiles`
  is installed + watch-dirs resolve. Prints a JSON summary and
  exits `0`/`1` without starting the supervisor or touching
  broker/backend. Use for systemd-unit and dev-container smoke
  tests before a full worker start.

### Added — Phase 5 Wave 1 (Operations)

- **R5.1 Prometheus metrics** (`pip install swarmq[metrics]`). Seven
  metrics with `task_name`/`queue`/`outcome` labels:
  `swarmq_tasks_total`, `swarmq_task_duration_seconds`,
  `swarmq_tasks_active`, `swarmq_queue_depth`,
  `swarmq_worker_uptime_seconds`, `swarmq_retries_total`,
  `swarmq_dlq_messages`. Stdlib-only HTTP `/metrics` endpoint.
- **R5.2 Health + Ready endpoints.** `GET /health` (liveness,
  always 200 while event loop alive), `GET /ready` (200 when
  broker + backend reachable, 503 with sanitised JSON body
  listing the unhealthy dependency otherwise). `HealthChecker`,
  `HealthServer`, `serve_health` as public API.
- **Auto-wire.** `SwarmQConfig.metrics_port` / `health_port` (or
  `--metrics-port` / `--health-port` on `swarmq worker`) opts in
  to both servers + signal-based instrumentation with no Python
  entrypoint boilerplate.

### Added — Phase 4 (Operator Features)

- **R4.3 Task Priorities.** `TaskInfo(priority="high"|"normal"|"low")`
  routes to `{queue}.{priority}` suffixes. Worker auto-declares the
  triplet when `priorities=True` (or `--priorities`). Warns at start
  when tasks declare non-normal priority but the worker config has
  priorities disabled.
- **R4.4 Task Expiry.** `TaskInfo(max_age=seconds)` + the
  `x-swarmq-enqueued-at` header stamped at publish (Client +
  Scheduler). Worker routes expired messages to DLQ with
  `TASK_EXPIRED` signal + `TaskError("task expired: …")` result
  before rate-limit / lock / pre_execute stages.
- **R4.5 Burst-Mode.** `swarmq worker --burst` exits 0 once all
  queues have been idle for `--burst-idle-timeout` seconds.
  `SwarmQConfig.burst_mode` / `burst_idle_timeout` +
  `SWARMQ_BURST_MODE` / `SWARMQ_BURST_IDLE_TIMEOUT` env vars.
- **R4.6 ProcessManager.** `swarmq worker --processes N` (alias
  `-p`) runs N worker subprocesses under a supervisor that auto-
  restarts crashed workers (rolling-window limit: 5 restarts per
  hour per worker). `spawn`-context keeps aio-pika + Valkey
  connections scoped per subprocess. SIGHUP triggers
  `os.execvp()`-based reload after graceful drain; SIGTERM /
  SIGINT trigger the same 3-phase shutdown (cooperative event
  → SIGTERM → SIGKILL) as a single-process worker.
- **R4.7 Autoscaling.** `swarmq worker --autoscale=MIN,MAX`
  scales worker count based on queue depth. Scale-up when
  pending tasks exceed `50 * active`; scale-down after 60s
  sustained idle; 30s cooldown between decisions. Queue-depth
  sampled via new `RabbitMQBroker.get_queue_depth()` (passive
  `declare(..., passive=True)` on main + `.DQ`).

### Added — Phase 3 (Comfort Features)

- **R3.5 Unique Tasks.** `TaskInfo(unique_for=seconds,
  unique_until="start"|"completion")` dedups by canonical-encoded
  args. Owner-check via Lua for TTL-expiry-race protection. Both
  `Client.schedule` and `Client.schedule_many` paths.
- **R3.6 Task Cancellation.** `Client.cancel(task_id)` sets a
  Valkey flag the worker checks pre-pickup + during execution.
  Dedicated `TASK_CANCELLED` signal. Cancels propagate as
  `TaskCancelledError` via `get_result`.
- **R3.7 Progress Tracking.** `Task.update_progress(current, total,
  message)` throttled at 500ms per instance with force-flush on
  `current == total`. `Client.get_progress` + `Client.watch_progress`
  AsyncIterator stream updates via Pub/Sub with HASH fallback on
  disconnect. ETA via linear extrapolation.
- **R3.10 DLQ-CLI.** `swarmq dlq list|inspect|retry|purge` for
  operator recovery. Drain-and-republish scan pattern, exit codes
  0 (success), 2 (config), 3 (task_id not found).
- **Embedded scheduler.** Cron + delayed (`delay=` / `eta=`) +
  recurring with leader-election failover. Graceful handover
  without TTL wait.
- **Full CLI.** `swarmq worker`, `swarmq schedule`, `swarmq inspect`
  — no Python entrypoint required for typical deployments.

### Added — E2E Hardening (separate initiative)

- **Chaos suite** (`tests/chaos/`): toxiproxy + container-restart
  coverage for Worker-SIGKILL, Broker/Backend restart, network
  chaos (latency/bandwidth/packet-loss/full-down), Clock-Skew,
  Message Corruption, Byzantine workers.
- **Property-based tests** (`tests/property/`): hypothesis against
  dedup-hash, progress, serialization, backoff, rate-limiter.
- **Fuzz** (`tests/fuzz/`): byte-fuzz on message parser +
  AMQP-header / Pub/Sub external-publisher fuzz.
- **Stress** (`tests/stress/`): 10-worker × 1000-task exact-once,
  50-parallel-cancel winner, 200-parallel progress-throttle.
- **Long-soak** (`tests/soak/`): 1h opt-in leak-detection harness
  with psutil RSS / FD / Valkey-conn / Rabbit-channel counters.
- **Three-tier CI** (GitHub Actions): pre-merge ≤ 5 min, nightly
  chaos+stress+fuzz ≤ 30 min, weekly mutation+soak ≤ 4h.

### Reliability hardening (ce-review follow-ups)

- **RabbitMQBroker** channel-recreate after connection reset;
  `_publish_with_retry` bounded-retry wrapper preserving
  asyncio task-level cancellation.
- **Quorum-queue `x-delivery-limit=20`** on main queue — pathological
  SIGSEGV-loops now terminate in DLQ instead of endless redelivery.
- **ValkeyBackend** retry policy + `health_check_interval` on the
  underlying valkey-py client.
- **Client.watch_progress** re-subscribes on Pub/Sub disconnect.
  valkey-py does not auto-resubscribe.
- **Task.update_progress** asyncio.Lock around the 500ms throttle
  read-compare-write — previously raced under `asyncio.gather`.
- **DLQ-drain republish** routes through `_publish_with_retry` —
  transient channel errors no longer silently drop drained messages.
- **HTTP surfaces** (`/metrics`, `/health`, `/ready`) bounded by
  `asyncio.timeout(5.0)` covering request-line + header drain +
  200-line cap. Binary request lines return 400, not log-spam.
  Default bind `127.0.0.1`. `/ready` error body sanitised to
  prevent host:port leak.

## [0.x] — earlier phases

Phase 1 (minimal round-trip) + Phase 2 (retries, DLQ, logging,
graceful shutdown) + Phase 3 core comfort features (middleware,
signals, rate-limit, locking) landed in the same branch as the
items above. Individual commit history in git.
