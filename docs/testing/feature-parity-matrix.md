# Feature-Parity-Matrix (S1)

Status: 2026-04-17 — Phase-1+2 abgeschlossen, Phase 3+4 in Arbeit.

Spalten: Happy-Path-E2E (`H`) · Sad-Path-E2E (`S`) · Chaos (`C`) · Property/Fuzz (`P`) · Stress (`Z`).

## Public API — Client / SwarmQ

| API | H | S | C | P | Z |
|---|---|---|---|---|---|
| `Client.schedule` | `tests/e2e/test_roundtrip.py` | `tests/e2e/test_roundtrip.py::test_task_with_error` | `tests/chaos/test_broker_restart.py` | – | `tests/stress/test_concurrency.py::test_100_parallel_schedule` |
| `Client.schedule_many` | `tests/e2e/test_bulk_mixed.py` | `tests/e2e/test_bulk_mixed.py::test_*_mixed_*` | `tests/e2e/test_bulk_mixed.py::test_*_reconnect_midway` | – | – |
| `Client.get_result` | `tests/e2e/test_roundtrip.py` | `tests/e2e/test_roundtrip.py::test_task_with_error` | `tests/chaos/test_backend_restart.py` | – | – |
| `Client.get_results` | `tests/e2e/test_performance.py::test_schedule_many_get_results` | `tests/e2e/test_bulk_mixed.py` | – | – | – |
| `Client.run` | `tests/e2e/test_performance.py` | – | – | – | – |
| `Client.cancel` | `tests/e2e/test_cancellation.py` | `tests/e2e/test_cancellation.py::test_e2e_cancel_unknown_*` | – | – | `tests/stress/test_concurrency.py::test_50_parallel_cancels` |
| `Client.get_progress` | `tests/e2e/test_progress.py` | `tests/e2e/test_progress.py::test_*_unknown_task` | `tests/chaos/test_backend_restart.py` | `tests/property/test_progress_properties.py` | – |
| `Client.watch_progress` | `tests/e2e/test_progress.py::test_watch_*` | `tests/unit/test_client.py::TestClientProgress::*malformed*` | `tests/chaos/test_network_chaos.py::test_pubsub_disconnect_resubscribe` | `tests/fuzz/test_external_publish_fuzz.py` | – |

## Worker / Task

| Feature | H | S | C | P | Z |
|---|---|---|---|---|---|
| Retry + Backoff | `tests/e2e/test_phase2.py::test_retry_backoff_success` | `tests/e2e/test_phase2.py::test_retry_exhaustion_dlq` | `tests/chaos/test_worker_crash.py` | `tests/property/test_backoff_properties.py` | – |
| NoRetry | – | `tests/e2e/test_dlq_cli.py` (poison) | – | – | – |
| TaskCancelledError | `tests/e2e/test_cancellation.py` | `tests/e2e/test_cancellation.py::test_e2e_running_cancel_*` | – | – | – |
| Soft + Hard Timeout | `tests/e2e/test_phase2.py::test_soft_timeout_retry` | – | – | – | – |
| Rate-Limit | `tests/e2e/test_rate_limiting.py` | `tests/e2e/test_scheduler.py::TestCronWithRateLimit` | `tests/chaos/test_clock_skew.py::test_rate_limit_*` | `tests/property/test_rate_limiter_properties.py` | `tests/stress/test_concurrency.py::test_50_parallel_rate_limit_*` |
| Lock (mutex/semaphore) | `tests/e2e/test_locking.py` | `tests/e2e/test_locking.py::test_force_release_*` | – | – | – |
| Unique-Dedup | `tests/e2e/test_unique_tasks.py` | `tests/e2e/test_unique_tasks.py::test_ttl_expiry_race_*` | – | `tests/property/test_dedup_hash_properties.py` | – |
| DeliveryMode (FAF) | `tests/e2e/test_phase2.py::test_fire_and_forget` | – | – | – | – |
| Multi-Queue routing | `tests/e2e/test_roundtrip.py` | – | – | – | – |
| Update-Progress | `tests/e2e/test_progress.py` | `tests/e2e/test_progress.py::test_get_progress_none_*` | `tests/chaos/test_clock_skew.py::test_progress_throttle_*` | `tests/property/test_progress_properties.py` | `tests/stress/test_concurrency.py::test_200_parallel_update_progress` |

## Scheduler

| Feature | H | S | C | P | Z |
|---|---|---|---|---|---|
| Cron | `tests/e2e/test_scheduler.py::TestCronSchedule` | – | `tests/chaos/test_clock_skew.py::test_scheduler_cron_*` | – | `tests/stress/test_concurrency.py::test_3_scheduler_*` |
| Delayed/ETA | `tests/e2e/test_scheduler.py::TestDelayAndEta` | – | – | – | – |
| Recurring + Leader-Wechsel | `tests/e2e/test_scheduler.py::TestRecurringAcrossLeaderChange` | – | – | – | – |
| Leader-Failover (graceful) | `tests/e2e/test_scheduler.py::TestGracefulHandover` | – | – | – | – |
| Leader-Failover (crash) | `tests/e2e/test_scheduler.py::TestCrashFailover` | – | `tests/chaos/test_backend_restart.py` | – | – |

## CLI

| Command | H | S |
|---|---|---|
| `swarmq worker` | `tests/integration/test_cli_worker.py` | – |
| `swarmq schedule` | `tests/integration/test_cli_schedule.py` | – |
| `swarmq dlq list` | `tests/e2e/test_dlq_cli.py::test_dlq_list_*` | `tests/e2e/test_dlq_cli.py::test_*_empty_*` |
| `swarmq dlq inspect` | `tests/e2e/test_dlq_cli.py::test_*_inspect_*` | `tests/e2e/test_dlq_cli.py::test_*_unknown_task_id_exits_3` |
| `swarmq dlq retry` | `tests/e2e/test_dlq_cli.py::test_*_retry_*` | – |
| `swarmq dlq purge` | `tests/e2e/test_dlq_cli.py::test_*_purge_*` | – |

## Middleware + Signals

| Feature | H | S |
|---|---|---|
| Single Middleware | `tests/e2e/test_middleware_signals.py` | `tests/e2e/test_middleware_signals.py::test_*_pre_enqueue_exception_*` |
| Middleware-Kette (3) | `tests/e2e/test_middleware_chains.py::test_three_middlewares_*` | `tests/e2e/test_middleware_chains.py::test_*_in_middle_raises_*` |
| Cross-Middleware-Communication | `tests/e2e/test_middleware_chains.py::test_*_message_dict_mutations` | – |
| Lifecycle-Order | `tests/e2e/test_middleware_chains.py::test_*_interleave_correctly` | – |
| Signal-Katalog (alle Enums) | `tests/e2e/test_signal_catalog.py` | – |

## Lücken / explizit deferred

- ~~`Signal.WORKER_HEARTBEAT` / `TASK_EXPIRED` / `SCHEDULER_STANDBY` — nicht dispatcht~~ **(behoben 2026-06-02)** — alle drei werden jetzt dispatcht: `TASK_EXPIRED` schon länger (`worker.py`), `WORKER_HEARTBEAT` aus dem Metrics-Sampler und `SCHEDULER_STANDBY` beim leader→standby-Übergang (Produktionssicherheits-Härtung). Die zugehörigen Skip-Tests wurden in Härtungs-Runde 2 (2026-06-03) reaktiviert und sind grün.
- `dlq retry --all --filter=task_name=X` — Plan-Annahme; CLI hat kein `--filter`-Flag. Test zeigt nur `--all` ohne Filter.
- ~~Hard-Panic (SIGSEGV) → DLQ — Quorum-Queue hat kein `x-delivery-limit`~~ **(veraltet)** — `x-delivery-limit=20` IST für Quorum-Queues gesetzt (`rabbitmq.py:_declare_queue_triplet`), ein deterministisch crashender Task erreicht also terminal die DLQ. Der SIGSEGV-Chaos-Test bleibt skipped (20 Worker-Crashes pro Lauf sind teuer), aber die Begründung „kein x-delivery-limit" ist falsch — der Schutz existiert.
