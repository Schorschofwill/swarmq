# Feature-Parity-Matrix

Stand: 0.14.4 (2026-10-24). Erste Fassung 2026-04-17 (S1, Phase 1+2 der
E2E-Härtung); seitdem um Workflows, CPU-Modus, Supervisor, Autoskalierung,
Kontrollkanal und Betriebsbefehle ergänzt.

Die Matrix zeigt, auf welchen Testebenen ein Feature abgesichert ist. Sie
ist keine Vollständigkeitsliste: Unit-Tests stehen nur dort, wo keine
höhere Ebene greift. Befund-Nachweise (`test_befund_*.py`) sind nicht
einzeln aufgeführt.

Spalten: Happy-Path-E2E (`H`) · Sad-Path-E2E (`S`) · Chaos (`C`, `tests/chaos`
oder die Toxiproxy-Kombinationen in `tests/e2e/combinations`) ·
Property/Fuzz (`P`) · Stress (`Z`). Wo ein Feld auf `tests/integration/`
zeigt, gibt es keinen e2e-Test, wohl aber einen gegen echte Dienste.

## Public API — Client / SwarmQ

| API | H | S | C | P | Z |
|---|---|---|---|---|---|
| `Client.schedule` | `tests/e2e/test_roundtrip.py` | `tests/e2e/test_roundtrip.py::test_task_with_error` | `tests/chaos/test_broker_restart.py`, `tests/chaos/test_publish_deadline.py` | – | `tests/stress/test_concurrency.py::test_100_parallel_schedule_distinct_task_ids` |
| `Client.schedule_many` | `tests/e2e/test_bulk_mixed.py` | `tests/e2e/test_bulk_mixed.py::test_*_mixed_*` | `tests/e2e/test_bulk_mixed.py::test_*_reconnect_midway` | – | – |
| `Client.get_result` | `tests/e2e/test_roundtrip.py` | `tests/e2e/test_roundtrip.py::test_task_with_error` | `tests/chaos/test_backend_restart.py` | – | – |
| `Client.get_results` | `tests/e2e/test_performance.py::test_schedule_many_get_results` | `tests/e2e/test_bulk_mixed.py` | – | – | – |
| `Client.run` | `tests/e2e/test_performance.py` | – | – | – | – |
| `Client.cancel` | `tests/e2e/test_cancellation.py` | `tests/e2e/test_cancellation.py::test_e2e_cancel_unknown_*` | – | – | `tests/stress/test_concurrency.py::test_50_parallel_cancels_one_winner` |
| `Client.get_progress` | `tests/e2e/test_progress.py` | `tests/e2e/test_progress.py::test_*_unknown_task` | `tests/chaos/test_backend_restart.py` | `tests/property/test_progress_properties.py` | – |
| `Client.watch_progress` | `tests/e2e/test_progress.py::test_watch_*` | `tests/unit/client/test_results.py::TestClientProgress::test_watch_progress_drops_malformed_payloads` | `tests/chaos/test_network_chaos.py::TestNetworkChaos::test_pubsub_disconnect_resubscribe_via_watch_progress` | `tests/fuzz/test_external_publish_fuzz.py` | – |
| `SwarmQ.apply` / `SwarmQ.workflow` (Chain, Group, Chord) | `tests/e2e/test_workflows.py` | `tests/e2e/test_workflows.py`, `tests/integration/test_workflow_orchestration.py` | `tests/chaos/test_workflow_chord_idempotency.py`, `tests/e2e/combinations/test_workflow_combinations.py` | `tests/property/test_workflow_advancer_properties.py`, `tests/property/test_workflow_roundtrip.py`, `tests/property/stateful/test_workflow_machine.py` | – |

## Worker / Task

| Feature | H | S | C | P | Z |
|---|---|---|---|---|---|
| Retry + Backoff | `tests/e2e/test_phase2.py::test_retry_backoff_success` | `tests/e2e/test_phase2.py::test_retry_exhaustion_dlq`, `tests/e2e/test_retry_with_filter.py` | `tests/chaos/test_worker_crash.py` | `tests/property/test_backoff_properties.py`, `tests/property/stateful/test_retry_chain_machine.py` | – |
| Retry im Prioritätsband | `tests/e2e/test_retry_band.py` | – | – | – | – |
| NoRetry | – | `tests/e2e/test_dlq_cli.py` (poison) | – | – | – |
| Dead-Lettering abschaltbar | `tests/e2e/test_dead_letter_switch.py` | – | – | – | – |
| TaskCancelledError | `tests/e2e/test_cancellation.py` | `tests/e2e/test_cancellation.py::test_e2e_running_cancel_*`, `tests/e2e/test_worker_cancel_teardown.py` | – | – | – |
| Soft + Hard Timeout | `tests/e2e/test_phase2.py::test_soft_timeout_retry` | `tests/e2e/test_cpu_mode.py::test_hard_timeout_kills_blocking_child_and_dead_letters` | – | – | – |
| Rate-Limit | `tests/e2e/test_rate_limiting.py` | `tests/e2e/test_scheduler.py::TestCronWithRateLimit` | `tests/chaos/test_clock_skew.py::test_rate_limit_*` | `tests/property/test_rate_limiter_properties.py` | `tests/stress/test_concurrency.py::test_50_parallel_rate_limit_checks_within_limit` |
| Lock (mutex/semaphore) | `tests/e2e/test_locking.py` | `tests/e2e/test_locking.py::test_force_release_*` | `tests/e2e/combinations/test_failure_cascades.py` | `tests/property/stateful/test_lock_machine.py` | – |
| Unique-Dedup | `tests/e2e/test_unique_tasks.py` | `tests/e2e/test_unique_tasks.py::test_ttl_expiry_race_*` | – | `tests/property/test_dedup_hash_properties.py` | – |
| Ausführungsschutz (Execution Guard) | `tests/integration/test_execution_guard.py` | `tests/e2e/test_schedfire_worker_crash.py` | `tests/e2e/combinations/test_failure_cascades.py` | – | – |
| Prioritäten | `tests/e2e/combinations/test_timing_order.py::*priority*` | – | – | – | – |
| DeliveryMode (FAF) | `tests/e2e/test_phase2.py::test_fire_and_forget` | – | – | – | – |
| Multi-Queue routing | `tests/e2e/test_roundtrip.py` | – | – | – | – |
| Update-Progress | `tests/e2e/test_progress.py` | `tests/e2e/test_progress.py::test_get_progress_none_*` | `tests/chaos/test_clock_skew.py::test_progress_throttle_*` | `tests/property/test_progress_properties.py` | `tests/stress/test_concurrency.py::test_200_parallel_update_progress_throttle_holds` |
| Dependency Injection (Dishka) | `tests/integration/test_cli_worker_dishka.py`, `tests/integration/test_injected_pools.py` | `tests/e2e/test_cpu_mode.py::test_di_child_is_reused_and_recycled_with_signal` | – | – | – |
| Burst-Modus | `tests/e2e/test_burst_mode.py` | `tests/integration/test_cli_worker_burst.py` | – | – | – |
| Graceful Shutdown unter Last | `tests/e2e/test_worker_shutdown_load.py` | `tests/e2e/test_recycling_drain_shutdown.py` | `tests/chaos/test_worker_crash.py` | – | – |
| Exactly-once-Zustellung (mehrere Worker) | – | – | – | – | `tests/stress/test_concurrency.py::test_10_workers_1000_tasks_each_exact_once` |

## Prozessmodell: Supervisor, CPU-Modus, Skalierung

| Feature | H | S | C | P | Z |
|---|---|---|---|---|---|
| Supervisor: Neustart abgestürzter Worker | `tests/integration/test_cli_worker.py` | `tests/integration/test_cli_worker_orphans.py` (Kinder sterben mit dem Supervisor) | `tests/chaos/test_worker_crash.py::TestWorkerCrash::test_segv_panic_redelivers_then_dlqs_after_max_retries`, `tests/e2e/combinations/test_concurrency_lifecycle.py` | – | – |
| Lebenszeichen, hängender Worker | `tests/integration/test_cli_worker_heartbeat.py` | `tests/integration/test_cli_worker_heartbeat.py` | – | – | – |
| Selbst-Recycling (Taskzahl, RSS) | `tests/integration/test_cli_worker_recycle.py` | `tests/e2e/test_recycling_drain_shutdown.py` | – | – | – |
| CPU-Modus (`--pool cpu`) | `tests/e2e/test_cpu_mode.py`, `tests/integration/test_cli_worker_cpu.py` | `tests/e2e/test_cpu_mode.py::test_exception_in_child_reaches_error_path_typed` | `tests/integration/test_cli_worker_chaos.py` | – | – |
| Autoskalierung (`--autoscale`) | `tests/e2e/combinations/test_concurrency_lifecycle.py` | – | – | `tests/property/stateful/test_autoscale_machine.py` | – |
| Kontrollkanal (`--max-processes`, `--group`) | `tests/integration/test_cli_worker_setpoint.py` | `tests/integration/backend/test_valkey_control.py` | – | `tests/property/stateful/test_autoscale_machine.py` | – |
| Reload per SIGHUP / `--reload` | `tests/integration/test_hot_reload_e2e.py`, `tests/integration/test_hot_reload_python_m_e2e.py` | `tests/integration/test_hot_reload_logconfig_e2e.py` | `tests/e2e/combinations/test_concurrency_lifecycle.py` | – | – |
| Logging (Format, `--log-config`, Pools) | `tests/e2e/test_logging_e2e_config.py`, `tests/e2e/test_logging_pools_e2e.py` | `tests/e2e/test_logging_e2e_runtime.py` | – | – | – |

## Scheduler

| Feature | H | S | C | P | Z |
|---|---|---|---|---|---|
| Cron | `tests/e2e/test_scheduler.py::TestCronSchedule` | `tests/e2e/test_befund_bz02_bz11_bz23_scheduler.py` | `tests/chaos/test_clock_skew.py::test_scheduler_cron_*` | `tests/fuzz/test_cron_fuzz.py` | `tests/stress/test_concurrency.py::test_3_scheduler_candidates_no_missed_cron_fires` |
| Delayed/ETA | `tests/e2e/test_scheduler.py::TestDelayAndEta` | – | – | – | – |
| Recurring + Leader-Wechsel | `tests/e2e/test_scheduler.py::TestRecurringAcrossLeaderChange` | – | – | `tests/property/stateful/test_leader_machine.py` | – |
| Leader-Failover (graceful) | `tests/e2e/test_scheduler.py::TestGracefulHandover` | – | – | – | – |
| Leader-Failover (crash) | `tests/e2e/test_scheduler.py::TestCrashFailover` | `tests/e2e/test_schedfire_worker_crash.py` | `tests/chaos/test_backend_restart.py`, `tests/e2e/combinations/test_concurrency_lifecycle.py` | – | – |
| Scheduler ein/aus je Worker | `tests/integration/test_scheduler_switch.py` | – | – | – | – |

## CLI

| Command | H | S |
|---|---|---|
| `swarmq worker` | `tests/integration/test_cli_worker.py` | `tests/e2e/test_cli_precedence_e2e.py`, `tests/e2e/test_app_dir_console_script.py` |
| `swarmq worker --check` | `tests/unit/process_manager/test_check.py` | `tests/unit/test_befund_f3_18_check_invalid_config_exit78.py` |
| `swarmq schedule` (`list`, `add`, `remove`, `status`, `prune`) | `tests/integration/test_cli_schedule.py` | `tests/e2e/test_befund_f4_06_schedule_status_workers_same_host.py` |
| `swarmq dlq list` | `tests/e2e/test_dlq_cli.py::test_dlq_list_*` | `tests/e2e/test_dlq_cli.py::test_*_empty_*` |
| `swarmq dlq inspect` | `tests/e2e/test_dlq_cli.py::test_*_inspect_*` | `tests/e2e/test_dlq_cli.py::test_*_unknown_task_id_exits_3` |
| `swarmq dlq retry` | `tests/e2e/test_dlq_cli.py::test_*_retry_*` | – |
| `swarmq dlq purge` | `tests/e2e/test_dlq_cli.py::test_*_purge_*` | – |
| `swarmq inspect` | `tests/unit/test_cli.py::*inspect*` | `tests/unit/test_cli.py::*inspect*` |
| `swarmq check` | `tests/integration/test_cli_env_check.py` | `tests/chaos/test_valkey_eviction_policy.py` |

## Middleware + Signals

| Feature | H | S |
|---|---|---|
| Single Middleware | `tests/e2e/test_middleware_signals.py` | `tests/e2e/test_middleware_signals.py::test_pre_enqueue_exception_prevents_enqueue` |
| Middleware-Kette (3) | `tests/e2e/test_middleware_chains.py::test_three_middlewares_*` | `tests/e2e/test_middleware_chains.py::test_*_in_middle_raises_*` |
| Cross-Middleware-Communication | `tests/e2e/test_middleware_chains.py::test_*_message_dict_mutations` | – |
| Lifecycle-Order | `tests/e2e/test_middleware_chains.py::test_*_interleave_correctly` | – |
| Signal-Katalog (alle Enums) | `tests/e2e/test_signal_catalog.py` | – |

## Verträge und Mischbetrieb

- Austauschbare Backends und Broker prüfen gemeinsame Vertrags-Suiten aus
  `tests/contracts/` (Publisher/Consumer, Result-Backend, Koordination,
  Ausführungsschutz): gegen die In-Memory-Fakes in
  `tests/unit/test_contracts_inmemory.py`, gegen die echten Dienste in
  `tests/integration/test_contracts_rabbitmq.py` und
  `tests/integration/test_contracts_valkey.py`.
- Mischbetrieb mit älteren Versionen aus PyPI (0.11.0 bis 0.13.1) und
  Namensraum-Rückfall: `tests/compat/` (opt-in, siehe
  `docs/testing/test-strategy.md`).
- Broker-Verhalten von RabbitMQ 4.3.6 und 4.2.9: `tests/integration/spikes/`.

## Lücken / explizit deferred

- `dlq retry --all --filter=task_name=X` — Plan-Annahme; die CLI hat bis
  0.14.4 kein `--filter`-Flag. Die Tests zeigen nur `--all` ohne Filter.
- `swarmq inspect` hat nur Unit-Tests, keinen Lauf gegen echte Dienste.
- Autoskalierung ist nur über die Zustandsmaschine (Property) und die
  e2e-Kombinationen abgedeckt; einen eigenen Integrationstest für
  Hoch- und Herunterskalieren unter Rückstau gibt es nicht.

Erledigt:

- `Signal.WORKER_HEARTBEAT` / `TASK_EXPIRED` / `SCHEDULER_STANDBY` werden
  dispatcht (2026-06-02): `TASK_EXPIRED` im Worker, `WORKER_HEARTBEAT` aus
  dem Metrics-Sampler, `SCHEDULER_STANDBY` beim Übergang leader → standby.
  Die zugehörigen Tests laufen seit Härtungs-Runde 2 (2026-06-03).
- Hard-Panic (SIGSEGV) → DLQ: Quorum-Queues tragen `x-delivery-limit`
  (Standard 20, `main_queue_arguments` in `src/swarmq/broker/rabbitmq/topology.py`), ein
  deterministisch abstürzender Task erreicht also terminal die DLQ.
  `tests/chaos/test_worker_crash.py::TestWorkerCrash::test_segv_panic_redelivers_then_dlqs_after_max_retries`
  fährt dazu einen Supervisor mit `--processes 2` und prüft zusätzlich
  Exit ≠ 0 bei erschöpftem Neustart-Budget. Er läuft im örtlichen
  Chaos-Gate vor Releases, nicht in der Pipeline.
