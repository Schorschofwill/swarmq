"""Prometheus metrics exporter (R5.1).

Optional feature — the ``prometheus_client`` package is only required
when the user opts in via the ``metrics`` extra:

    pip install swarmq[metrics]

When the package is absent, :func:`get_metrics` returns a no-op
registry and all instrumentation calls become cheap attribute hits.
The module therefore imports unconditionally; callers never branch on
"is metrics enabled" — they always call :class:`Metrics`, and the
registry decides whether to record or skip.

Metrics (plan §R5.1):

- ``swarmq_tasks_total{task_name,queue,outcome}`` — counter
- ``swarmq_tasks_rejected_total{queue,reason}`` — counter
  (outcome: completed | failed | cancelled | expired | rate_limited
  | dlq | duplicate)
- ``swarmq_task_duration_seconds{task_name,queue}`` — histogram
- ``swarmq_tasks_active{queue}`` — gauge of in-flight tasks per queue
- ``swarmq_queue_depth{queue,kind}`` — gauge (kind: main | delay | dlq)
  sampled by the worker on a schedule; the exporter reads it.
- ``swarmq_worker_uptime_seconds{worker_id}`` — gauge
- ``swarmq_retries_total{task_name,queue}`` — counter
- ``swarmq_dlq_messages{queue}`` — gauge (live DLQ depth)

The worker signal dispatcher wires every signal to a ``Metrics``
method. Attaching the HTTP exposition server is a worker-side concern
(see :class:`MetricsServer`).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

try:
    import prometheus_client
    from prometheus_client import (
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )
    from prometheus_client.core import CounterMetricFamily

    _HAVE_PROMETHEUS = True
except ImportError:  # pragma: no cover — optional
    prometheus_client = None  # type: ignore[assignment]
    CollectorRegistry = None  # type: ignore[assignment,misc]
    Counter = None  # type: ignore[assignment,misc]
    Gauge = None  # type: ignore[assignment,misc]
    Histogram = None  # type: ignore[assignment,misc]
    generate_latest = None  # type: ignore[assignment]
    CounterMetricFamily = None  # type: ignore[assignment,misc]

    _HAVE_PROMETHEUS = False

if TYPE_CHECKING:
    from swarmq.signals import SignalDispatcher

# CONTENT_TYPE used by /metrics endpoint. Falls back to the Prometheus
# text-exposition default when the client is absent so callers do not
# have to branch.
CONTENT_TYPE = (
    prometheus_client.CONTENT_TYPE_LATEST
    if _HAVE_PROMETHEUS
    else "text/plain; version=0.0.4; charset=utf-8"
)


class _NoopMetric:
    """Null-object fallback used when prometheus_client is absent.

    All Counter/Gauge/Histogram operations (labels, inc, dec, set,
    observe) become cheap attribute access + no-op calls. The
    ``labels()`` method returns ``self`` so chained idioms like
    ``metric.labels(queue="q").inc()`` work unchanged.
    """

    def labels(self, *_a: Any, **_kw: Any) -> _NoopMetric:
        return self

    def inc(self, _amount: float = 1.0) -> None: pass
    def dec(self, _amount: float = 1.0) -> None: pass
    def set(self, _value: float) -> None: pass
    def observe(self, _value: float) -> None: pass


class _LogRecordsDroppedCollector:
    """Liest den Drop-Zaehler von ``swarmq.logging`` erst beim Scrape
    (Plan 2026-10-06-0831, KTD5): Der Logpfad bleibt frei von Metrik-Code."""

    def __init__(self, namespace: str) -> None:
        self._name = f"{namespace}_log_records_dropped"

    def collect(self) -> Iterator[Any]:
        from swarmq.logging import dropped_log_records

        yield CounterMetricFamily(
            self._name,
            "Log records dropped because the logging queue was full",
            value=dropped_log_records(),
        )


class Metrics:
    """Typed accessors for SwarmQ's Prometheus metrics.

    Constructed once per worker/app. When prometheus_client is absent,
    every metric is a :class:`_NoopMetric` so the hot path stays free
    of ``if enabled`` branches.

    **Label cardinality warning.**
    ``task_name`` and ``queue`` labels are passed through verbatim
    from the message payload — they are NOT validated or normalised.
    Applications that embed user-controlled data in task names (e.g.
    ``send_email_user_12345``) will produce one Prometheus time series
    per unique value, which can OOM the Prometheus server and expose
    the embedded identifier to any scraper. Keep task names bounded
    (ideally a small enum) and route per-entity state through task
    arguments, not task names.
    """

    def __init__(
        self,
        registry: CollectorRegistry | None = None,
        namespace: str = "swarmq",
    ) -> None:
        self._enabled = _HAVE_PROMETHEUS
        if self._enabled:
            self.registry: CollectorRegistry | None = registry or CollectorRegistry()
            self.tasks_total: Counter | _NoopMetric = Counter(
                f"{namespace}_tasks_total",
                "Total tasks processed by outcome",
                labelnames=("task_name", "queue", "outcome"),
                registry=self.registry,
            )
            self.task_duration_seconds: Histogram | _NoopMetric = Histogram(
                f"{namespace}_task_duration_seconds",
                "Task run() duration in seconds",
                labelnames=("task_name", "queue"),
                registry=self.registry,
                # Buckets tuned for mixed-workload queues: sub-second
                # latency bands + long-tail coverage up to 5 min.
                buckets=(
                    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
                    1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0,
                ),
            )
            self.tasks_active: Gauge | _NoopMetric = Gauge(
                f"{namespace}_tasks_active",
                "Tasks currently in-flight per queue",
                labelnames=("queue",),
                registry=self.registry,
            )
            self.queue_depth: Gauge | _NoopMetric = Gauge(
                f"{namespace}_queue_depth",
                "Broker-reported queue message count",
                labelnames=("queue", "kind"),
                registry=self.registry,
            )
            self.worker_uptime_seconds: Gauge | _NoopMetric = Gauge(
                f"{namespace}_worker_uptime_seconds",
                "Seconds since worker start",
                labelnames=("worker_id",),
                registry=self.registry,
            )
            self.retries_total: Counter | _NoopMetric = Counter(
                f"{namespace}_retries_total",
                "Task retries scheduled",
                labelnames=("task_name", "queue"),
                registry=self.registry,
            )
            self.recyclings_total: Counter | _NoopMetric = Counter(
                f"{namespace}_recyclings_total",
                "Process recyclings (compute children and workers) by reason",
                labelnames=("kind", "reason"),
                registry=self.registry,
            )
            self.tasks_rejected_total: Counter | _NoopMetric = Counter(
                f"{namespace}_tasks_rejected_total",
                (
                    "Messages handed to the broker DLX without diagnostic "
                    "headers, by reason (U6, R13)"
                ),
                labelnames=("queue", "reason"),
                registry=self.registry,
            )
            self.dlq_messages: Gauge | _NoopMetric = Gauge(
                f"{namespace}_dlq_messages",
                "Dead-letter-queue live message count",
                labelnames=("queue",),
                registry=self.registry,
            )
            self.scheduler_leader: Gauge | _NoopMetric = Gauge(
                f"{namespace}_scheduler_leader",
                "1 when this worker holds the scheduler leader lock, else 0",
                labelnames=("worker_id",),
                registry=self.registry,
            )
            self.scheduler_cron_conflicts_total: Counter | _NoopMetric = Counter(
                f"{namespace}_scheduler_cron_conflicts_total",
                (
                    "Leader selection ticks that saw several live definitions "
                    "of one static cron for more than 10 minutes"
                ),
                labelnames=("schedule_name",),
                registry=self.registry,
            )
            self.scheduler_fires_unroutable_total: Counter | _NoopMetric = Counter(
                f"{namespace}_scheduler_fires_unroutable_total",
                (
                    "Scheduler fires the broker returned as unroutable (no queue "
                    "with this name): kind=cron_lost or oneshot_dropped"
                ),
                labelnames=("queue", "kind"),
                registry=self.registry,
            )
            self.scheduler_leader_outdated: Gauge | _NoopMetric = Gauge(
                f"{namespace}_scheduler_leader_outdated",
                (
                    "1 while this worker sees a scheduler leader running a "
                    "swarmq before 0.12 (minimum leader version), else 0"
                ),
                labelnames=("worker_id",),
                registry=self.registry,
            )
            self.consumer_reregistrations_total: Counter | _NoopMetric = Counter(
                f"{namespace}_consumer_reregistrations_total",
                (
                    "Consumers the worker registered again after the broker "
                    "ended them (consumer timeout, channel close)"
                ),
                labelnames=("queue",),
                registry=self.registry,
            )
            self.execution_guard_events_total: Counter | _NoopMetric = Counter(
                f"{namespace}_execution_guard_events_total",
                (
                    "Execution guard events: duplicate_skipped, deferred, "
                    "orphan_limit, lease_lost, defer_stalled"
                ),
                labelnames=("event",),
                registry=self.registry,
            )
            self.registry.register(_LogRecordsDroppedCollector(namespace))
        else:
            noop = _NoopMetric()
            self.registry = None
            self.tasks_total = noop
            self.task_duration_seconds = noop
            self.tasks_active = noop
            self.queue_depth = noop
            self.worker_uptime_seconds = noop
            self.retries_total = noop
            self.recyclings_total = noop
            self.tasks_rejected_total = noop
            self.dlq_messages = noop
            self.scheduler_leader = noop
            self.scheduler_cron_conflicts_total = noop
            self.scheduler_fires_unroutable_total = noop
            self.scheduler_leader_outdated = noop
            self.consumer_reregistrations_total = noop
            self.execution_guard_events_total = noop

    @property
    def enabled(self) -> bool:
        return self._enabled

    def count_consumer_reregistration(self, queue: str, reason: str) -> None:
        """Hook des Consumer-Waechters (Plan 2026-10-08-1541, U6). Der Grund
        steht im Log ``consumer_reregistered``, nicht im Label."""
        self.consumer_reregistrations_total.labels(queue=queue).inc()

    def count_execution_guard_event(self, event: str) -> None:
        """Hook des Ausfuehrungsschutzes (Plan 2026-10-08-1541, U8)."""
        self.execution_guard_events_total.labels(event=event).inc()

    def render(self) -> bytes:
        """Return the Prometheus text-exposition payload.

        Returns an empty body when prometheus_client is absent — the
        endpoint still responds 200 so scrapers don't spam errors.
        """
        if not self._enabled:
            return b"# swarmq metrics disabled - install prometheus_client\n"
        assert self.registry is not None  # _enabled impliziert echte Registry
        return generate_latest(self.registry)


class MetricsServer:
    """Minimal asyncio HTTP server exposing ``/metrics``.

    Uses the stdlib via :class:`swarmq._http.AsyncHttpServer`. The
    server lives inside the worker event loop and is shut down in
    tandem with the worker.

    **Bind-host default: ``127.0.0.1``.** Production
    deployments should front the worker with a reverse proxy /
    firewall boundary; /metrics has no built-in auth and task_name
    labels may carry sensitive payload if the application author
    embeds identifiers in task names (see :class:`Metrics`). Bind
    to ``0.0.0.0`` explicitly when the scraper runs on another host
    and the network path is otherwise restricted.
    """

    def __init__(
        self, metrics: Metrics,
        host: str = "127.0.0.1", port: int = 9090,
    ) -> None:
        from swarmq._http import AsyncHttpServer
        self._metrics = metrics
        self._server = AsyncHttpServer(
            self._handle_request,
            host=host, port=port, label="metrics_server",
        )

    async def start(self) -> None:
        await self._server.start()

    async def stop(self) -> None:
        await self._server.stop()

    async def _handle_request(
        self,
        method: str,
        path: str,
        writer: asyncio.StreamWriter,
    ) -> None:
        from swarmq._http import write_http_response
        if path.split("?")[0] != "/metrics":
            await write_http_response(writer, 404, b"not found\n")
            return
        body = self._metrics.render()
        await write_http_response(writer, 200, body, CONTENT_TYPE)


@asynccontextmanager
async def serve_metrics(
    metrics: Metrics, host: str = "127.0.0.1", port: int = 9090,
) -> AsyncIterator[MetricsServer]:
    """Async context manager: start MetricsServer on enter, stop on exit."""
    server = MetricsServer(metrics, host=host, port=port)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


def _install_lifecycle_collectors(
    dispatcher: SignalDispatcher, metrics: Metrics,
) -> None:
    from swarmq.signals import Signal

    @dispatcher.on_signal(Signal.TASK_STARTED)
    async def _task_started(**kw: Any) -> None:
        metrics.tasks_active.labels(queue=kw.get("queue", "")).inc()

    @dispatcher.on_signal(Signal.TASK_COMPLETED)
    async def _task_completed(**kw: Any) -> None:
        queue = kw.get("queue", "")
        name = kw.get("task_name", "")
        metrics.tasks_active.labels(queue=queue).dec()
        metrics.tasks_total.labels(
            task_name=name, queue=queue, outcome="completed",
        ).inc()
        duration_ms = kw.get("duration_ms")
        if isinstance(duration_ms, (int, float)):
            metrics.task_duration_seconds.labels(
                task_name=name, queue=queue,
            ).observe(duration_ms / 1000.0)

    @dispatcher.on_signal(Signal.TASK_RETRYING)
    async def _task_retrying(**kw: Any) -> None:
        metrics.retries_total.labels(
            task_name=kw.get("task_name", ""),
            queue=kw.get("queue", ""),
        ).inc()


def _install_recycling_counter(dispatcher: SignalDispatcher, metrics: Metrics) -> None:
    """Plan 2026-09-03-001 (R7/R23): jedes Recycling mit Grund als Zaehler."""
    from swarmq.signals import Signal

    async def _count(**kw: Any) -> None:
        metrics.recyclings_total.labels(
            kind=str(kw.get("kind", "")), reason=str(kw.get("reason", "")),
        ).inc()

    dispatcher.on_signal(Signal.WORKER_RECYCLING)(_count)


def _install_rejected_counter(dispatcher: SignalDispatcher, metrics: Metrics) -> None:
    """U6 (R13): jede Ablehnung am DLX-Fallback mit ihrem Grund.

    Getrennt von ``tasks_total{outcome="dlq"}``: die beiden Wege haben
    verschiedene Ursachen und verschiedene Gegenmassnahmen, und ein Alert auf
    "wir dead-lettern" soll nicht bei jeder zu grossen Nachricht anschlagen.
    """
    from swarmq.signals import Signal

    async def _count(**kw: Any) -> None:
        metrics.tasks_rejected_total.labels(
            queue=str(kw.get("queue") or ""), reason=str(kw.get("reason") or ""),
        ).inc()

    dispatcher.on_signal(Signal.TASK_REJECTED)(_count)


def _install_outcome_counters(
    dispatcher: SignalDispatcher, metrics: Metrics,
) -> None:
    """Ein generischer Zähler-Handler pro Terminal-Outcome.

    ``dec_active=True`` für die Outcomes, die einen laufenden Task beenden
    (failed/cancelled). ``expired`` (R4.4) und ``dlq`` zählen die terminalen
    Zustände, auf die Operatoren alerten — ``dlq`` dekrementiert nicht,
    weil der vorangehende TASK_FAILED-Handler das bereits tut.
    """
    from swarmq.signals import Signal

    outcomes: list[tuple[Any, str, bool]] = [
        (Signal.TASK_FAILED, "failed", True),
        (Signal.TASK_CANCELLED, "cancelled", True),
        (Signal.TASK_RATE_LIMITED, "rate_limited", False),
        (Signal.TASK_DUPLICATE_DETECTED, "duplicate", False),
        (Signal.TASK_EXPIRED, "expired", False),
        (Signal.TASK_DLQ, "dlq", False),
    ]
    for signal, outcome, dec_active in outcomes:
        def _make(outcome: str = outcome, dec_active: bool = dec_active) -> Any:
            async def _count(**kw: Any) -> None:
                queue = kw.get("queue", "")
                if dec_active:
                    metrics.tasks_active.labels(queue=queue).dec()
                metrics.tasks_total.labels(
                    task_name=kw.get("task_name", ""),
                    queue=queue,
                    outcome=outcome,
                ).inc()
            return _count
        dispatcher.on_signal(signal)(_make())


def _install_scheduler_gauge(
    dispatcher: SignalDispatcher, metrics: Metrics,
) -> None:
    """Leader-Status als 0/1-Gauge, damit Split-Brain (zwei Leader) und
    Zero-Leader (Cron still gestoppt) alertbar sind; dazu der Zähler langer
    Cron-Konflikte (Plan 2026-10-08-1541, R12) und die Gauge eines veralteten
    Leaders (KTD11)."""
    from swarmq.signals import Signal

    @dispatcher.on_signal(Signal.SCHEDULER_LEADER)
    @dispatcher.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
    async def _scheduler_became_leader(**kw: Any) -> None:
        metrics.scheduler_leader.labels(
            worker_id=str(kw.get("worker_id", "")),
        ).set(1)

    @dispatcher.on_signal(Signal.SCHEDULER_CRON_CONFLICT)
    async def _cron_conflict(**kw: Any) -> None:
        metrics.scheduler_cron_conflicts_total.labels(
            schedule_name=str(kw.get("schedule_name", "")),
        ).inc()

    @dispatcher.on_signal(Signal.SCHEDULER_FIRE_UNROUTABLE)
    async def _fire_unroutable(**kw: Any) -> None:
        metrics.scheduler_fires_unroutable_total.labels(
            queue=str(kw.get("queue", "")),
            kind=str(kw.get("kind", "")),
        ).inc()

    @dispatcher.on_signal(Signal.SCHEDULER_LEADER_OUTDATED)
    async def _leader_outdated(**kw: Any) -> None:
        metrics.scheduler_leader_outdated.labels(
            worker_id=str(kw.get("worker_id", "")),
        ).set(1 if kw.get("outdated") else 0)

    @dispatcher.on_signal(Signal.SCHEDULER_STANDBY)
    @dispatcher.on_signal(Signal.SCHEDULER_LEADER_HANDOVER)
    @dispatcher.on_signal(Signal.WORKER_SHUTDOWN)
    async def _scheduler_not_leader(**kw: Any) -> None:
        metrics.scheduler_leader.labels(
            worker_id=str(kw.get("worker_id", "")),
        ).set(0)


def install_signal_collectors(
    dispatcher: SignalDispatcher, metrics: Metrics,
) -> None:
    """Register Metrics-updater handlers on the given SignalDispatcher.

    Called once by the worker during start-up. Each registered handler
    pulls the relevant labels out of the signal kwargs and updates the
    appropriate counter/gauge/histogram. The signal schema is documented
    in ``signals.py``; the three helpers below are the single place where
    metric labels are derived from signal payloads.
    """
    _install_lifecycle_collectors(dispatcher, metrics)
    _install_outcome_counters(dispatcher, metrics)
    _install_recycling_counter(dispatcher, metrics)
    _install_rejected_counter(dispatcher, metrics)
    _install_scheduler_gauge(dispatcher, metrics)
