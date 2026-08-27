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
import logging
from collections.abc import AsyncIterator
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

    _HAVE_PROMETHEUS = True
except ImportError:  # pragma: no cover — optional
    prometheus_client = None  # type: ignore[assignment]
    CollectorRegistry = None  # type: ignore[assignment,misc]
    Counter = None  # type: ignore[assignment,misc]
    Gauge = None  # type: ignore[assignment,misc]
    Histogram = None  # type: ignore[assignment,misc]
    generate_latest = None  # type: ignore[assignment]

    _HAVE_PROMETHEUS = False

if TYPE_CHECKING:
    from swarmq.signals import SignalDispatcher

logger = logging.getLogger("swarmq.metrics")

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


class Metrics:
    """Typed accessors for SwarmQ's Prometheus metrics.

    Constructed once per worker/app. When prometheus_client is absent,
    every metric is a :class:`_NoopMetric` so the hot path stays free
    of ``if enabled`` branches.

    **Label cardinality warning (ce-review SEC-005).**
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
        else:
            noop = _NoopMetric()
            self.registry = None
            self.tasks_total = noop
            self.task_duration_seconds = noop
            self.tasks_active = noop
            self.queue_depth = noop
            self.worker_uptime_seconds = noop
            self.retries_total = noop
            self.dlq_messages = noop
            self.scheduler_leader = noop

    @property
    def enabled(self) -> bool:
        return self._enabled

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

    Uses the stdlib via :class:`swarmq._http.AsyncHttpServer`
    (ce-review M-01 dedup). The server lives inside the worker event
    loop and is shut down in tandem with the worker.

    **Bind-host default: ``127.0.0.1``** (ce-review SEC-004). Production
    deployments should front the worker with a reverse proxy /
    firewall boundary; /metrics has no built-in auth and task_name
    labels may carry sensitive payload if the application author
    embeds identifiers in task names (see ce-review SEC-005). Bind
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


def install_signal_collectors(  # noqa: C901  # Follow-Up 0.2.x
    dispatcher: SignalDispatcher, metrics: Metrics,
) -> None:
    """Register Metrics-updater handlers on the given SignalDispatcher.

    Called once by the worker during start-up. Each registered handler
    pulls the relevant labels out of the signal kwargs and updates the
    appropriate counter/gauge/histogram. The signal schema is documented
    in ``signals.py``; this function is the single place where metric
    labels are derived from signal payloads.
    """
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

    @dispatcher.on_signal(Signal.TASK_FAILED)
    async def _task_failed(**kw: Any) -> None:
        queue = kw.get("queue", "")
        metrics.tasks_active.labels(queue=queue).dec()
        metrics.tasks_total.labels(
            task_name=kw.get("task_name", ""),
            queue=queue,
            outcome="failed",
        ).inc()

    @dispatcher.on_signal(Signal.TASK_CANCELLED)
    async def _task_cancelled(**kw: Any) -> None:
        queue = kw.get("queue", "")
        metrics.tasks_active.labels(queue=queue).dec()
        metrics.tasks_total.labels(
            task_name=kw.get("task_name", ""),
            queue=queue,
            outcome="cancelled",
        ).inc()

    @dispatcher.on_signal(Signal.TASK_RETRYING)
    async def _task_retrying(**kw: Any) -> None:
        metrics.retries_total.labels(
            task_name=kw.get("task_name", ""),
            queue=kw.get("queue", ""),
        ).inc()

    @dispatcher.on_signal(Signal.TASK_RATE_LIMITED)
    async def _task_rate_limited(**kw: Any) -> None:
        metrics.tasks_total.labels(
            task_name=kw.get("task_name", ""),
            queue=kw.get("queue", ""),
            outcome="rate_limited",
        ).inc()

    @dispatcher.on_signal(Signal.TASK_DUPLICATE_DETECTED)
    async def _task_duplicate(**kw: Any) -> None:
        metrics.tasks_total.labels(
            task_name=kw.get("task_name", ""),
            queue=kw.get("queue", ""),
            outcome="duplicate",
        ).inc()

    @dispatcher.on_signal(Signal.TASK_EXPIRED)
    async def _task_expired(**kw: Any) -> None:
        # R4.4 Task Expiry — outcome="expired" label documented in the
        # module docstring. Without this handler the counter would
        # never increment (ce-review T-06/TG-001).
        metrics.tasks_total.labels(
            task_name=kw.get("task_name", ""),
            queue=kw.get("queue", ""),
            outcome="expired",
        ).inc()

    @dispatcher.on_signal(Signal.TASK_DLQ)
    async def _task_dlq(**kw: Any) -> None:
        # 2026-06-02 audit P1-4: terminal dead-lettering was invisible to
        # Prometheus (outcome="dlq" never incremented). tasks_active is
        # already decremented by the preceding TASK_FAILED handler, so
        # this only bumps the terminal counter operators alert on.
        metrics.tasks_total.labels(
            task_name=kw.get("task_name", ""),
            queue=kw.get("queue", ""),
            outcome="dlq",
        ).inc()

    # 2026-06-02 audit P1-5: scheduler-leader status was unobservable
    # despite the SCHEDULER_* signals firing correctly. Map them to a
    # 0/1 gauge so a split-brain (two leaders) or zero-leader (cron
    # silently stopped) condition can be alerted on.
    @dispatcher.on_signal(Signal.SCHEDULER_LEADER)
    @dispatcher.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
    async def _scheduler_became_leader(**kw: Any) -> None:
        metrics.scheduler_leader.labels(
            worker_id=str(kw.get("worker_id", "")),
        ).set(1)

    @dispatcher.on_signal(Signal.SCHEDULER_STANDBY)
    @dispatcher.on_signal(Signal.SCHEDULER_LEADER_HANDOVER)
    @dispatcher.on_signal(Signal.WORKER_SHUTDOWN)
    async def _scheduler_not_leader(**kw: Any) -> None:
        metrics.scheduler_leader.labels(
            worker_id=str(kw.get("worker_id", "")),
        ).set(0)
