"""Health + readiness HTTP endpoints (R5.2).

Two endpoints, both stdlib-only (same asyncio HTTP server pattern as
:mod:`swarmq.metrics`):

- ``GET /health`` — liveness. Returns 200 unconditionally as long as
  the process and event loop are responsive. Used by orchestrators
  (Kubernetes, systemd) to decide whether to restart the worker.
- ``GET /ready`` — readiness. Returns 200 only when both the broker
  AND the backend are reachable (lightweight ping) and every named
  extra check passes — in a worker ``consumers`` (every consumed queue
  has an active consumer, Plan 2026-10-08-1541 U6) and, in CPU mode,
  ``compute``. Returns 503 with a JSON body listing which dependency is
  unhealthy when not. Used by load balancers and rolling-deploy
  automation to decide when a fresh worker is ready to accept traffic.
  Never use ``/ready`` as a liveness probe: a broker outage would make
  the orchestrator restart every worker at once.

``swarmq worker --check`` is a separate concern — it performs
offline config validation (module importable, watchfiles available,
watch-dirs resolvable) and does NOT touch broker or backend. For a
liveness/readiness probe at runtime, call the HTTP endpoints above
directly or use ``--health-port`` to expose them.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, NotRequired, TypedDict, cast

from swarmq.logging import _error_fields, get_logger

if TYPE_CHECKING:
    from swarmq.contracts import Publisher, ResultBackend

logger = get_logger(__name__)

#: Laengere Fehlertexte werden auf den Klassennamen reduziert.
_MAX_ERROR_TAG_LEN = 120

#: Zusatz-Check fuer ``/ready``: (ok, Fehlertext oder ``None``).
ReadinessCheck = Callable[[], Awaitable[tuple[bool, str | None]]]


class _DependencyState(TypedDict):
    ok: bool
    error: str | None


class ReadinessSnapshot(TypedDict):
    """Shape of :meth:`HealthChecker.check_all`.

    Documented for callers that compose /ready checks (Kubernetes
    probes, test fixtures, downstream health aggregators).
    """

    ready: bool
    broker: _DependencyState
    backend: _DependencyState
    compute: NotRequired[_DependencyState]
    consumers: NotRequired[_DependencyState]


class HealthChecker:
    """Pings broker + backend for the readiness endpoint and CLI probe.

    Each check is bounded by a 2s asyncio.wait_for so a slow remote
    cannot wedge the readiness response. Seit Plan U9 laufen beide Checks
    über die Vertragsmethode ``ping`` (Publisher/ResultBackend) — kein
    Zugriff auf ``_client``/``_connection`` mehr; damit ist Health gegen den
    In-Memory-Stub testbar.
    """

    def __init__(
        self,
        broker: Publisher,
        backend: ResultBackend,
        *,
        timeout_s: float = 2.0,
        checks: Mapping[str, ReadinessCheck] | None = None,
    ) -> None:
        self._broker = broker
        self._backend = backend
        self._timeout = timeout_s
        # Benannte Zusatz-Checks, jeder mit eigenem Eintrag im Snapshot:
        # ``compute`` (CPU-Modus, Plan 2026-09-03-001 U7 — tot bedeutet, kein
        # Task kann mehr rechnen) und ``consumers`` (Plan 2026-10-08-1541 U6).
        self._checks = dict(checks or {})

    @staticmethod
    def _sanitise_error(exc: BaseException) -> str:
        """Return a minimally-informative error tag for the /ready body.

        valkey-py and aio_pika exception messages embed host:port (and in some code paths the full URL
        with credentials). /ready is typically behind a reverse proxy
        but may be externally reachable in some deployments, so we
        return only the exception class + a short static reason and
        log the full detail server-side.
        """
        cls = type(exc).__name__
        # Heuristic: if the exception message is short and does NOT
        # look like a URL/host:port leak, keep it; otherwise collapse
        # to the class name only.
        msg = str(exc)
        if not msg:
            return cls
        if len(msg) > _MAX_ERROR_TAG_LEN or "://" in msg or "@" in msg:
            return cls
        # Strip host:port fragments. Two patterns:
        #  (a) '[::1]:6379' — IPv6 bracket notation
        #  (b) 'host.example:6379' — v4 / DNS
        msg = re.sub(r"\[[0-9a-fA-F:]+\]:\d+", "<redacted>", msg)
        msg = re.sub(r"[\w.-]+:\d+", "<redacted>", msg)
        return f"{cls}: {msg}"

    async def check_backend(self) -> tuple[bool, str | None]:
        """Return (ok, error_tag_or_None). ``ResultBackend.ping`` wirft vor
        ``connect()`` und bei Störung ``BackendConnectionError``."""
        try:
            await asyncio.wait_for(self._backend.ping(), timeout=self._timeout)
        except Exception as exc:  # noqa: BLE001 - Fehler wird geloggt, Check meldet nicht bereit
            logger.warning("check_backend_failed", **_error_fields(exc))
            return False, self._sanitise_error(exc)
        else:
            return True, None

    async def check_broker(self) -> tuple[bool, str | None]:
        """``Publisher.ping`` wirft, wenn nicht verbunden oder die Verbindung
        nicht bereit ist (transportnah, keine Queue-Deklarationen)."""
        try:
            await asyncio.wait_for(self._broker.ping(), timeout=self._timeout)
        except Exception as exc:  # noqa: BLE001 - Fehler wird geloggt, Check meldet nicht bereit
            logger.warning("check_broker_failed", **_error_fields(exc))
            return False, self._sanitise_error(exc)
        else:
            return True, None

    async def check_all(self) -> ReadinessSnapshot:
        """Return a structured snapshot suitable for JSON serialisation."""
        broker_ok, broker_err = await self.check_broker()
        backend_ok, backend_err = await self.check_backend()
        snapshot = ReadinessSnapshot(
            ready=broker_ok and backend_ok,
            broker=_DependencyState(ok=broker_ok, error=broker_err),
            backend=_DependencyState(ok=backend_ok, error=backend_err),
        )
        for name, check in self._checks.items():
            try:
                ok, err = await asyncio.wait_for(check(), timeout=self._timeout)
            except Exception as exc:  # noqa: BLE001 - Nutzer-Check darf Readiness-Abfrage nicht abbrechen
                ok, err = False, self._sanitise_error(exc)
            cast("dict[str, object]", snapshot)[name] = _DependencyState(ok=ok, error=err)
            snapshot["ready"] = snapshot["ready"] and ok
        return snapshot


def consumer_check(
    gaps: Callable[[], Mapping[str, float]],
    expected: Sequence[str],
    *,
    grace_s: float,
    missing: Callable[[], Iterable[str]] | None = None,
) -> ReadinessCheck:
    """``/ready``-Check: jede erwartete Queue hat einen aktiven Consumer (U6, R2).

    ``gaps`` liefert je konsumierter Queue die Sekunden ohne aktiven Consumer
    (``Broker.consumer_gaps``); rot wird es erst nach ``grace_s``. Eine
    Queue, die noch gar nicht konsumiert wird, macht den Worker sofort nicht
    bereit — vor dem ersten ``consume`` ist er es nicht. ``missing`` nennt
    konsumierte Queues, die beim Broker fehlen (``Broker.consumer_missing_queues``,
    PF-20, E-083): Sie machen sofort nicht bereit, ohne Gnadenfrist.
    """

    async def _check() -> tuple[bool, str | None]:
        gone = [] if missing is None else list(missing())
        if gone:
            return False, f"consumed queue missing at the broker: {', '.join(gone)}"
        current = gaps()
        unconsumed = [queue for queue in expected if queue not in current]
        if unconsumed:
            return False, f"not consuming: {', '.join(unconsumed)}"
        # Kurzes Fehlen ist normal: Der Consumer-Waechter meldet binnen zwei
        # Takten neu an. Erst danach ist es ein Ausfall.
        stale = [queue for queue in expected if current[queue] > grace_s]
        if stale:
            return False, f"no active consumer for more than {grace_s}s: {', '.join(stale)}"
        return True, None

    return _check


class HealthServer:
    """Minimal asyncio HTTP server exposing ``/health`` and ``/ready``.

    Same shape as :class:`swarmq.metrics.MetricsServer` — kept separate
    so an operator can run health on a different port (typical pattern:
    health on the same port as the worker proxy, metrics on the
    metrics-scraping port).

    **Bind-host default: ``127.0.0.1``.**
    Kubernetes liveness / readiness probes run in the same network
    namespace as the pod by default, so loopback is sufficient. Bind
    to ``0.0.0.0`` only when the probe originates outside the pod /
    container.

    **No built-in auth** (mirrors the /metrics note):
    ``/ready`` reveals which dependency (broker/backend) is down plus a
    sanitised error tag — an infrastructure fingerprint. When binding
    to ``0.0.0.0``, front the endpoint with a reverse proxy / firewall
    boundary.
    """

    def __init__(
        self,
        checker: HealthChecker,
        *,
        host: str = "127.0.0.1",
        port: int = 8080,
        liveness_started: float | None = None,
    ) -> None:
        from swarmq._http import AsyncHttpServer

        self._checker = checker
        self._started_at = liveness_started or time.monotonic()
        self._server = AsyncHttpServer(
            self._handle_request,
            host=host,
            port=port,
            label="health_server",
        )

    async def start(self) -> None:
        await self._server.start()

    async def stop(self) -> None:
        await self._server.stop()

    async def _handle_request(
        self,
        method: str,  # noqa: ARG002 - Handler-Signatur des HTTP-Servers vorgegeben
        path: str,
        writer: asyncio.StreamWriter,
    ) -> None:
        from swarmq._http import write_http_response

        base_path = path.split("?", maxsplit=1)[0]
        if base_path == "/health":
            body = json.dumps(
                {
                    "status": "ok",
                    "uptime_s": round(time.monotonic() - self._started_at, 3),
                }
            ).encode()
            await write_http_response(writer, 200, body, "application/json")
        elif base_path == "/ready":
            snapshot = await self._checker.check_all()
            status = 200 if snapshot["ready"] else 503
            body = json.dumps(snapshot).encode()
            await write_http_response(writer, status, body, "application/json")
        else:
            await write_http_response(writer, 404, b"not found\n")


@asynccontextmanager
async def serve_health(
    checker: HealthChecker,
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> AsyncGenerator[HealthServer]:
    """Async context manager: start HealthServer on enter, stop on exit."""
    server = HealthServer(checker, host=host, port=port)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()
