"""Client-Fassade (Paket seit Plan U10). ``swarmq.client.Client`` bleibt der
Importpfad; ``_MISSING`` wird re-exportiert (app.py). Die Fassade
re-exportiert keine gepatchten Namen — Patches zielen auf die Submodule
(``swarmq.client.results.asyncio.sleep`` usw.)."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from swarmq.middleware import MiddlewareChain, coerce_middleware_chain_factory
from swarmq.registry import TaskRegistry
from swarmq.signals import SignalDispatcher

if TYPE_CHECKING:
    from swarmq.client.schedule import ClientBackend, ClientBroker
    from swarmq.scheduler import ScheduleStore

from swarmq.client.results import ClientResults
from swarmq.client.schedule import _MISSING as _MISSING  # Re-Export (app.py)
from swarmq.client.schedule import ClientScheduling
from swarmq.client.schedule import _MissingType as _MissingType
from swarmq.client.workflows import ClientWorkflows


class Client(ClientScheduling, ClientResults, ClientWorkflows):
    """Schedule tasks and retrieve results.

    Operates in two modes:
    - With registry: validates task names against the registry and resolves
      queues from TaskInfo.
    - Without registry (Client-Modus, spec 2.6): skips validation, requires
      an explicit queue for every schedule() call.
    """

    def __init__(
        self,
        broker: ClientBroker,
        backend: ClientBackend,
        registry: TaskRegistry | None = None,
        middleware_chain: MiddlewareChain
        | Callable[[], MiddlewareChain]
        | None = None,
        signal_dispatcher: SignalDispatcher | None = None,
        backend_op_timeout: float | None = None,
    ) -> None:
        self._broker = broker
        self._backend = backend
        self._registry = registry
        # R3.5: bound every Valkey call on the enqueue hot path so a
        # hung backend cannot wedge Client.schedule. None preserves
        # pre-R3.5 behavior (no enforcement at the Client layer);
        # SwarmQ passes config.backend_op_timeout through so app-mode
        # callers inherit the same guard the Worker already uses.
        self._backend_op_timeout = backend_op_timeout
        # Accept either a static MiddlewareChain (backward compat) or a
        # zero-arg factory callable that returns a fresh chain on each call.
        # Storing a factory lets us see middleware registered AFTER Client
        # construction — e.g. when SwarmQ.add_middleware() is called after
        # the Client has already been built.
        self._middleware_chain_factory = coerce_middleware_chain_factory(
            middleware_chain
        )
        self._signal_dispatcher = (
            signal_dispatcher if signal_dispatcher is not None else SignalDispatcher()
        )
        # Lazy-init: ScheduleStore requires a
        # connected backend to register its Lua scripts — but Client is
        # constructed before backend.connect(). We defer the construction to
        # the first schedule(delay=|eta=) call.
        self._schedule_store: ScheduleStore | None = None

    def _get_chain(self) -> MiddlewareChain:
        """Return a live MiddlewareChain for the current schedule() call."""
        return self._middleware_chain_factory()


__all__ = ["Client"]
