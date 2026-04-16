"""SwarmQ Task base class and TaskInfo."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field

from swarmq.limits import parse_rate_limit
from swarmq.locking import compute_lock_ttl_ms


class DeliveryMode(enum.Enum):
    AT_LEAST_ONCE = "at_least_once"
    FIRE_AND_FORGET = "fire_and_forget"


_VALID_BACKOFF_STRATEGIES = {"exponential", "linear", "fixed"}


@dataclass(frozen=True)
class TaskInfo:
    """Declarative task metadata. Frozen — set once at class definition.

    Field notes (only the less-obvious ones):
      * ``rate_limit``: optional Celery-style rate limit string,
        e.g. ``"100/m"``, ``"10/s"``, ``"1000/h"``. Enforced globally
        across all workers via Valkey. Limited messages are requeued
        with a per-Lua-script delay rather than DLQ'd. Validated at
        construction (a bad string raises ``ValueError`` immediately).
        Supported units: ``s``, ``m``/``min``, ``h``/``hour``.
      * ``lock``: optional resource name (or format string like
        ``"user-{user_id}"``) that gates concurrent execution of this
        task across all workers. With ``max_concurrency=1`` (default)
        it acts as a Mutex; with ``max_concurrency>1`` as a Semaphore.
        **Requires ``hard_timeout`` to be set** — the lock TTL is
        derived from it via ``max(30s, 0.25 * hard_timeout)``. Setting
        ``lock`` without ``hard_timeout`` raises ``ValueError`` at
        TaskInfo construction (fail-fast at module import).
      * ``max_concurrency``: only meaningful when ``lock`` is set.
        Maximum number of concurrent task instances allowed across all
        workers for this lock name. Defaults to 1 (Mutex).
    """

    name: str
    queues: tuple[str, ...] = field(default=())
    soft_timeout: int | None = None
    hard_timeout: int | None = None
    delivery: DeliveryMode = DeliveryMode.AT_LEAST_ONCE
    result_ttl: int | None = None
    max_retries: int = 0
    retry_backoff: str = "exponential"
    retry_exceptions: tuple[type[BaseException], ...] | None = None
    retry_base_delay: float = 1.0
    rate_limit: str | None = None
    lock: str | None = None
    max_concurrency: int = 1

    def __init__(
        self,
        name: str,
        queues: list[str],
        soft_timeout: int | None = None,
        hard_timeout: int | None = None,
        delivery: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
        result_ttl: int | None = None,
        max_retries: int = 0,
        retry_backoff: str = "exponential",
        retry_exceptions: list[type[BaseException]] | None = None,
        retry_base_delay: float = 1.0,
        rate_limit: str | None = None,
        lock: str | None = None,
        max_concurrency: int = 1,
    ) -> None:
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "queues", tuple(queues))
        object.__setattr__(self, "soft_timeout", soft_timeout)
        object.__setattr__(self, "hard_timeout", hard_timeout)
        object.__setattr__(self, "delivery", delivery)
        object.__setattr__(self, "result_ttl", result_ttl)
        object.__setattr__(self, "max_retries", max_retries)
        object.__setattr__(self, "retry_backoff", retry_backoff)
        object.__setattr__(
            self,
            "retry_exceptions",
            tuple(retry_exceptions) if retry_exceptions is not None else None,
        )
        object.__setattr__(self, "retry_base_delay", retry_base_delay)
        object.__setattr__(self, "rate_limit", rate_limit)
        object.__setattr__(self, "lock", lock)
        object.__setattr__(self, "max_concurrency", max_concurrency)
        self.__post_init__()

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name must not be empty")
        if len(self.name) > 255:
            raise ValueError("name must not exceed 255 characters")
        if not self.queues:
            raise ValueError("queues must have at least 1 element")
        if (
            self.soft_timeout is not None
            and self.hard_timeout is not None
            and self.hard_timeout <= self.soft_timeout
        ):
            raise ValueError("hard_timeout must be greater than soft_timeout")
        if self.retry_backoff not in _VALID_BACKOFF_STRATEGIES:
            raise ValueError(
                f"retry_backoff must be one of {sorted(_VALID_BACKOFF_STRATEGIES)}, "
                f"got {self.retry_backoff!r}"
            )
        if self.retry_base_delay < 0:
            raise ValueError("retry_base_delay must be >= 0")
        if self.rate_limit is not None:
            # Validate eagerly at construction so a bad string fails at
            # task-module import time, not inside the worker loop.
            parse_rate_limit(self.rate_limit)
        if self.max_concurrency < 1:
            raise ValueError(
                f"max_concurrency must be >= 1 (got {self.max_concurrency})"
            )
        if self.max_concurrency != 1 and self.lock is None:
            raise ValueError(
                "max_concurrency requires lock to be set"
            )
        if self.lock is not None:
            # Empty / whitespace-only lock would render to the bare Valkey
            # namespace key "sq:lock:" and silently collide with every
            # other empty-locked task in the system. Reject at construction.
            if not self.lock.strip():
                raise ValueError(
                    "lock must not be empty or whitespace-only"
                )
            # Locks need a finite ceiling for safe TTL derivation. Surface
            # the requirement at TaskInfo construction (module import) so
            # the bug fires at the obvious site, not inside the worker loop
            # at first schedule. compute_lock_ttl_ms encapsulates the
            # contract — its ValueError is the right message verbatim.
            compute_lock_ttl_ms(self.hard_timeout)


class Task:
    task_info: TaskInfo
    _registered_tasks: list[type] = []

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if hasattr(cls, "task_info"):
            Task._registered_tasks.append(cls)

    @classmethod
    def _clear_registered_tasks(cls) -> None:
        cls._registered_tasks.clear()

    async def pre_execution(self) -> None:
        pass

    async def run(self, *args, **kwargs):
        raise NotImplementedError

    async def post_execution(self) -> None:
        pass
