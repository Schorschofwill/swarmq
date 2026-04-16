"""SwarmQ Task base class and TaskInfo."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class DeliveryMode(enum.Enum):
    AT_LEAST_ONCE = "at_least_once"
    FIRE_AND_FORGET = "fire_and_forget"


_VALID_BACKOFF_STRATEGIES = {"exponential", "linear", "fixed"}


@dataclass(frozen=True)
class TaskInfo:
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
            # Validate eagerly at construction time so a bad string fails
            # in the task-module import, not inside the worker loop.
            # Imported locally to avoid a circular dep via
            # swarmq.limits -> swarmq.lua at module top-level.
            from swarmq.limits import parse_rate_limit

            parse_rate_limit(self.rate_limit)


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
