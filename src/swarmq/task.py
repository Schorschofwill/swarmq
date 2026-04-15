"""SwarmQ Task base class and TaskInfo."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class DeliveryMode(enum.Enum):
    AT_LEAST_ONCE = "at_least_once"
    FIRE_AND_FORGET = "fire_and_forget"


@dataclass(frozen=True)
class TaskInfo:
    name: str
    queues: tuple[str, ...] = field(default=())
    soft_timeout: int | None = None
    hard_timeout: int | None = None
    delivery: DeliveryMode = DeliveryMode.AT_LEAST_ONCE
    result_ttl: int | None = None

    def __init__(
        self,
        name: str,
        queues: list[str],
        soft_timeout: int | None = None,
        hard_timeout: int | None = None,
        delivery: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
        result_ttl: int | None = None,
    ) -> None:
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "queues", tuple(queues))
        object.__setattr__(self, "soft_timeout", soft_timeout)
        object.__setattr__(self, "hard_timeout", hard_timeout)
        object.__setattr__(self, "delivery", delivery)
        object.__setattr__(self, "result_ttl", result_ttl)
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
