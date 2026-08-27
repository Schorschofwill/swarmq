"""Scheduler-Fassade (Paket seit Plan U11). ``swarmq.scheduler`` bleibt der
Importpfad; Patches zielen auf die Submodule (``swarmq.scheduler.loop.logger``
usw.). ``SCHEDULER_LOCK_KEY``/``SCHEDULER_LOCK_NAME``/``lock_key`` bleiben
hier importierbar (KTD10, leben seit U18 in ``swarmq.locking``)."""

from swarmq.locking import SCHEDULER_LOCK_KEY, SCHEDULER_LOCK_NAME, lock_key
from swarmq.scheduler.definition import (
    ScheduleDefinition,
    ScheduleStore,
    make_schedfire_key,
    parse_schedfire_key,
)
from swarmq.scheduler.loop import SchedulerLoop

__all__ = [
    "SCHEDULER_LOCK_KEY",
    "SCHEDULER_LOCK_NAME",
    "ScheduleDefinition",
    "ScheduleStore",
    "SchedulerLoop",
    "lock_key",
    "make_schedfire_key",
    "parse_schedfire_key",
]
