"""Loop-Test-Helfer (Plan U11) — verbatim aus ``test_scheduler_loop.py``."""

from __future__ import annotations

import pytest

from swarmq.contracts.coordination import LockToken
from swarmq.locking import SCHEDULER_LOCK_NAME
from swarmq.scheduler import (
    ScheduleDefinition,
    SchedulerLoop,
)

# AGENTS.md §Async-Code testen rule 3: 5s per-test timeout. Narrower
# than the pyproject default (30s); fails fast on deadlock without
# burning CI time (Unit 3 / H-6).
pytestmark = [pytest.mark.timeout(5)]



_LEADER_TOKEN = LockToken(name=SCHEDULER_LOCK_NAME, owner="test-owner", max_concurrency=1)


def _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher,
               owner_token="test-owner", version="0.10.0"):
    return SchedulerLoop(
        backend=backend,
        broker=broker,
        config=config,
        lock_manager=lock_manager,
        store=store,
        signal_dispatcher=signal_dispatcher,
        owner_token=owner_token,
        worker_id="w-1",
        version=version,
    )


def _def(name="cron_a", *, cron="*/5 * * * *", fire_ts_ms=1_700_000_000_000,
         source="static", disabled=False) -> ScheduleDefinition:
    return ScheduleDefinition(
        schedule_name=name,
        task_name="ping",
        args=(),
        kwargs={},
        queue="default",
        cron=cron,
        fire_ts_ms=fire_ts_ms,
        source=source,
        disabled=disabled,
    )


def _cron_def(
    *, name="cron_unique", task_name="cron_task", fire_ts_ms=1_000,
    args=(), kwargs=None, source="static", dedup_hash=None,
) -> ScheduleDefinition:
    return ScheduleDefinition(
        schedule_name=name,
        task_name=task_name,
        args=args,
        kwargs=kwargs if kwargs is not None else {},
        queue="default",
        cron="*/5 * * * *",
        fire_ts_ms=fire_ts_ms,
        source=source,
        dedup_hash=dedup_hash,
    )
