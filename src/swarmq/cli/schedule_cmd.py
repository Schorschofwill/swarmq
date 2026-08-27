"""``swarmq schedule *`` subcommand handlers.

Groups list/add/remove/status handlers plus the schedule-specific
table renderer. Each handler connects to the backend only (no broker)
and closes the backend in a ``finally`` block so a crashing handler
does not leak the connection.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
import time
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import msgspec

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend

from swarmq.cli._shared import _render_table
from swarmq.exceptions import ConfigurationError
from swarmq.scheduler import ScheduleDefinition, ScheduleStore
from swarmq.serialization import compute_dedup_hash
from swarmq.task import UniqueUntil

# ``_backend_from_env`` is resolved LAZILY via ``swarmq.cli`` so that
# a future ``monkeypatch.setattr(cli, "_backend_from_env", fake)`` in
# schedule tests intercepts the call. Direct
# ``from swarmq.cli._shared import _backend_from_env`` would bind the
# name into this module's namespace and silently bypass the patch —
# the same trap ``dlq_cmd._get_broker_factory`` avoids.


def _get_backend_factory() -> Callable[[], Coroutine[Any, Any, ValkeyBackend]]:
    """Lazy import of ``_backend_from_env`` through the facade."""
    from swarmq.cli import _backend_from_env
    return _backend_from_env


def _format_schedule_table(schedules: list[ScheduleDefinition]) -> str:
    """Plain-text table for terminal output — stable column ordering.

    Columns include R3.5 dedup state (DEDUP + UNIQUE_UNTIL) so operators
    can tell client-originated deferred-dedup entries from plain CLI or
    static-cron ones at a glance (ce-review P3 #23).
    """
    rows = [(
        "NAME", "CRON", "TASK", "QUEUE", "NEXT_FIRE",
        "DISABLED", "SOURCE", "DEDUP", "UNIQUE_UNTIL",
    )]
    for s in schedules:
        next_fire = (
            datetime.fromtimestamp(s.fire_ts_ms / 1000, tz=UTC).isoformat()
            if s.fire_ts_ms is not None
            else "-"
        )
        dedup_state = f"{s.dedup_hash[:8]}…" if s.dedup_hash else "-"
        unique_until = s.unique_until if s.dedup_hash else "-"
        rows.append((
            s.schedule_name,
            s.cron or "-",
            s.task_name,
            s.queue,
            next_fire,
            "yes" if s.disabled else "no",
            s.source,
            dedup_state,
            unique_until,
        ))
    return _render_table(rows, "(no schedules)")


async def _run_schedule_list(args: argparse.Namespace) -> None:
    backend = await _get_backend_factory()()
    try:
        store = ScheduleStore(backend)
        schedules = await store.list_schedules()
        schedules.sort(key=lambda s: s.schedule_name)
        if args.format == "json":
            payload = [msgspec.to_builtins(s) for s in schedules]
            sys.stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        else:
            sys.stdout.write(_format_schedule_table(schedules) + "\n")
    finally:
        await backend.close()


async def _run_schedule_add(args: argparse.Namespace) -> None:  # noqa: C901, PLR0915  # Follow-Up 0.2.x
    try:
        parsed_args = json.loads(args.args)
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"--args is not valid JSON: {exc}") from exc
    if not isinstance(parsed_args, list):
        raise ConfigurationError("--args must be a JSON array")

    try:
        parsed_kwargs = json.loads(args.kwargs)
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"--kwargs is not valid JSON: {exc}") from exc
    if not isinstance(parsed_kwargs, dict):
        raise ConfigurationError("--kwargs must be a JSON object")

    if args.cron is not None:
        cron = args.cron
        # Compute first fire via croniter so the zset has a real score.
        from croniter import CroniterBadCronError, croniter
        try:
            next_dt = croniter(cron, datetime.now(UTC)).get_next(datetime)
        except (CroniterBadCronError, ValueError) as exc:
            raise ConfigurationError(
                f"invalid cron expression: {cron!r} ({exc})"
            ) from exc
        fire_ts_ms = int(next_dt.timestamp() * 1000)
    else:
        # --at <iso-datetime>
        try:
            dt = datetime.fromisoformat(args.at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ConfigurationError(
                f"invalid --at datetime: {args.at!r} ({exc})"
            ) from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        cron = None
        fire_ts_ms = int(dt.timestamp() * 1000)

    # R3.5 ce-review P3 #23: --unique-for claims the dedup flag before
    # the ScheduleStore insert, matching Client._schedule_deferred
    # semantics. On duplicate, the insert is skipped and the existing
    # claim's owner is reported so operators can correlate.
    unique_for: int | None = getattr(args, "unique_for", None)
    unique_until: UniqueUntil = getattr(args, "unique_until", "completion")

    backend = await _get_backend_factory()()
    dedup_hash: str | None = None
    existing_owner: str | None = None
    try:
        if unique_for is not None:
            dedup_hash = compute_dedup_hash(
                args.task, tuple(parsed_args), parsed_kwargs,
            )
            # TTL = unique_for + fire_offset, ceiling-div so sub-second
            # deltas still get a full second of padding (same formula
            # as Client._schedule_deferred).
            now_ms = int(time.time() * 1000)
            fire_offset_ms = max(0, fire_ts_ms - now_ms)
            fire_offset_s = (fire_offset_ms + 999) // 1000
            ttl_ms = (unique_for + int(fire_offset_s)) * 1000
            existing_owner = await backend.setnx_unique(
                dedup_hash, args.name, ttl_ms,
            )

        definition = ScheduleDefinition(
            schedule_name=args.name,
            task_name=args.task,
            args=tuple(parsed_args),
            kwargs=parsed_kwargs,
            queue=args.queue,
            cron=cron,
            fire_ts_ms=fire_ts_ms,
            source="cli_unique" if dedup_hash is not None else "cli",
            dedup_hash=dedup_hash,
            unique_until=unique_until,
        )

        if existing_owner is not None:
            # Dup detected — don't insert; surface the clash to the
            # operator. The flag already belongs to another schedule.
            inserted = False
        else:
            store = ScheduleStore(backend)
            try:
                inserted = await store.add_schedule(definition)
            except BaseException:
                # Rollback the dedup claim on store failure (P1 #1).
                if dedup_hash is not None:
                    with contextlib.suppress(Exception):
                        await backend.release_unique(dedup_hash, args.name)
                raise

        if args.format == "json":
            payload = {
                "inserted": inserted,
                "name": args.name,
                "already_exists": not inserted,
            }
            if unique_for is not None:
                payload["dedup_hash"] = dedup_hash
                payload["unique_until"] = unique_until
                if existing_owner is not None:
                    payload["duplicate_of"] = existing_owner
            sys.stdout.write(json.dumps(payload) + "\n")
        elif inserted:
            sys.stdout.write(f"added schedule {args.name!r}\n")
        elif existing_owner is not None:
            sys.stdout.write(
                f"schedule {args.name!r} rejected — dedup flag already "
                f"held by {existing_owner!r} (args duplicate within "
                f"{unique_for}s TTL)\n"
            )
        else:
            sys.stdout.write(
                f"schedule {args.name!r} already exists; use "
                f"'schedule remove --permanent' first to replace it\n"
            )
    finally:
        await backend.close()


async def _run_schedule_remove(args: argparse.Namespace) -> None:
    backend = await _get_backend_factory()()
    try:
        store = ScheduleStore(backend)
        if args.permanent:
            changed = await store.remove_schedule(args.name)
            action = "removed"
        else:
            changed = await store.disable_schedule(args.name)
            action = "disabled"

        if args.format == "json":
            sys.stdout.write(json.dumps({
                "removed": changed,
                "permanent": bool(args.permanent),
                "name": args.name,
            }) + "\n")
            return

        if changed:
            verb = "permanently removed" if args.permanent else "disabled"
            sys.stdout.write(f"{verb} schedule {args.name!r}\n")
        else:
            sys.stdout.write(
                f"schedule {args.name!r} not found — {action} was a no-op\n"
            )
    finally:
        await backend.close()


async def _run_schedule_status(args: argparse.Namespace) -> None:
    backend = await _get_backend_factory()()
    try:
        # Public API call — plan 2026-04-23-001 Unit 4 / KP-5. The
        # CLI no longer pokes at ``backend._client`` for the scheduler
        # lock; ``get_scheduler_leader`` is the contract. Connection
        # errors propagate so ``schedule status`` surfaces an outage
        # via exit code rather than silently printing "no leader".
        leader = await backend.get_scheduler_leader()

        store = ScheduleStore(backend)
        now_ms = int(time.time() * 1000)
        # Look ahead 24h for the "next N" view.
        horizon_ms = now_ms + 24 * 3600 * 1000
        upcoming = await store.poll_due(horizon_ms, limit=args.limit)
        upcoming.sort(key=lambda t: t[1])  # sort by score

        if args.format == "json":
            payload = {
                "leader": leader,
                "now_ms": now_ms,
                "upcoming": [
                    {
                        "name": name,
                        "fire_ts_ms": score,
                        "fire_at": datetime.fromtimestamp(
                            score / 1000, tz=UTC
                        ).isoformat(),
                        "task": defn.task_name,
                        "queue": defn.queue,
                        "disabled": defn.disabled,
                    }
                    for name, score, defn in upcoming
                ],
            }
            sys.stdout.write(json.dumps(payload, indent=2) + "\n")
            return

        # Table format
        sys.stdout.write(f"LEADER: {leader or '(none)'}\n")
        sys.stdout.write(f"NOW:    {datetime.fromtimestamp(now_ms/1000, tz=UTC).isoformat()}\n")
        sys.stdout.write("UPCOMING:\n")
        if not upcoming:
            sys.stdout.write("  (none in next 24h)\n")
        else:
            for name, score, defn in upcoming:
                fire_at = datetime.fromtimestamp(score / 1000, tz=UTC).isoformat()
                disabled = " [disabled]" if defn.disabled else ""
                sys.stdout.write(
                    f"  {fire_at}  {name}  task={defn.task_name}"
                    f"  queue={defn.queue}{disabled}\n"
                )
    finally:
        await backend.close()


async def _run_schedule_async(args: argparse.Namespace) -> None:
    """Dispatch a parsed 'swarmq schedule <subcommand>' invocation."""
    if args.schedule_command == "list":
        await _run_schedule_list(args)
    elif args.schedule_command == "add":
        await _run_schedule_add(args)
    elif args.schedule_command == "remove":
        await _run_schedule_remove(args)
    elif args.schedule_command == "status":
        await _run_schedule_status(args)
    else:  # pragma: no cover — argparse required=True guards this
        raise ConfigurationError(
            f"unknown schedule subcommand: {args.schedule_command!r}"
        )
