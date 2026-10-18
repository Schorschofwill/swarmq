"""``swarmq dlq *`` subcommand handlers.

Operates on RabbitMQ dead-letter queues (``{queue}.XQ``). Each handler
is broker-only (no backend) and uses the shared ``_broker_from_env``
factory. The ``_DLQNotFound`` sentinel lets ``main()`` map targeted-
lookup misses to exit code 3 without spraying extra stderr noise.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from swarmq.broker.rabbitmq import RabbitMQBroker

from swarmq.cli._shared import _render_table
from swarmq.exceptions import ConfigurationError
from swarmq.headers import HEADER_RETRY_COUNT

# ``_broker_from_env`` is resolved LAZILY via ``swarmq.cli`` so that
# ``monkeypatch.setattr(cli, "_broker_from_env", fake)`` in tests/unit/
# test_cli.py:767 intercepts the call. Direct
# ``from swarmq.cli._shared import _broker_from_env`` would bind the
# name into this module's namespace and silently bypass the patch.


def _get_broker_factory() -> Callable[[], Coroutine[Any, Any, RabbitMQBroker]]:
    """Lazy import of ``_broker_from_env`` (aus ``_shared``; patchbar dort)."""
    from swarmq.cli._shared import _broker_from_env

    return _broker_from_env


def _format_dlq_list_table(entries: list[dict[str, Any]]) -> str:
    """One row per DLQ entry — short header summary columns."""
    rows = [("TASK_ID", "TASK_NAME", "RETRIES", "ERROR_PREVIEW", "BODY_BYTES")]
    for e in entries:
        headers: dict[str, Any] = e.get("headers") or {}
        retries = headers.get(HEADER_RETRY_COUNT, "-")
        traceback_hdr = headers.get("x-swarmq-traceback", "")
        if isinstance(traceback_hdr, bytes):
            traceback_hdr = traceback_hdr.decode("utf-8", errors="replace")
        preview = (traceback_hdr.splitlines()[0] if traceback_hdr else "")[:60]
        rows.append(
            (
                e.get("task_id") or "(malformed)",
                e.get("task_name") or "-",
                str(retries),
                preview or "-",
                str(e["body_size"]),
            )
        )
    return _render_table(rows, "(DLQ empty)")


def _normalise_headers_for_json(
    headers: dict[str, Any],
) -> dict[str, Any]:
    """Decode bytes-valued headers for JSON serialisability.

    aio_pika sometimes surfaces header values as bytes when they come
    off the wire. json.dumps refuses bytes; decode defensively so
    agent/script consumers of --format json get clean strings.
    """
    out: dict[str, Any] = {}
    for k, v in (headers or {}).items():
        if isinstance(v, bytes):
            out[k] = v.decode("utf-8", errors="replace")
        else:
            out[k] = v
    return out


# R3.10 review P3 #19: targeted lookups (dlq inspect / dlq retry <id>)
# that find no match should exit with a distinct code so agent scripts
# can branch on it without parsing JSON. 3 is traditionally "not found".
_DLQ_NOT_FOUND_EXIT_CODE = 3


def _hit_limit(result: tuple[Any, int], limit: int) -> bool:
    """Ob das Lesen am Limit endete (F4-05, E-028): aus ``hit_limit`` des
    Ergebnisses (``DeadLetterScan``), sonst Rückfall auf ``scanned >= limit``
    für Implementierungen, die nur das Tupel liefern."""
    return bool(getattr(result, "hit_limit", result[1] >= limit))


class _DLQNotFound(Exception):
    """Sentinel for not-found lookups so main() can exit 3 cleanly.

    Carries no message — the handler writes the user-facing line to
    stdout BEFORE raising. main() catches this and maps it to exit 3
    without any additional stderr noise.
    """


async def _run_dlq_list(args: argparse.Namespace) -> None:
    broker = await _get_broker_factory()()
    try:
        listed = await broker.list_dead_letters(
            args.queue,
            limit=args.limit,
        )
        entries, scanned = listed
        truncated = _hit_limit(listed, args.limit)
        if args.format == "json":
            for e in entries:
                e["headers"] = _normalise_headers_for_json(e.get("headers") or {})
            # Wrap in an envelope so agents can detect truncation
            # without reasoning about exact-count equality (R3.10 P3 #18).
            envelope = {
                "entries": entries,
                "scanned": scanned,
                "limit": args.limit,
                "truncated": truncated,
                "queue": args.queue,
            }
            sys.stdout.write(json.dumps(envelope, indent=2, default=str) + "\n")
        else:
            sys.stdout.write(_format_dlq_list_table(entries) + "\n")
            if truncated:
                sys.stdout.write(f"(truncated at --limit {args.limit}; raise --limit to see more)\n")
    finally:
        await broker.close()


async def _run_dlq_inspect(args: argparse.Namespace) -> None:
    broker = await _get_broker_factory()()
    try:
        inspected = await broker.inspect_dead_letter(
            args.queue,
            args.task_id,
            scan_limit=args.scan_limit,
        )
        result, scanned = inspected
        if result is None:
            scan_exhausted = _hit_limit(inspected, args.scan_limit)
            if args.format == "json":
                sys.stdout.write(
                    json.dumps(
                        {
                            "found": False,
                            "task_id": args.task_id,
                            "queue": args.queue,
                            "scanned": scanned,
                            "scan_exhausted": scan_exhausted,
                        }
                    )
                    + "\n"
                )
            else:
                hint = " — scan hit --scan-limit; raise it to keep looking" if scan_exhausted else ""
                sys.stdout.write(
                    f"task {args.task_id!r} not found in {args.queue}.XQ (scanned {scanned} messages){hint}\n"
                )
            raise _DLQNotFound

        result["headers"] = _normalise_headers_for_json(result.get("headers") or {})

        if args.format == "json":
            # Add explicit found:true for symmetry with the miss shape
            # (R3.10 P3 #18 — agent can do kwargs.get("found") uniformly).
            payload = {"found": True, "scanned": scanned, **result}
            sys.stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
            return

        sys.stdout.write(f"TASK_ID:   {result['task_id']}\n")
        sys.stdout.write(f"QUEUE:     {result['queue']}.XQ\n")
        sys.stdout.write(f"BODY_SIZE: {result['body_size']} bytes\n")
        sys.stdout.write("HEADERS:\n")
        if not result["headers"]:
            sys.stdout.write("  (none)\n")
        else:
            for k, v in result["headers"].items():
                sys.stdout.write(f"  {k}: {v}\n")
        sys.stdout.write("BODY:\n")
        if result["body"] is not None:
            sys.stdout.write(json.dumps(result["body"], indent=2, default=str) + "\n")
        else:
            sys.stdout.write(result["body_raw"] + "\n")
    finally:
        await broker.close()


async def _run_dlq_retry(args: argparse.Namespace) -> None:
    # Validate task_id / --all pairing. argparse can't cleanly model
    # 'positional XOR --all' when the positional is optional, so we
    # enforce it here and exit via parser.error()-equivalent (code 2),
    # matching the exit-code contract for argument-shape errors
    # (R3.10 P3 #12).
    if args.all and args.task_id:
        sys.stderr.write("swarmq dlq retry: pass either a task_id positional argument OR --all, not both\n")
        raise SystemExit(2)
    if not args.all and not args.task_id:
        sys.stderr.write("swarmq dlq retry: requires either a task_id argument or --all\n")
        raise SystemExit(2)

    broker = await _get_broker_factory()()
    try:
        if args.all:
            moved_all = await broker.retry_all_dead_letters(
                args.queue,
                limit=args.limit,
            )
            moved, scanned = moved_all
            skipped = scanned - moved
            if args.format == "json":
                sys.stdout.write(
                    json.dumps(
                        {
                            "moved": moved,
                            "scanned": scanned,
                            "skipped_malformed": skipped,
                            "truncated": _hit_limit(moved_all, args.limit),
                            "queue": args.queue,
                            "all": True,
                        }
                    )
                    + "\n"
                )
            else:
                suffix = (
                    f" ({skipped} malformed message(s) left in DLQ — use 'dlq purge' to remove)" if skipped > 0 else ""
                )
                sys.stdout.write(f"republished {moved} message(s) from {args.queue}.XQ to {args.queue}{suffix}\n")
            return

        retried = await broker.retry_dead_letter(
            args.queue,
            args.task_id,
            scan_limit=args.scan_limit,
        )
        found, scanned = retried
        scan_exhausted = _hit_limit(retried, args.scan_limit)
        if args.format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "moved": 1 if found else 0,
                        "queue": args.queue,
                        "task_id": args.task_id,
                        "found": found,
                        "scanned": scanned,
                        "scan_exhausted": scan_exhausted,
                    }
                )
                + "\n"
            )
        elif found:
            sys.stdout.write(f"republished task {args.task_id!r} from {args.queue}.XQ to {args.queue}\n")
        else:
            hint = " — scan hit --scan-limit; raise it to keep looking" if scan_exhausted else ""
            sys.stdout.write(f"task {args.task_id!r} not found in {args.queue}.XQ (scanned {scanned} messages){hint}\n")
        if not found:
            raise _DLQNotFound
    finally:
        await broker.close()


async def _run_dlq_purge(args: argparse.Namespace) -> None:
    broker = await _get_broker_factory()()
    try:
        purged = await broker.purge_dead_letters(args.queue)
        if args.format == "json":
            sys.stdout.write(
                json.dumps(
                    {
                        "purged": purged,
                        "queue": args.queue,
                    }
                )
                + "\n"
            )
        else:
            sys.stdout.write(f"purged {purged} message(s) from {args.queue}.XQ\n")
    finally:
        await broker.close()


async def _run_dlq_async(args: argparse.Namespace) -> None:
    """Dispatch a parsed 'swarmq dlq <subcommand>' invocation."""
    if args.dlq_command == "list":
        await _run_dlq_list(args)
    elif args.dlq_command == "inspect":
        await _run_dlq_inspect(args)
    elif args.dlq_command == "retry":
        await _run_dlq_retry(args)
    elif args.dlq_command == "purge":
        await _run_dlq_purge(args)
    else:  # pragma: no cover — argparse required=True guards this
        raise ConfigurationError(f"unknown dlq subcommand: {args.dlq_command!r}")
