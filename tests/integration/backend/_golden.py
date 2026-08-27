"""Golden-Fixtures der Daten- und Wire-Kompatibilität 0.1.2 → 0.2.0 (KTD12).

Sechs Fixtures unter ``golden/`` halten prüfbar, was unverändert bleiben muss:
``keys.json`` (Valkey-Keys + Typen), ``values/`` (Result-Envelope mit allen
sieben msgpack-Ext-Typen, Workflow-Zustand, Schedule-Definition,
Progress-HASH), ``channels.txt`` (Pub/Sub-Kanäle + Payload),
``queue_args.json`` (RabbitMQ-Deklarationsargumente), ``headers.txt``
(``x-swarmq-*``-Header) und ``lua_contract.txt`` (KEYS/ARGV-Arität und
Slot-Gleichheit der Mehrschlüssel-Skripte).

Erzeugt mit festen IDs; die einzige uhrabhängige Stelle (Rate-Limit-Fenster
aus dem Server-``TIME``) wird auf ``<window>`` normalisiert. Regenerieren::

    SWARMQ_TEST_VALKEY_PORT=6380 uv run --extra test python -m tests.integration.backend._golden --write

Eine Abweichung ist ein benannter Bruch mit Migrationshinweis im Changelog (R15).
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import msgspec
from valkey.crc import key_slot

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.cancellation import build_result_dict
from swarmq.limits import RateLimitConfig, RateLimiter
from swarmq.locking import LockManager
from swarmq.scheduler import ScheduleDefinition
from swarmq.serialization import encode_json, encode_result
from swarmq.workflow import Signature, chain, chord, group
from swarmq.workflow_orchestration import ChordManager, build_initial_workflow_state

GOLDEN_DIR = Path(__file__).parent / "golden"
SRC_DIR = Path(__file__).resolve().parents[3] / "src" / "swarmq"

TASK_ID = "golden-task-0001"
WF_ID = "golden-wf-0001"
DEDUP_HASH = "0" * 64
SCHEDULE = "golden-sched"
FIRE_TS_MS = 1_700_000_000_000
TTL_MS = 600_000

PROGRESS_FIELDS = {
    "current": "3",
    "total": "10",
    "percent": "30.00",
    "message": "golden",
    "updated_at": "2026-01-02T03:04:05Z",
    "started_at": "2026-01-02T03:00:00Z",
    "eta_seconds": "7",
}


# ---------------------------------------------------------------------------
# values/
# ---------------------------------------------------------------------------


def golden_result_dict() -> dict[str, Any]:
    return build_result_dict(
        task_id=TASK_ID,
        queue="golden",
        task_name="golden.task",
        is_error=False,
        is_cancelled=False,
        result={
            "dt": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
            "d": date(2026, 1, 2),
            "t": time(3, 4, 5),
            "dec": Decimal("1.50"),
            "uid": UUID(int=1),
            "set": {1},
            "fset": frozenset({"a"}),
        },
    )


def golden_workflow_state() -> dict[str, Any]:
    topology = chain(
        Signature("golden.a", (1,)),
        chord(group(Signature("golden.b", (2,)), Signature("golden.c", kwargs={"k": "v"})), Signature("golden.d")),
    )
    return build_initial_workflow_state(WF_ID, topology.to_dict())


def golden_schedule_definition() -> ScheduleDefinition:
    return ScheduleDefinition(
        schedule_name=SCHEDULE,
        task_name="golden.task",
        args=(1,),
        kwargs={"k": "v"},
        queue="golden",
        cron="* * * * *",
        fire_ts_ms=FIRE_TS_MS,
        source="static",
    )


def value_fixtures() -> dict[str, bytes]:
    return {
        "result.msgpack": encode_result(golden_result_dict()),
        "workflow_state.json": encode_json(golden_workflow_state()),
        "schedule_definition.json": msgspec.json.encode(golden_schedule_definition()),
        "progress_hash.json": json.dumps(PROGRESS_FIELDS, sort_keys=True).encode(),
    }


# ---------------------------------------------------------------------------
# keys.json — gegen echtes Valkey
# ---------------------------------------------------------------------------

_RL_WINDOW = re.compile(r"^(sq:rl:\{[^}]+\}:)\d+$")


def _normalise_key(key: str) -> str:
    return _RL_WINDOW.sub(r"\1<window>", key)


async def snapshot_keys(backend: ValkeyBackend) -> dict[str, str]:
    """Schreibt jeden Datentyp mit festen IDs in ein geleertes Valkey und
    liefert ``{key: TYPE}``."""
    client = backend.client
    await client.flushdb()
    await backend.store_result(TASK_ID, value_fixtures()["result.msgpack"], TTL_MS)
    await backend.set_progress(TASK_ID, PROGRESS_FIELDS, TTL_MS)
    await backend.set_cancel_flag(TASK_ID, TTL_MS)
    await backend.store_workflow_state(WF_ID, value_fixtures()["workflow_state.json"], TTL_MS)
    await backend.set_workflow_cancel_flag(WF_ID, TTL_MS)
    await backend.claim_unique(DEDUP_HASH, TASK_ID, TTL_MS)
    await backend.hset_schedule_def(SCHEDULE, value_fixtures()["schedule_definition.json"])
    await backend.zadd_schedule(SCHEDULE, FIRE_TS_MS)
    await backend.set_schedfire_dedup(f"{SCHEDULE}:{FIRE_TS_MS}", TTL_MS)
    await backend.set_schedfire_inflight(f"{SCHEDULE}:{FIRE_TS_MS + 60_000}", TTL_MS)
    await backend.set_schedfire_handover_marker("golden-owner", TTL_MS)
    locks = LockManager(client)
    await locks.acquire("golden-lock", "golden-owner", TTL_MS, 1)
    await locks.acquire("{sched}:leader", "golden-owner", TTL_MS, 1)
    await locks.acquire("golden-sem", "golden-owner", TTL_MS, 2)
    await RateLimiter(client).check("golden-task", RateLimitConfig(max_requests=10, window_size_ms=60_000))
    await ChordManager(client).record_and_check_complete(WF_ID, "c1", "m1", 2, TTL_MS)

    keys: dict[str, str] = {}
    for raw in await client.keys("*"):
        key = raw.decode() if isinstance(raw, bytes) else str(raw)
        type_ = await client.type(key)
        keys[_normalise_key(key)] = type_.decode() if isinstance(type_, bytes) else str(type_)
    await client.flushdb()
    return dict(sorted(keys.items()))


# ---------------------------------------------------------------------------
# channels.txt, queue_args.json, headers.txt, lua_contract.txt — rein
# ---------------------------------------------------------------------------


def channels() -> str:
    backend = ValkeyBackend("valkey://golden")
    lines = [
        f"{backend.notify_channel_name(TASK_ID)}\tdone",
        f"{backend.progress_channel_name(TASK_ID)}\t<json der HASH-Felder>",
        f"{backend.workflow_notify_channel(WF_ID)}\tdone",
    ]
    return "\n".join(lines) + "\n"


async def queue_args() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for queue_type in ("quorum", "classic"):
        broker = RabbitMQBroker("amqp://golden", queue_type=queue_type)
        channel = AsyncMock()
        channel.is_closed = False
        broker._publish_channel = channel
        await broker.declare_queue("golden")
        out[queue_type] = {
            call.args[0]: {"durable": call.kwargs["durable"], "arguments": call.kwargs["arguments"]}
            for call in channel.declare_queue.await_args_list
        }
    out["delay"] = "per-message expiration (Sekunden) auf {queue}.DQ; kein x-message-ttl"
    return out


def header_names() -> str:
    found: set[str] = set()
    for path in SRC_DIR.rglob("*.py"):
        found.update(re.findall(r"x-swarmq-[a-z0-9-]+", path.read_text(encoding="utf-8")))
    return "\n".join(sorted(found)) + "\n"


def lua_contract() -> str:
    lines: list[str] = []
    for path in sorted((SRC_DIR / "lua").glob("*.lua")):
        text = path.read_text(encoding="utf-8")
        keys = max((int(n) for n in re.findall(r"KEYS\[(\d+)\]", text)), default=0)
        argv = max((int(n) for n in re.findall(r"ARGV\[(\d+)\]", text)), default=0)
        lines.append(f"{path.stem}\tKEYS={keys}\tARGV={argv}")
    multi = {
        "sched_commit": ["sq:{sched}:zset", "sq:{sched}:defs", "sq:lock:{sched}:leader"],
        "sched_disable": ["sq:{sched}:defs", "sq:{sched}:zset"],
        "sched_poll": ["sq:{sched}:zset", "sq:{sched}:defs"],
        "sliding_window_counter": ["sq:rl:{golden-task}:1700000000000", "sq:rl:{golden-task}:1699999940000"],
    }
    for script, keys_ in multi.items():
        slots = {key_slot(k.encode()) for k in keys_}
        lines.append(f"{script}\tslots={'equal' if len(slots) == 1 else 'DIFFERENT'}\t{' '.join(keys_)}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


async def write_all(backend: ValkeyBackend) -> None:
    (GOLDEN_DIR / "values").mkdir(parents=True, exist_ok=True)
    (GOLDEN_DIR / "keys.json").write_text(json.dumps(await snapshot_keys(backend), indent=2) + "\n")
    for name, data in value_fixtures().items():
        (GOLDEN_DIR / "values" / name).write_bytes(data)
    (GOLDEN_DIR / "channels.txt").write_text(channels())
    (GOLDEN_DIR / "queue_args.json").write_text(json.dumps(await queue_args(), indent=2, sort_keys=True) + "\n")
    (GOLDEN_DIR / "headers.txt").write_text(header_names())
    (GOLDEN_DIR / "lua_contract.txt").write_text(lua_contract())


async def _main() -> None:
    from tests.support.services import valkey_db_url

    backend = ValkeyBackend(valkey_db_url())
    await backend.connect()
    try:
        await write_all(backend)
    finally:
        await backend.close()
    print(f"golden fixtures written to {GOLDEN_DIR}")


if __name__ == "__main__":
    if "--write" not in sys.argv:
        sys.exit("usage: python -m tests.integration.backend._golden --write")
    asyncio.run(_main())
