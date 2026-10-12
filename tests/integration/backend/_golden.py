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
from aio_pika.exceptions import ChannelNotFoundEntity
from valkey.crc import key_slot

from swarmq.backend.valkey import ValkeyBackend
from swarmq.backend.valkey.control import ValkeySetpointStore
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.cancellation import build_error_result_dict, build_result_dict
from swarmq.contracts.control import Setpoint
from swarmq.queue_names import queue_family
from swarmq.scheduler import ScheduleDefinition
from swarmq.serialization import encode_json, encode_result
from swarmq.workflow import Signature, chain, chord, group
from swarmq.workflow_orchestration import build_initial_workflow_state

GOLDEN_DIR = Path(__file__).parent / "golden"
# Namensraum fuer ``keys.namespace.json`` (Plan 0.12.0, MR 15, BZ-10).
NAMESPACE = "golden-ns"
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


def golden_error_result_dict() -> dict[str, Any]:
    """Fehlerergebnis mit ``error_class`` (P9, Plan 2026-10-12-1105, MR 3)."""
    return build_error_result_dict(
        task_id=TASK_ID,
        queue="golden",
        task_name="golden.task",
        error_message="",
        error_class="builtins.ValueError",
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
        "error_result.msgpack": encode_result(golden_error_result_dict()),
        "workflow_state.json": encode_json(golden_workflow_state()),
        "schedule_definition.json": msgspec.json.encode(golden_schedule_definition()),
        "progress_hash.json": json.dumps(PROGRESS_FIELDS, sort_keys=True).encode(),
    }


# ---------------------------------------------------------------------------
# keys.json — gegen echtes Valkey
# ---------------------------------------------------------------------------

_RL_WINDOW = re.compile(r"^((?:[^:]+:)?sq:rl:\{[^}]+\}:)\d+$")


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
    await backend.schedules.put(SCHEDULE, value_fixtures()["schedule_definition.json"], FIRE_TS_MS)
    await backend.schedules.mark_fire_completed(f"{SCHEDULE}:{FIRE_TS_MS}", TTL_MS)
    await backend.schedules.claim_fire(f"{SCHEDULE}:{FIRE_TS_MS + 60_000}", TTL_MS)
    await backend.schedules.set_handover_marker("golden-owner", TTL_MS)
    await backend.schedules.report_cron(
        SCHEDULE, "0" * 16, value_fixtures()["schedule_definition.json"], live_window_ms=TTL_MS
    )
    await backend.schedules.report_worker_version("golden-worker", "0.10.0", ttl_ms=TTL_MS)
    # Gemeldete Queues (Plan 0.12.0, MR 18, BZ-14).
    await backend.schedules.report_queues(["golden"], ttl_ms=TTL_MS)
    # Cron-Aufraeumen (KTD8): ein deaktiviert entfernter Cron landet im Set.
    await backend.schedules.put("golden-pruned", value_fixtures()["schedule_definition.json"], None)
    await backend.schedules.prune_cron(
        "golden-pruned",
        value_fixtures()["schedule_definition.json"],
        stale_after_ms=None,
        remember_disabled=True,
    )
    await backend.schedules.set_leader_version("golden-owner", "0.10.0", ttl_ms=TTL_MS)
    # Epoch des Datenbestands (Plan 0.12.0, MR 14, BZ-04).
    await backend.schedules.ensure_epoch("golden-epoch")
    # Ausfuehrungsschutz (Plan 2026-10-08-1541, KTD5): Running-Marker und Zustand.
    await backend.execution_guard.claim(
        TASK_ID,
        owner="golden-owner",
        retry_count=0,
        ttl_ms=TTL_MS,
        state_ttl_ms=TTL_MS,
        max_orphans=3,
    )
    # Die Stores des Backends selbst (Review MR 15, Hinweis): mit Namensraum
    # belegt die Golden-Datei so auch die Verdrahtung von Locks, Rate-Limit
    # und Chords mit dem KeySpace des Backends (BZ-10).
    keys = backend._keys
    locks = backend.locks
    await locks.acquire("golden-lock", owner="golden-owner", ttl_ms=TTL_MS, max_concurrency=1)
    await locks.acquire("{sched}:leader", owner="golden-owner", ttl_ms=TTL_MS, max_concurrency=1)
    await locks.acquire("golden-sem", owner="golden-owner", ttl_ms=TTL_MS, max_concurrency=2)
    await backend.rate_limit.check("golden-task", max_requests=10, window_ms=60_000)
    await backend.chords.record_and_check_complete(WF_ID, "c1", "m1", 2, TTL_MS)
    # Kontrollkanal (Plan 0.12.0, MR 0): Sollwert und Rueckmeldung.
    control = ValkeySetpointStore(client=client, keys=keys)
    await control.write_setpoint("golden-group", Setpoint(low=1, high=2))
    await control.write_status("golden-group", "golden-sup", {"workers": 1}, ttl_s=TTL_MS / 1000)

    found: dict[str, str] = {}
    for raw in await client.keys("*"):
        key = raw.decode() if isinstance(raw, bytes) else str(raw)
        type_ = await client.type(key)
        found[_normalise_key(key)] = type_.decode() if isinstance(type_, bytes) else str(type_)
    await client.flushdb()
    return dict(sorted(found.items()))


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


def channels_namespace() -> str:
    """Wie ``channels``, mit Namensraum ``NAMESPACE`` (additiv, Q3)."""
    backend = ValkeyBackend("valkey://golden", namespace=NAMESPACE)
    lines = [
        f"{backend.notify_channel_name(TASK_ID)}\tdone",
        f"{backend.progress_channel_name(TASK_ID)}\t<json der HASH-Felder>",
        f"{backend.workflow_notify_channel(WF_ID)}\tdone",
    ]
    return "\n".join(lines) + "\n"


async def _declared_on_own_channels(queue_type: str, strategy: str, *, tiers: bool = False) -> dict[str, Any]:
    """Die Deklarationen einer neuen Queue-Familie ``golden``. Seit MR 8 laufen
    sie auf eigenen Wegwerf-Channels nach einem passiven Declare (hier
    NOT_FOUND: die Queue ist neu); erfasst wird je Queue die gelungene.

    Mit ``tiers`` (MR 10, Q3: getrennter Aufruf, eigener Schluessel) nur die
    Delay-Stufen, die der Start seit Review MR 10 (P1-2) nach der Familie
    anlegt; ohne ``tiers`` nur die Familie."""
    broker = RabbitMQBroker("amqp://golden", queue_type=queue_type)
    broker.dead_letter_strategy = strategy
    broker._publish_channel = AsyncMock(is_closed=False)
    declared: dict[str, Any] = {}

    async def open_channel(**_: Any) -> AsyncMock:
        channel = AsyncMock(is_closed=False)

        async def declare_queue(
            name: str, *, durable: bool = True, arguments: dict[str, Any] | None = None, passive: bool = False
        ) -> None:
            if passive:
                raise ChannelNotFoundEntity(f"NOT_FOUND - no queue '{name}' in vhost '/'")
            declared[name] = {"durable": durable, "arguments": arguments}

        channel.declare_queue = AsyncMock(side_effect=declare_queue)
        return channel

    broker._connection = AsyncMock(is_closed=False)
    broker._connection.channel = AsyncMock(side_effect=open_channel)
    await broker.declare_queue("golden")
    family = set(queue_family("golden"))
    return {name: value for name, value in declared.items() if (name not in family) == tiers}


async def queue_args() -> dict[str, Any]:
    # Q3 (MR 8): ``quorum`` und ``classic`` bleiben die Argumente aus 0.11
    # (Modus ``legacy``); die at-least-once-Neuanlage ist ein eigener Schlüssel.
    out: dict[str, Any] = {
        "quorum": await _declared_on_own_channels("quorum", "legacy"),
        "classic": await _declared_on_own_channels("classic", "legacy"),
        "quorum_at_least_once": await _declared_on_own_channels("quorum", "auto"),
    }
    out["delay_tiers"] = {
        "quorum": await _declared_on_own_channels("quorum", "legacy", tiers=True),
        "classic": await _declared_on_own_channels("classic", "legacy", tiers=True),
        "quorum_at_least_once": await _declared_on_own_channels("quorum", "auto", tiers=True),
    }
    out["delay"] = (
        "per-message expiration (Sekunden) auf der kleinsten Stufe {queue}.DQ.<1s|4s|15s|1m|4m|15m|1h> "
        ">= Wartezeit, ueber 1 h auf {queue}.DQ; kein x-message-ttl"
    )
    return out


def header_names() -> str:
    """``x-swarmq-*``-Literale im Code — Kommentare werden ignoriert (U7: ein
    Kommentar nannte den nie emittierten ``x-swarmq-cancel-deferred-count``)."""
    found: set[str] = set()
    for path in SRC_DIR.rglob("*.py"):
        for line in path.read_text(encoding="utf-8").splitlines():
            code = line.split("#", 1)[0]
            found.update(re.findall(r"x-swarmq-[a-z0-9-]+", code))
    return "\n".join(sorted(found)) + "\n"


def lua_contract() -> str:
    lines: list[str] = []
    for path in sorted((SRC_DIR / "lua").glob("*.lua")):
        text = path.read_text(encoding="utf-8")
        keys = max((int(n) for n in re.findall(r"KEYS\[(\d+)\]", text)), default=0)
        argv = max((int(n) for n in re.findall(r"ARGV\[(\d+)\]", text)), default=0)
        lines.append(f"{path.stem}\tKEYS={keys}\tARGV={argv}")
    multi = {
        "guard_claim": ["sq:guard:{golden-task-0001}:run", "sq:guard:{golden-task-0001}:state"],
        "guard_finish": ["sq:guard:{golden-task-0001}:run", "sq:guard:{golden-task-0001}:state"],
        "sched_commit": ["sq:{sched}:zset", "sq:{sched}:defs", "sq:lock:{sched}:leader"],
        "sched_disable": ["sq:{sched}:defs", "sq:{sched}:zset"],
        "sched_poll": ["sq:{sched}:zset", "sq:{sched}:defs"],
        "sched_replan": ["sq:{sched}:defs", "sq:{sched}:zset"],
        "sched_report": ["sq:{sched}:report:first_seen", "sq:{sched}:report:last_seen", "sq:{sched}:report:def"],
        "sched_prune": ["sq:{sched}:defs", "sq:{sched}:zset", "sq:{sched}:report:last_seen", "sq:{sched}:disabled"],
        "sched_report_prune": ["sq:{sched}:report:first_seen", "sq:{sched}:report:last_seen", "sq:{sched}:report:def"],
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
    namespaced = ValkeyBackend(client=backend.client, namespace=NAMESPACE)
    (GOLDEN_DIR / "keys.namespace.json").write_text(json.dumps(await snapshot_keys(namespaced), indent=2) + "\n")
    for name, data in value_fixtures().items():
        (GOLDEN_DIR / "values" / name).write_bytes(data)
    (GOLDEN_DIR / "channels.txt").write_text(channels())
    (GOLDEN_DIR / "channels.namespace.txt").write_text(channels_namespace())
    (GOLDEN_DIR / "queue_args.json").write_text(json.dumps(await queue_args(), indent=2, sort_keys=True) + "\n")
    (GOLDEN_DIR / "headers.txt").write_text(header_names())
    (GOLDEN_DIR / "lua_contract.txt").write_text(lua_contract())


async def _main() -> None:
    from tests.support.services import require_safe_valkey_url, valkey_db_url

    # snapshot_keys flushes the database: refuse a foreign stack on 6379.
    require_safe_valkey_url(valkey_db_url())
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
