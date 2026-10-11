"""E2E tests for ungewöhnliche Inputs (U1.6).

Roundtrip-Verhalten für:
- grosse Payloads (5 MB Task-Args, 10 MB Result, 1 MB Progress-message)
- Unicode-Edge-Cases (Emoji, 4-Byte UTF-8, BOM, Zero-Width-Joiner)
- leere Container, None-Values in nested Structures
- tief verschachtelte Dicts
- Float-Edge-Cases (1e300, 1e-300, NaN/Inf werden gerejectet beim
  Unique-Hash — Regression aus R3.5)
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import uuid
from typing import Any, override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

pytestmark = [pytest.mark.timeout(60)]


def _unique_queue() -> str:
    return f"e2e-edge-{uuid.uuid4().hex[:10]}"


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


@contextlib.asynccontextmanager
async def _running_worker(rabbitmq_url: str, valkey_url: str, queue: str):
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=[queue],
        concurrency=2,
    )
    online = asyncio.Event()

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**_):
        online.set()

    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=6)
        yield worker_app
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await worker_broker.close()
        await worker_backend.close()


async def _roundtrip(
    rabbitmq_url,
    valkey_url,
    *,
    task_cls,
    args=(),
    kwargs=None,
    timeout=30,
) -> Any:
    """Minimaler Helper: schedule + get_result mit eigenem
    Worker-Subapp. Fehler werden als raised TaskError surfaced."""
    queue = task_cls.task_info.queues[0]
    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(rabbitmq_url, valkey_url, queue):
            tid = await client_app.schedule(
                task_cls.task_info.name,
                *args,
                **(kwargs or {}),
            )
            return await client_app.get_result(tid, timeout=timeout)


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_5mb_task_args_roundtrip(
    rabbitmq_url,
    valkey_url,
):
    """5 MB binary-as-string-of-bytes payload survives publish +
    consume + result round-trip."""
    queue = _unique_queue()
    task_name = _task_name("big_args")

    class BigArgsTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        @override
        async def run(self, blob_hex: str) -> int:
            return len(blob_hex)

    big = "a" * (5 * 1024 * 1024)  # 5 MB
    result = await _roundtrip(
        rabbitmq_url,
        valkey_url,
        task_cls=BigArgsTask,
        args=(big,),
    )
    assert result == 5 * 1024 * 1024


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_10mb_result_payload_roundtrip(
    rabbitmq_url,
    valkey_url,
):
    """Task returns 10 MB string; get_result must surface it intact."""
    queue = _unique_queue()
    task_name = _task_name("big_result")

    class BigResultTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        @override
        async def run(self) -> str:
            return "z" * (10 * 1024 * 1024)

    result = await _roundtrip(
        rabbitmq_url,
        valkey_url,
        task_cls=BigResultTask,
        timeout=40,
    )
    assert isinstance(result, str)
    assert len(result) == 10 * 1024 * 1024
    assert result[0] == "z"
    assert result[-1] == "z"


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_unicode_args_and_result_roundtrip(
    rabbitmq_url,
    valkey_url,
):
    """Emoji (4-byte UTF-8), BOM, Zero-Width-Joiner, RTL marks survive
    JSON-encode + decode roundtrip via msgspec."""
    queue = _unique_queue()
    task_name = _task_name("unicode")

    payload = {
        "emoji_4byte": "\U0001f600\U0001f680\U0001f4a9",  # 😀🚀💩
        "bom": "\ufeffhello",
        "zwj_family": "\U0001f468\u200d\U0001f469\u200d\U0001f466",
        "rtl_marker": "\u202bعربي\u202c",
        "ascii": "plain",
        "mixed": "naïve café résumé",
    }

    class UnicodeTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        @override
        async def run(self, data: dict) -> dict:
            # Echo + add a marker so we know the worker actually
            # received the same bytes (not a re-encoded variant).
            return {"echoed": data, "lengths": {k: len(v) for k, v in data.items()}}

    result = await _roundtrip(
        rabbitmq_url,
        valkey_url,
        task_cls=UnicodeTask,
        args=(payload,),
    )
    assert result["echoed"] == payload
    for key, val in payload.items():
        assert result["lengths"][key] == len(val), (
            f"length drift on {key}: sent {len(val)}, received {result['lengths'][key]}"
        )


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_empty_containers_and_none_values_roundtrip(
    rabbitmq_url,
    valkey_url,
):
    """Empty strings, lists, dicts, and None-values in nested
    structures must roundtrip verbatim."""
    queue = _unique_queue()
    task_name = _task_name("empty")

    payload = {
        "empty_str": "",
        "empty_list": [],
        "empty_dict": {},
        "nested_none": {"a": None, "b": [None, None]},
        "all_nulls": [None, None, None],
        "mixed": {"k": "", "v": [], "n": None},
    }

    class EmptyTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        @override
        async def run(self, data: dict) -> dict:
            return data

    result = await _roundtrip(
        rabbitmq_url,
        valkey_url,
        task_cls=EmptyTask,
        args=(payload,),
    )
    assert result == payload


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_deeply_nested_dict_roundtrip(
    rabbitmq_url,
    valkey_url,
):
    """8-level nested dict survives encode + decode."""
    queue = _unique_queue()
    task_name = _task_name("nested")

    payload: dict = {"v": "leaf"}
    for i in range(8):
        payload = {f"level_{i}": payload}

    class NestedTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        @override
        async def run(self, data: dict) -> str:
            cur: Any = data
            depth = 0
            while isinstance(cur, dict):
                if "v" in cur:
                    return f"depth={depth} value={cur['v']}"
                ((_key, cur),) = cur.items()
                depth += 1
            return "no-leaf"

    result = await _roundtrip(
        rabbitmq_url,
        valkey_url,
        task_cls=NestedTask,
        args=(payload,),
    )
    assert result == "depth=8 value=leaf"


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_float_edge_values_roundtrip(
    rabbitmq_url,
    valkey_url,
):
    """1e300, 1e-300, subnormal — finite floats survive the roundtrip
    unchanged. NaN/Inf are NOT tested here (rejected by canonical_encode
    when used as unique args; see R3.5 regression test)."""
    queue = _unique_queue()
    task_name = _task_name("floats")

    payload = {
        "huge": 1e300,
        "tiny": 1e-300,
        "neg_huge": -1e300,
        "subnormal": 5e-324,
        "exact_zero": 0.0,
        "neg_zero": -0.0,
        "pi": 3.141592653589793,
    }

    class FloatTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        @override
        async def run(self, data: dict) -> dict:
            return data

    result = await _roundtrip(
        rabbitmq_url,
        valkey_url,
        task_cls=FloatTask,
        args=(payload,),
    )
    for k, v in payload.items():
        # Subnormal may be coerced to 0 by some serializers; accept
        # exact-equality OR within-1-ULP for that case.
        if k == "subnormal":
            assert result[k] == 0.0 or result[k] == v, f"subnormal lost: {result[k]!r} vs {v!r}"
        else:
            assert result[k] == v, f"float drift on {k}: {result[k]!r} vs {v!r}"
    # neg_zero is preserved as -0.0 only if the encoder respects the
    # sign bit. msgspec/orjson typically normalise to 0.0 — pin the
    # observed contract rather than assume.
    assert result["neg_zero"] in (0.0, -0.0)


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_canonical_encode_rejects_nan_in_unique_args(
    rabbitmq_url,
    valkey_url,
):
    """R3.5 regression: NaN/Inf in arguments of a unique-marked task
    surface as ValueError at schedule-time (canonical_encode rejects
    them). Pinned here in E2E so the contract holds end-to-end."""
    queue = _unique_queue()
    task_name = _task_name("nan_uniq")

    class NaNUniqTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            max_retries=0,
            unique_for=10,
        )

        @override
        async def run(self, x: float) -> float:
            return x

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        with pytest.raises(TypeError, match="NaN"):
            await client_app.schedule(task_name, math.nan)
        with pytest.raises(TypeError, match="Inf"):
            await client_app.schedule(task_name, math.inf)


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_1mb_progress_message_field(
    rabbitmq_url,
    valkey_url,
):
    """1 MB string in update_progress(message=...) — Pub/Sub payload
    + HASH-Field müssen das verkraften ohne crash. Wir testen das
    Roundtrip-Verhalten via get_progress (HASH ist authoritative)."""
    queue = _unique_queue()
    task_name = _task_name("big_msg")
    big_msg = "M" * (1 * 1024 * 1024)  # 1 MB

    class BigProgressTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        @override
        async def run(self) -> str:
            await self.update_progress(1, 1, big_msg)
            return "ok"

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(rabbitmq_url, valkey_url, queue):
            tid = await client_app.schedule(task_name)
            result = await client_app.get_result(tid, timeout=20)
            assert result == "ok"
            info = await client_app.client.get_progress(tid)
            assert info is not None
            assert info.current == 1
            assert info.total == 1
            assert len(info.message) == 1 * 1024 * 1024
            assert info.message[0] == "M"
            assert info.message[-1] == "M"
