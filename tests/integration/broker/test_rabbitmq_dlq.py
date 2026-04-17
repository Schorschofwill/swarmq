"""Integration tests for R3.10 DLQ-primitives on RabbitMQBroker.

Test-first per plan: nails down the drain-and-republish contract
before the implementation. Every test declares a unique queue name so
concurrent test runs don't collide.
"""

from __future__ import annotations

import json
import uuid

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker

pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]


def _unique(prefix: str = "dlq") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _make_body(task_id: str, task_name: str = "t", args=None, kwargs=None) -> bytes:
    """Build a SwarmQ-shaped message body for DLQ population."""
    payload = {
        "task": {"id": task_id, "queue": "q", "name": task_name},
        "args": list(args or []),
        "kwargs": kwargs or {},
    }
    return json.dumps(payload).encode("utf-8")


@pytest.fixture
async def broker_and_queue(rabbitmq_url: str):
    """Declare a fresh main+DQ+XQ triplet; yield (broker, queue_name).

    Teardown deletes all three queues so tests don't leak state.
    """
    b = RabbitMQBroker(rabbitmq_url)
    await b.connect()
    queue_name = _unique()
    await b.declare_queue(queue_name)
    yield b, queue_name
    # Teardown — delete queues, close broker.
    if b._connection is not None and not b._connection.is_closed:
        for qname in (queue_name, f"{queue_name}.DQ", f"{queue_name}.XQ"):
            try:
                ch = await b._connection.channel()
                q = await ch.get_queue(qname, ensure=False)
                await q.delete(if_empty=False, if_unused=False)
                await ch.close()
            except Exception:  # noqa: BLE001
                pass
        await b.close()


async def _dlq_depth(broker: RabbitMQBroker, queue_name: str) -> int:
    """Return current message count in {queue}.XQ via declaration reply."""
    ch = await broker._connection.channel()
    try:
        q = await ch.declare_queue(f"{queue_name}.XQ", passive=True)
        return q.declaration_result.message_count
    finally:
        await ch.close()


async def _main_depth(broker: RabbitMQBroker, queue_name: str) -> int:
    """Return current message count in the main queue."""
    ch = await broker._connection.channel()
    try:
        q = await ch.declare_queue(queue_name, passive=True)
        return q.declaration_result.message_count
    finally:
        await ch.close()


# ---------------------------------------------------------------------------
# list_dead_letter_queue
# ---------------------------------------------------------------------------


async def test_list_empty_returns_empty(broker_and_queue):
    broker, qname = broker_and_queue
    entries, scanned = await broker.list_dead_letter_queue(qname)
    assert entries == []
    assert scanned == 0


async def test_list_returns_each_message_exactly_once(broker_and_queue):
    broker, qname = broker_and_queue
    ids = [f"t-{i}" for i in range(3)]
    for tid in ids:
        await broker.publish_to_dead_letter_queue(
            qname, _make_body(tid, "add"),
            headers={"x-swarmq-retry-count": 3},
        )

    entries, scanned = await broker.list_dead_letter_queue(qname)
    seen_ids = [e["task_id"] for e in entries]
    assert sorted(seen_ids) == sorted(ids), (
        f"list must return every published task exactly once; got {seen_ids}"
    )
    assert scanned == 3
    # And the DLQ retains all three (non-destructive).
    assert await _dlq_depth(broker, qname) == 3


async def test_list_respects_limit(broker_and_queue):
    broker, qname = broker_and_queue
    for i in range(5):
        await broker.publish_to_dead_letter_queue(
            qname, _make_body(f"t-{i}"),
        )
    entries, scanned = await broker.list_dead_letter_queue(qname, limit=2)
    assert len(entries) == 2
    assert scanned == 2
    # DLQ still has all 5 after list (scan-only).
    assert await _dlq_depth(broker, qname) == 5


async def test_list_includes_headers_and_body_size(broker_and_queue):
    broker, qname = broker_and_queue
    body = _make_body("t-1", "add")
    await broker.publish_to_dead_letter_queue(
        qname, body,
        headers={"x-swarmq-retry-count": 5, "x-swarmq-traceback": "boom"},
    )
    entries, scanned = await broker.list_dead_letter_queue(qname)
    assert len(entries) == 1
    assert scanned == 1
    e = entries[0]
    assert e["task_id"] == "t-1"
    assert e["task_name"] == "add"
    assert e["queue"] == qname
    assert e["body_size"] == len(body)
    assert e["headers"]["x-swarmq-retry-count"] == 5
    assert e["headers"]["x-swarmq-traceback"] == "boom"


async def test_list_surfaces_malformed_body_as_task_id_none(broker_and_queue):
    broker, qname = broker_and_queue
    await broker.publish_to_dead_letter_queue(qname, b"not-valid-json")
    entries, scanned = await broker.list_dead_letter_queue(qname)
    assert len(entries) == 1
    assert scanned == 1
    assert entries[0]["task_id"] is None
    assert entries[0]["task_name"] is None


# ---------------------------------------------------------------------------
# inspect_dead_letter_message
# ---------------------------------------------------------------------------


async def test_inspect_returns_full_payload_on_match(broker_and_queue):
    broker, qname = broker_and_queue
    body = _make_body("wanted", "mul", args=[2, 3], kwargs={"x": 1})
    await broker.publish_to_dead_letter_queue(
        qname, _make_body("other"),
    )
    await broker.publish_to_dead_letter_queue(
        qname, body,
        headers={"x-swarmq-retry-count": 2, "x-swarmq-traceback": "trace"},
    )

    result, scanned = await broker.inspect_dead_letter_message(qname, "wanted")
    assert result is not None
    assert result["task_id"] == "wanted"
    assert result["queue"] == qname
    assert result["body"]["task"]["name"] == "mul"
    assert result["body"]["args"] == [2, 3]
    assert result["body"]["kwargs"] == {"x": 1}
    assert result["headers"]["x-swarmq-retry-count"] == 2
    # scanned <= 2 depending on break_after_first_match ordering.
    assert 1 <= scanned <= 2
    # DLQ retains both messages.
    assert await _dlq_depth(broker, qname) == 2


async def test_inspect_missing_task_returns_none(broker_and_queue):
    broker, qname = broker_and_queue
    await broker.publish_to_dead_letter_queue(qname, _make_body("other"))
    result, scanned = await broker.inspect_dead_letter_message(qname, "missing")
    assert result is None
    assert scanned == 1
    assert await _dlq_depth(broker, qname) == 1


async def test_inspect_empty_queue_returns_none(broker_and_queue):
    broker, qname = broker_and_queue
    result, scanned = await broker.inspect_dead_letter_message(qname, "anything")
    assert result is None
    assert scanned == 0


# ---------------------------------------------------------------------------
# retry_dead_letter_message
# ---------------------------------------------------------------------------


async def test_retry_moves_matching_message_to_main(broker_and_queue):
    broker, qname = broker_and_queue
    await broker.publish_to_dead_letter_queue(qname, _make_body("keep"))
    await broker.publish_to_dead_letter_queue(
        qname, _make_body("move"),
        headers={"x-swarmq-retry-count": 3, "x-swarmq-traceback": "err"},
    )

    moved, scanned = await broker.retry_dead_letter_message(qname, "move")
    assert moved is True
    # DLQ shrunk by one; main gained one.
    assert await _dlq_depth(broker, qname) == 1
    assert await _main_depth(broker, qname) == 1


async def test_retry_missing_returns_false_and_leaves_dlq_intact(broker_and_queue):
    broker, qname = broker_and_queue
    await broker.publish_to_dead_letter_queue(qname, _make_body("a"))
    await broker.publish_to_dead_letter_queue(qname, _make_body("b"))

    moved, scanned = await broker.retry_dead_letter_message(qname, "missing")
    assert moved is False
    assert scanned == 2
    assert await _dlq_depth(broker, qname) == 2
    assert await _main_depth(broker, qname) == 0


async def test_retry_resets_retry_count_header(broker_and_queue):
    """Retry republishes with x-swarmq-retry-count stripped so the task
    gets the full retry budget on the redelivery."""
    broker, qname = broker_and_queue
    await broker.publish_to_dead_letter_queue(
        qname, _make_body("t-retry"),
        headers={"x-swarmq-retry-count": 5, "x-swarmq-traceback": "boom"},
    )

    moved, _scanned = await broker.retry_dead_letter_message(qname, "t-retry")
    assert moved is True

    # Consume from the main queue to verify the forwarded headers.
    ch = await broker._connection.channel()
    try:
        q = await ch.get_queue(qname)
        msg = await q.get(no_ack=True)
        assert msg is not None
        assert "x-swarmq-retry-count" not in (msg.headers or {}), (
            "retry-count must be reset on forward to main queue"
        )
        # Traceback header DOES survive (first-fail context visible).
        assert (msg.headers or {}).get("x-swarmq-traceback") == "boom"
    finally:
        await ch.close()


# ---------------------------------------------------------------------------
# retry_all_dead_letter_messages
# ---------------------------------------------------------------------------


async def test_retry_all_moves_everything(broker_and_queue):
    broker, qname = broker_and_queue
    for i in range(4):
        await broker.publish_to_dead_letter_queue(
            qname, _make_body(f"t-{i}"),
            headers={"x-swarmq-retry-count": 3},
        )

    moved, scanned = await broker.retry_all_dead_letter_messages(qname)
    assert moved == 4
    assert scanned == 4
    assert await _dlq_depth(broker, qname) == 0
    assert await _main_depth(broker, qname) == 4


async def test_retry_all_empty_returns_zero(broker_and_queue):
    broker, qname = broker_and_queue
    moved, scanned = await broker.retry_all_dead_letter_messages(qname)
    assert moved == 0
    assert scanned == 0


async def test_retry_all_respects_limit(broker_and_queue):
    broker, qname = broker_and_queue
    for i in range(5):
        await broker.publish_to_dead_letter_queue(qname, _make_body(f"t-{i}"))
    moved, scanned = await broker.retry_all_dead_letter_messages(qname, limit=3)
    assert moved == 3
    assert scanned == 3
    assert await _dlq_depth(broker, qname) == 2
    assert await _main_depth(broker, qname) == 3


async def test_retry_all_skips_malformed_bodies(broker_and_queue):
    """Review P2 #6 / ADV-003: retry_all must not forward unparseable
    bodies to main (worker would just dead-letter them again)."""
    broker, qname = broker_and_queue
    await broker.publish_to_dead_letter_queue(qname, _make_body("valid-1"))
    await broker.publish_to_dead_letter_queue(qname, b"not-json")
    await broker.publish_to_dead_letter_queue(qname, _make_body("valid-2"))

    moved, scanned = await broker.retry_all_dead_letter_messages(qname)
    assert moved == 2, "only the two valid bodies should reach main"
    assert scanned == 3
    # Malformed body stays in DLQ for operator purge.
    assert await _dlq_depth(broker, qname) == 1
    assert await _main_depth(broker, qname) == 2


async def test_retry_single_only_moves_first_match(broker_and_queue):
    """Review P2 #3 / C-2: docstring promises 'first matching'. Enforce
    it — duplicate task_ids in the DLQ yield exactly one move per call."""
    broker, qname = broker_and_queue
    # Two messages with the same task_id.
    for _ in range(2):
        await broker.publish_to_dead_letter_queue(qname, _make_body("dup"))

    moved, _scanned = await broker.retry_dead_letter_message(qname, "dup")
    assert moved is True
    # Only ONE was moved; second copy remains in the DLQ.
    assert await _main_depth(broker, qname) == 1
    assert await _dlq_depth(broker, qname) == 1


async def test_inspect_scan_limit_truncates_without_spurious_miss(
    broker_and_queue,
):
    """Review P3 #21 / T-04: bounded-scan correctness. If the target is
    at position scan_limit+1, inspect must return None + scanned_count
    equal to scan_limit."""
    broker, qname = broker_and_queue
    await broker.publish_to_dead_letter_queue(qname, _make_body("a"))
    await broker.publish_to_dead_letter_queue(qname, _make_body("b"))
    await broker.publish_to_dead_letter_queue(qname, _make_body("target"))

    # scan_limit=2 means target (at position 3) is unreachable.
    result, scanned = await broker.inspect_dead_letter_message(
        qname, "target", scan_limit=2,
    )
    assert result is None
    assert scanned == 2


# ---------------------------------------------------------------------------
# purge_dead_letter_queue
# ---------------------------------------------------------------------------


async def test_purge_removes_all_messages(broker_and_queue):
    broker, qname = broker_and_queue
    for i in range(3):
        await broker.publish_to_dead_letter_queue(qname, _make_body(f"t-{i}"))
    purged = await broker.purge_dead_letter_queue(qname)
    assert purged == 3
    assert await _dlq_depth(broker, qname) == 0


async def test_purge_empty_returns_zero(broker_and_queue):
    broker, qname = broker_and_queue
    assert await broker.purge_dead_letter_queue(qname) == 0
