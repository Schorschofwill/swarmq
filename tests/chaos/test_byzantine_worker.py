"""F8 — Byzantine-worker scenarios: schema evolution + hash divergence.

Plan: docs/plans/2026-04-17-007-feat-e2e-hardening-plan.md U2.8.

Three scenarios:

1. ``test_old_schema_writer_new_schema_reader`` — an "old" worker writes
   a msgpack result blob that is missing fields the current ``Client.
   _unpack`` knows about (``is_cancelled`` was added in Phase 3b.3).
   ``_unpack`` reads via ``.get(..., default)`` so the blob still
   decodes. We pin that contract here so a future refactor to, say,
   ``msgspec.Struct`` with required fields would break the test loudly.

2. ``test_new_schema_writer_old_schema_reader`` — a "new" worker writes
   a blob with extra top-level fields that the current decoder does not
   know. Because msgpack decodes into an untyped ``dict`` and ``_unpack``
   only touches the keys it cares about, the blob decodes cleanly and
   the extra fields are silently ignored. Pin the forward-compat
   invariant.

3. ``test_divergent_unique_hash_breaks_dedup_documented`` — two
   conceptually-equivalent callers compute their unique hash with
   different ``task_name`` normalisations (canonical case vs lower-
   cased). ``compute_dedup_hash`` hashes the task_name bytes verbatim,
   so the two calls land on different Valkey cells and BOTH execute.
   This is a divergence bug if the two callers believed they were
   deduping the "same logical task" — but the chaos test pins the
   *tolerance* contract: divergence degrades to no-dedup, never to
   corruption or crash. Fixing the divergence is out of scope; this
   test documents the failure mode.

All tests use the ``chaos_stack`` fixture from ``tests/chaos/conftest.py``.
Schema-evolution tests write raw msgpack blobs to Valkey via a direct
``valkey.asyncio.Valkey.from_url`` client so we can simulate a worker of
a different code version without running two swarmq checkouts.
"""

from __future__ import annotations

import asyncio
import uuid

import msgpack
import pytest
import valkey.asyncio

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskError
from swarmq.serialization import compute_dedup_hash
from swarmq.task import Task, TaskInfo
from tests.chaos.conftest import ChaosStack

pytestmark = [pytest.mark.timeout(120)]


def _result_key(task_id: str) -> str:
    """Mirror of ValkeyBackend._key — the canonical result key prefix."""
    return f"sq:result:{task_id}"


class TestByzantineWorker:
    # ---------------------------------------------------------------------
    # Scenario 1 — old writer (missing fields), new reader.
    # ---------------------------------------------------------------------
    async def test_old_schema_writer_new_schema_reader(
        self, chaos_stack: ChaosStack,
    ) -> None:
        """An old worker writes a blob without ``is_cancelled``.

        Phase 3b.3 added ``is_cancelled`` to the result envelope (see
        :func:`swarmq.cancellation.build_result_dict`). Workers running
        pre-3b.3 code would write a four-key blob; the current client
        must still decode it. ``Client._unpack`` reads ``is_cancelled``
        via ``.get(..., False)`` precisely for this reason.
        """
        task_id = str(uuid.uuid7())

        # Old-style result: four keys, no ``is_cancelled``.
        old_blob = msgpack.packb(
            {
                "task": {
                    "id": task_id, "queue": "q", "name": "legacy",
                },
                "is_error": False,
                "error_message": "",
                "result": {"legacy": True, "value": 42},
            }
        )
        assert isinstance(old_blob, bytes)

        client = valkey.asyncio.Valkey.from_url(chaos_stack.valkey_url)
        try:
            await client.set(_result_key(task_id), old_blob, px=60_000)
        finally:
            await client.aclose()

        # Read via the production Client path.
        broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        backend = ValkeyBackend(chaos_stack.valkey_url)
        app = SwarmQ(broker, backend, queues=["byzantine-q"])
        async with app:
            value = await app.get_result(task_id, timeout=5)

        assert value == {"legacy": True, "value": 42}, (
            f"old-schema blob did not roundtrip: got {value!r}"
        )

    # ---------------------------------------------------------------------
    # Scenario 2 — new writer (extra fields), old reader.
    # ---------------------------------------------------------------------
    async def test_new_schema_writer_old_schema_reader(
        self, chaos_stack: ChaosStack,
    ) -> None:
        """A newer worker writes extra top-level fields.

        A "next version" worker may attach observability metadata
        (``trace_id``, ``worker_version``, ``timings``, …) that the
        running client does not know about. ``Client._unpack`` only
        touches known keys, so the extras are silently dropped and
        the canonical ``result`` is still returned.
        """
        task_id = str(uuid.uuid7())

        future_blob = msgpack.packb(
            {
                "task": {
                    "id": task_id,
                    "queue": "q",
                    "name": "from_future",
                    "future_subkey": ["irrelevant", "metadata"],
                },
                "is_error": False,
                "is_cancelled": False,
                "error_message": "",
                "result": "hello-from-the-future",
                # Fields unknown to the current _unpack:
                "trace_id": "abc123",
                "worker_version": "99.0.0-next",
                "timings": {"queue_wait_ms": 12, "run_ms": 4},
                "unknown_flag": True,
            }
        )
        assert isinstance(future_blob, bytes)

        client = valkey.asyncio.Valkey.from_url(chaos_stack.valkey_url)
        try:
            await client.set(_result_key(task_id), future_blob, px=60_000)
        finally:
            await client.aclose()

        broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        backend = ValkeyBackend(chaos_stack.valkey_url)
        app = SwarmQ(broker, backend, queues=["byzantine-q"])
        async with app:
            value = await app.get_result(task_id, timeout=5)

        assert value == "hello-from-the-future", (
            f"future-schema blob did not decode cleanly: got {value!r}"
        )

        # Error flag from the future must still propagate. Second
        # blob exercises the is_error branch with future extras.
        err_task_id = str(uuid.uuid7())
        future_err_blob = msgpack.packb(
            {
                "task": {"id": err_task_id, "queue": "q", "name": "future_err"},
                "is_error": True,
                "is_cancelled": False,
                "error_message": "boom from the future",
                "result": None,
                "future_error_envelope": {"cause_chain": ["A", "B"]},
                "retry_attempt": 7,
            }
        )
        client2 = valkey.asyncio.Valkey.from_url(chaos_stack.valkey_url)
        try:
            await client2.set(
                _result_key(err_task_id), future_err_blob, px=60_000,
            )
        finally:
            await client2.aclose()

        broker2 = RabbitMQBroker(chaos_stack.rabbitmq_url)
        backend2 = ValkeyBackend(chaos_stack.valkey_url)
        app2 = SwarmQ(broker2, backend2, queues=["byzantine-q"])
        async with app2:
            with pytest.raises(TaskError, match="boom from the future"):
                await app2.get_result(err_task_id, timeout=5)

    # ---------------------------------------------------------------------
    # Scenario 3 — divergent unique-hash → dedup silently no-ops.
    # ---------------------------------------------------------------------
    async def test_divergent_unique_hash_breaks_dedup_documented(
        self, chaos_stack: ChaosStack,
    ) -> None:
        """Two producers normalise task_name differently → no dedup.

        ``compute_dedup_hash`` hashes ``task_name.encode("utf-8") + b"\\x00"
        + canonical_encode(args, kwargs)``. ``canonical_encode`` already
        forces sorted keys and permutation-invariant encoding, so
        divergence there is not realistic. task_name, however, is
        hashed verbatim — two producers that disagree on casing
        (``"EchoTask"`` vs ``"echotask"``) compute different hashes and
        land on different ``sq:unique:*`` cells. Both SETNX succeed.
        Both tasks execute.

        This test pins the *failure mode*, not a fix. The system
        degrades gracefully: divergent hashes break dedup but do not
        corrupt state, crash workers, or lose messages.
        """
        # Demonstrate the divergence at the hash layer directly — this
        # is the root cause documented without needing the broker.
        canonical_name = "EchoTask"
        divergent_name = canonical_name.lower()
        args = (42,)
        kwargs: dict[str, object] = {"mode": "demo"}

        hash_canonical = compute_dedup_hash(canonical_name, args, kwargs)
        hash_divergent = compute_dedup_hash(divergent_name, args, kwargs)
        assert hash_canonical != hash_divergent, (
            "regression: task_name case normalisation now converges — "
            "update this test's divergence example"
        )

        # End-to-end: both producers schedule with unique_for set, but
        # against task NAMES that differ. Register two task classes
        # under the two names so the worker can execute either.
        queue = f"byz-{uuid.uuid4().hex[:8]}"
        results: list[str] = []

        class EchoCanonical(Task):
            task_info = TaskInfo(
                name=canonical_name, queues=[queue], unique_for=60,
            )

            async def run(self, value: int, mode: str = "") -> str:
                results.append(f"canonical:{value}:{mode}")
                return f"canonical:{value}:{mode}"

        class EchoDivergent(Task):
            task_info = TaskInfo(
                name=divergent_name, queues=[queue], unique_for=60,
            )

            async def run(self, value: int, mode: str = "") -> str:
                results.append(f"divergent:{value}:{mode}")
                return f"divergent:{value}:{mode}"

        # Task subclasses auto-register into Task._registered_tasks;
        # SwarmQ() picks them up via registry at construction.
        client_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        client_backend = ValkeyBackend(chaos_stack.valkey_url)
        client_app = SwarmQ(
            client_broker, client_backend, queues=[queue],
        )

        worker_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        worker_backend = ValkeyBackend(chaos_stack.valkey_url)
        worker_app = SwarmQ(
            worker_broker, worker_backend, queues=[queue],
        )

        worker_task: asyncio.Task | None = None
        async with client_app:
            try:
                async with worker_app:
                    worker_task = asyncio.create_task(
                        worker_app.start_worker()
                    )
                    await asyncio.sleep(0.8)  # consumer setup

                    # Two "logically identical" schedules with diverging
                    # name normalisation.
                    tid_canonical = await client_app.schedule(
                        canonical_name, 42, mode="demo",
                    )
                    tid_divergent = await client_app.schedule(
                        divergent_name, 42, mode="demo",
                    )

                    # Different task_ids: no silent dedup took place.
                    assert tid_canonical != tid_divergent, (
                        "regression: dedup collapsed the two schedules "
                        "into one — case-divergent task_names now share "
                        "a cell, which this test asserts is NOT the case"
                    )

                    v1 = await client_app.get_result(
                        tid_canonical, timeout=15,
                    )
                    v2 = await client_app.get_result(
                        tid_divergent, timeout=15,
                    )

                    # Both executed — dedup bypass confirmed.
                    assert v1 == "canonical:42:demo"
                    assert v2 == "divergent:42:demo"
                    assert sorted(results) == [
                        "canonical:42:demo", "divergent:42:demo",
                    ], (
                        f"expected both task variants to run exactly once, "
                        f"got {results!r}"
                    )
            finally:
                if worker_task is not None:
                    worker_task.cancel()
                    try:
                        await worker_task
                    except (asyncio.CancelledError, Exception):
                        pass
